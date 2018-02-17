#!/usr/bin/python3

'''
The minimal usage would be:
pp=proxypool('proxies.txt')
rtv=retriever(pp)
r=rtv.request('get', 'www.google.com')
'''

from time import sleep, time
from contextlib import suppress
from logging import getLogger
from pdb import set_trace
from ssl import SSLError
from OpenSSL.SSL import Error as openssl_error
from os import makedirs, path
import urllib3.exceptions as u3_exceptions

from requests import exceptions, Session, Request
from requests.adapters import HTTPAdapter
from requests.utils import cookiejar_from_dict
from requests.packages.urllib3.poolmanager import ProxyManager
from requests.packages.urllib3.exceptions import LocationParseError

from . import ValidateException

lg=getLogger(__name__)

class _adapter(HTTPAdapter):
    def __init__(self, p, proxy_headers={}, timeout=30,
                 max_retries=0, ca_certs=False):
        super().__init__(max_retries=max_retries, # This one is the main value
                         pool_connections=10,
                         pool_maxsize=1, pool_block=True)
        self.pm=ProxyManager(p, num_pools=10, timeout=timeout, # retries=2, #max_retries,
                             proxy_headers=proxy_headers,  
                             ca_certs=ca_certs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        return self.pm
        
# Has its own Session object
class retriever:
    '''Retriever with proxy support, in case of a problem with a proxy, it switches to another proxy.'''
    def setup_session(self):
        '''To be overridden in the descendant classes. This function is called after each change of the proxy, the intention is to prepare the session somehow. May login for example.
        Another example would be to setup special headers (use self.headers)
'''
        pass

    # 'Accept-Encoding': 'gzip, deflate, sdch'
    def __init__(self, pp, proxy_headers={},
                 max_retries=0, timeout=60, ca_certs=None,
                 discard_timeout=False):
        '''pp - proxypool to use
max_retries and timeout - the same as in requests.Session.

discard_timeout set to True will discard timed out proxies. Should have some sort of refresh, or we'll run out of proxies
'''
        self.pp = pp # Proxypool
        self.p = None
        self.headers={} # To use when downloading, can be set in setup_session()
        self.proxy_headers=proxy_headers # To use when downloading
        self.max_retries=max_retries
        self.timeout=timeout
        self.discard_timeout=discard_timeout
        self.ca_certs=ca_certs
        self.s=None
        self.quit_flag=False # Set to True from outside to make thread quit
        self.f=None # To be filled by the farm, backlink to the farm object
        
    def __del__(self):        
        if self.p: # Leave this good proxy for someone to use
            self.pp.release_proxy(self.p)
        del self.s
        
    def change_proxy(self, err, status=None, *args):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        # We could use change_proxy in validate: self.p is None
        if not self.p: return

        lg.debug("Changing proxy {} {}→{}: {}".format(
            self.p, self.p.flags, status, err))
        self.pp.set_status(self.p, status, *args)
        self.p=None # Will pick new proxy on next download

    def pick_proxy(self, regular=False):
        '''Pick proxy manually, may be needed for cases like visa, where we need to know
        our IP to put into the requests.

        '''

        if self.p is not None: return
        
        self.p=self.pp.get_proxy()
        lg.debug("proxy: {}".format(self.p))
        
        # As we're based on the requests.Session, which in turn has
        # connectionpool and persistent connections, remove previous
        # session and make a totally new one to be safe.
        if self.s:
            self.s.close()
            del self.s
        self.s = Session()
        # I cannot make it work unless set the headers here. No idea why.
        if self.p.p: # Only if we have some proxy            
            self.proxy_headers={'Proxy-Authorization': self.p.creds} if self.p.creds else None            
            a=_adapter(self.p.p, self.proxy_headers,
                       self.timeout, self.max_retries, self.ca_certs)
            self.s.mount('http://', a)
            self.s.mount('https://', a)
        if regular: self.setup_session()

    def cached(self, fn, what, binary, *args, **kwargs):
        '''Expects do_filter and cache members'''
        try:
            if self.do_filter: raise FileNotFoundError
            if self.cache:
                # Only create intermediate dirs if we use cache
                makedirs(path.dirname(fn), exist_ok=True)
                with open(fn, 'rb' if binary else 'r') as ff: text=ff.read()
            else:
                raise FileNotFoundError # Fire artificially to retrieve the page
            lg.debug('Found {}: {}'.format(what, fn))
            cached=True
        except:
            lg.debug('Getting {}'.format(what))
            r=self.request(*args, **kwargs)
            text=r.content if binary else r.text 
            if self.cache:
                if binary:
                    open_args={'mode': 'wb'}
                else:
                    open_args={'mode': 'w', 'encoding': 'utf8'}
                with open(fn,  **open_args) as ff: ff.write(text)
            cached=False
        
        return text, cached

    def clear_cookies(self):
        '''Clear the session cookies'''
        lg.info( 'Clearing cookies' )
        if self.s:
            self.s.cookies=cookiejar_from_dict({})

    def validate(self, url, r):
        '''Sometimes the proxy returns good error code, but the page is wrong, for
example, requiring authorisation at the proxy, or telling we're over the limit.
This function validates the page, for example, looking for anchors. Basic
function checks only for status code. To signal some unusual condition, raise a ValidateException (see the doc)
This function may be superseded in the descendant.

        '''
        if r.status_code in [200, 301]: return

        raise ValidateException('retry',
                                'bad result: {}'.format(r.status_code))

    def update_request(self, req, regular):
        '''To be implemented in the descendant class. Call it to update the prepared request used within the request() cycle. For example setup_session may return some token to be used in subsequent calls'''
        pass
    
    def request(self, what, url, regular=True, **params):
        '''Downloads individual URL, the signature is more or less the same as in Session.request(). It manages proxies within the proxypool depending on the request outcome.

Setting regular to False is used in setup_session (to evade calling setup_session() ad inf., and for other things that differ)

        '''
        r = None # r will live on this level
        retry=0 # For chunked retries

        #set_trace()
        request_headers=params.pop('headers', {})            

        allow_redirects=True # Default
        with suppress(KeyError):
            allow_redirects=params.pop('allow_redirects')

        # NB: headers, if present in the request args, is passed here
        req = Request(what, url, **params)

        while True:
            #print(self.quit_flag)
            if self.quit_flag:
                raise KeyboardInterrupt
            
            status=None # Status to set should we change proxy
            args=[] # Additional args for the proxy
            
            #lg.info('New run')
            self.pick_proxy(regular) # This will eventually call setup_session()

            self.update_request(req, regular)
            # In case they were set in setup_session()
            prepped = self.s.prepare_request(req)
            prepped.headers.update(self.headers)
            prepped.headers.update(request_headers)

            err=""
            try:
                proxies={'https': self.p.p, 'http': self.p.p} if self.p else {}
                #lg.debug( 'Before request: {} {}'.format(what, url) )

                # params['headers']['User-Agent']=self.ua.ff
                # sleep(10)

                time_start=time()
                r = self.s.send(prepped, proxies=proxies,
                                verify=self.ca_certs, timeout=self.timeout,
                                allow_redirects=allow_redirects)
                #lg.debug( 'After request: {} {}'.format(what, url) )
                #print(r.text)
                self.validate(url, r)
                self.pp.set_status( self.p, 'P' ) # Mark proxy as working
                self.p.time=time()-time_start
                return r
            except exceptions.ChunkedEncodingError:
                err="chunked" # Fail otherwise
                # TODO: don't remember why it's here
                # retry+=1
                # if retry < self.max_retries:
                #     _safe_del(r)
                #     lg.debug( '{}: retrying'.format(err) )
                #     continue # Try this file again
            except (exceptions.ReadTimeout, exceptions.ConnectTimeout):
                err="timeout"
                if self.discard_timeout: status='D'
            except exceptions.TooManyRedirects:
                err="redirects"
                status='D'
            except exceptions.ConnectionError as e:
                err=e.__str__()
                e1=e.args[0]
                if isinstance(e1, u3_exceptions.MaxRetryError):
                    e2=e1.reason
                    #set_trace()
                    err=e2.__str__()
                    if isinstance(e2, u3_exceptions.ProxyError): # (str, exc)
                        reason, exc=e2.args
                        if isinstance(exc, u3_exceptions.NewConnectionError):
                            err=exc.args[0] # Timeout or pool error
                        else:
                            status='D' # Bad proxy                    
                elif isinstance(e, (
                        exceptions.ProxyError,
                )):
                    #set_trace()
                    status='D' # Bad proxy
                elif type(e) in (
                        exceptions.SSLError, exceptions.ConnectionError
                ):
                    pass # Just an error
                else:
                    lg.warning('Unhandled exception in download')
                    raise
            except ValidateException as e:
                #set_trace()
                tp, *args=e.args
                err=args[0]
                if tp == 'continue': # args=(msg,time_to_sleep)
                    lg.warning(args[0])
                    status='C'
                    args=[time()+args[1]]
                # elif res == res_nok:
                # return res, err, r
                elif tp == 'retry':
                    if regular:
                        pass # Fall to the bottom to change proxy and retry
                    else:
                        raise # Propagate to the toplevel, let'em handle it
                else:
                    raise # Unknown exceptions are propagated
            except Exception as e:
                lg.warning('Exception: {} — {}'.format(type(e), e.__str__()))
                if not isinstance(e, (
                        OSError, # Network unreachable for example
                        LocationParseError, # What the hell is this?
                        ConnectionResetError, exceptions.SSLError, SSLError,
                        openssl_error, exceptions.ContentDecodingError
                )):
                    raise # Unknown (yet) exception
                err=type(e)

            # Sometimes r is not bound on return from the function.
            with suppress(UnboundLocalError): del r
            
            #lg.info( 'After try: {} {} {} {}'.format(what, url, err, status) )

            # If we're one time (filtering), quit here. sel.p can be
            # None if we're cancelled
            if self.p and 'O' in self.p.flags:
                # This goes to the outer function. Depending on our needs we
                # can either remove the proxy from the master_list (so it'll be
                # gone forever), or mark it as bad ('B'). I discard totally
                # wrong proxies, say if the proxy itself reports an error, or
                # needs authorisation. And bad is for the proxies I still hope
                # can be brought back to senses, maybe it's temporarily blocked
                # or something.
                
                #lg.exception('Error: {}'.format(err))
                #set_trace()
                raise ProxyException(self.p.p)

            self.change_proxy(err, status, *args)
