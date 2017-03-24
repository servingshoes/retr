#!/usr/bin/python3

'''
The minimal usage would be:
pp=proxypool('proxies.txt')
rtv=retriever(pp)
r=rtv.request('get', 'www.google.com')
'''

from time import sleep
from contextlib import suppress
from logging import getLogger
from pdb import set_trace

from requests import exceptions, Session, Request
from requests.adapters import HTTPAdapter
from requests.utils import cookiejar_from_dict
from requests.packages.urllib3.poolmanager import ProxyManager
from requests.packages.urllib3.exceptions import LocationParseError

from .proxypool_common import st_disabled, st_proven

class ValidateException(Exception):
# Used in the validate function. Argument:
# ('retry', reason) # Retry with different proxy
# ('continue', warning, sleep_in_seconds) # Retry with the same proxy. Temporary condition
    pass

lg=getLogger(__name__)

class _adapter(HTTPAdapter):
    def __init__(self, p, headers={}, proxy_headers={}, timeout=30,
                 max_retries=0, ca_certs=False):
        super().__init__(max_retries=max_retries, # This one is the main value
                         pool_connections=10,
                         pool_maxsize=1, pool_block=True)
        self.pm=ProxyManager(p, num_pools=10, timeout=timeout, # retries=2, #max_retries,
                             headers=headers, proxy_headers=proxy_headers,  
                             ca_certs=ca_certs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        return self.pm
        
# Has its own Session object
class retriever:
    '''Retriever with proxy support, in case of a problem with a proxy, it switches to another proxy.'''
    def setup_session(self):
        '''To be overridden in the descendant classes. This function is called after each change of the proxy, the intention is to prepare the session somehow. May login for example.'''
        pass

    # 'Accept-Encoding': 'gzip, deflate, sdch'
    def __init__(self, pp, headers={}, proxy_headers={},
                 max_retries=0, timeout=60, ca_certs=None,
                 discard_timeout=False):
        '''pp - proxypool to use
headers - the headers to use in the underlying requests.Session
max_retries and timeout - the same as in requests.Session.

discard_timeout set to True will discard timed out proxies. Should have some sort of refresh, or we'll run out of proxies
'''
        self.pp = pp # Proxypool
        self.p = None
        self.headers=headers # To use when downloading
        self.proxy_headers=proxy_headers # To use when downloading
        self.max_retries=max_retries
        self.timeout=timeout
        self.discard_timeout=discard_timeout
        self.ca_certs=ca_certs
        self.s=None
        self.quit_flag=False # Set to True from outside to make thread quit

    def __del__(self):        
        if self.p: # Leave this good proxy for someone to use
            self.pp.release_proxy(self.p)
        del self.s
        
    def change_proxy(self, err, status=None):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        # We could use change_proxy in validate: self.p is None
        if not self.p: return

        lg.debug("Changing proxy ({}): {}".format(self.p, err))
        self.pp.set_status(self.p, status)
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
            #set_trace()
            del self.s
        self.s = Session()
        a=_adapter('http://'+self.p.p, self.headers, self.proxy_headers,
                   self.timeout, self.max_retries, self.ca_certs)
        self.s.mount('http://', a)
        self.s.mount('https://', a)
        if regular: self.setup_session()

    def clear_cookies(self):
        '''Clear the session cookies'''
        lg.warning( 'Clearing cookies' )
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

        def _safe_del(r):
            '''Sometimes r is not bound on return from the function.'''
            try: del r
            except: pass

        headers=self.headers
        allow_redirects=True # Default
        with suppress(KeyError):
            headers.update(params.pop('headers'))
        with suppress(KeyError):
            allow_redirects=params.pop('allow_redirects')
        req = Request(what, url, headers=headers, **params)

        while True:
            #print(self.quit_flag)
            if self.quit_flag:
                raise KeyboardInterrupt
            
            status=0 # Status to set should we change proxy

            #lg.info('New run')
            self.pick_proxy(regular) # This will eventually call setup_session()

            self.update_request(req, regular)

            prepped = self.s.prepare_request(req)

            #set_trace()
            err=""
            try:
                proxies={ 'https': self.p.p, 'http': self.p.p } if self.p else {}
                #lg.info( 'Before request: {} {}'.format(what, url) )

                # params['headers']['User-Agent']=self.ua.ff
                # sleep(10)

                r = self.s.send(prepped, proxies=proxies,
                                verify=self.ca_certs, timeout=self.timeout,
                                allow_redirects=allow_redirects)
                #lg.info( 'After request: {} {}'.format(what, url) )
                #print(r.text)
                self.validate(url, r)
                self.pp.set_status( self.p, st_proven ) # Mark proxy as working
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
                if self.discard_timeout:
                    status=st_disabled
            except exceptions.SSLError:
                err="sslerror"
            except exceptions.ContentDecodingError:
                err="decoding"
            except exceptions.TooManyRedirects:
                err="redirects"
                status=st_disabled
            except ConnectionResetError:
                err="connreset"
            except OSError as e: # Network unreachable for example
                err='oserror: '+e.__str__()
            except exceptions.ConnectionError as e:
                err=e.__str__()
                if 'Connection reset by peer' in err or\
                   'Read timed out' in err or\
                   'EOF occurred in violation of protocol' in err or\
                   'Too many open connections' in err or\
                   'Temporary failure in name' in err:
                    pass
                elif 'Caused by ProxyError' in err or\
                     'No connections allowed from your IP' in err or\
                     'Connection timed out' in err or\
                     'Connection aborted' in err:
                    status=st_disabled
                else:
                    lg.warning('Unhandled exception in download')
                    raise
            except LocationParseError: # What the hell is this?
                lg.warning('LocationParseError: {}'.format(url))
                err='locationparse'
                #return res_nok, 
            except ValidateException as e:
                tp, *args=e.args
                err=args[0]
                if tp == 'continue': # args=(msg,time_to_sleep)
                    lg.warning(args[0])
                    sleep(args[1])
                    continue
                # elif res == res_nok:
                # return res, err, r
                elif tp == 'retry':
                    if regular:
                        pass # Fall to the bottom to change proxy and retry
                    else:
                        raise # Propagate to the toplevel, let'em handle it
                else:
                    raise # Unknown exceptions are propagated
            else:
                lg.info( 'In else: {} {}'.format(what, url) )
                _exit(1)
                
            #lg.info( 'After try: {} {} {} {}'.format(what, url, err, status) )
            
            _safe_del(r)
            
            self.change_proxy(err, status)
