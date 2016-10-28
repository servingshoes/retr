#!/usr/bin/python3

'''
The minimal usage would be:
pp=proxypool(fn='proxies.txt')
rtv=retriever(pp)
r=rtv.request('get', 'www.google.com')
'''

from time import sleep
from logging import getLogger
from pdb import set_trace

import requests
from requests.packages.urllib3.poolmanager import ProxyManager

from .proxypool import st_disabled, st_proven

lg=getLogger(__name__)

# Result of the validate function
res_ok=0
res_nok=1
res_retry=2 # Retry with different proxy
res_continue=3 # Retry with the same proxy. Temporary condition

class _adapter(requests.adapters.HTTPAdapter):
    def __init__(self, p, headers={}, proxy_headers={}, timeout=30,
                 max_retries=3, ca_certs=False):
        super().__init__(max_retries=max_retries,
                         pool_connections=10,
                         pool_maxsize=1, pool_block=True)
        self.pm=ProxyManager(p, num_pools=10, timeout=timeout, retries=max_retries,
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
        
    def __init__(self, pp, headers={}, proxy_headers={},
                 max_retries=1, timeout=60, ca_certs=None,
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
        self.setup_session()

    def __del__(self):        
        if self.p: # Leave this good proxy for someone to use
            self.pp.release_proxy(self.p)
        del self.s
        
    def change_proxy(self, err, status=None):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        # We could have use change_proxy in validate: self.p is None
        if not self.p: return

        lg.debug("Changing proxy ({}): {}".format(self.p, err))
        self.pp.set_status(self.p, status)
        self.p=None # Will pick new proxy on next download

    def pick_proxy(self):
        '''Pick proxy manually, may be needed for cases like visa, where we need to know
        our IP to put into the requests.

        '''

        if self.p is not None: return # It's set, continue with it
        
        self.p=self.pp.get_proxy()
        if self.p is None: raise LookupError
        lg.debug("proxy: {}".format(self.p))
        
        # As we're based on the requests.Session, which in turn has
        # connectionpool and persistent connections, remove previous
        # session and make a totally new one to be safe.
        if self.s:
            self.s.close()
            #set_trace()
            del self.s
        self.s = requests.Session()
        a=_adapter('http://'+self.p.p, self.headers, self.proxy_headers,
                   self.timeout, self.max_retries, self.ca_certs)
        self.s.mount('http://', a)
        self.s.mount('https://', a)

    def clear_cookies(self):
        '''Clear the session cookies'''
        lg.warning( 'Clearing cookies' )
        self.s.cookies=requests.utils.cookiejar_from_dict({})

    def validate(self, url, r):
        '''Sometimes the proxy returns good error code, but the page is wrong, for
example, requiring authorisation at the proxy, or telling we're over the limit.
This function validates the page, for example, looking for anchors. Basic
function checks only for status code. There may be several error codes returned
with different actions to be taken: look for res_*.
This function may be superseded in the descendant.

        '''
        if r.status_code in [200, 301]: return res_ok, None

        err='bad result'
        lg.debug( '{}: {}'.format(err, r.status_code) )

        return res_retry, err

    def request(self, what, url, reinit=True, **params):
        '''Downloads individual URL, the signature is more or less the same as in Session.request(). It manages proxies within the proxypool depending on the request outcome.

Setting reinit to False is used in setup_session (to evade calling setup_session() ad inf.)

        '''
        r = None # r will live on this level
        retry=0 # For chunked retries

        def _safe_del(r):
            '''Sometimes r is not bound on return from the function.'''
            try: del r
            except: pass

        while True:
            if self.quit_flag: return res_nok, 'abort'
            status=0 # Status to set should we change proxy

            try:
                self.pick_proxy()
            except LookupError:
                lg.warning('No more proxies left')
                return res_nok, 'noproxies' # Hope it'll be handled by outer scope
            
            #set_trace()
            err=""
            try:
                proxies={ 'https': self.p.p, 'http': self.p.p } if self.p else {}
                #lg.info( 'Before request: {} {}'.format(what, url) )

                # params['headers']['User-Agent']=self.ua.ff
                # sleep(10)

                r = self.s.request(what, url, timeout=self.timeout,
                                   verify=self.ca_certs, proxies=proxies,
                                   **params)
                #lg.info( 'After request: {} {}'.format(what, url) )
            except requests.exceptions.ChunkedEncodingError:
                err="chunked" # Fail otherwise
                retry+=1
                if retry < self.max_retries:
                    _safe_del(r)
                    lg.debug( '{}: retrying'.format(err) )
                    continue # Try this file again
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectTimeout):
                err="timeout"
                if self.discard_timeout:
                    status=st_disabled
            except requests.exceptions.SSLError:
                err="sslerror"
            except requests.exceptions.ContentDecodingError:
                err="decoding"
            except requests.exceptions.TooManyRedirects:
                status=st_disabled
            except ConnectionResetError:
                err="connreset"
            except OSError: # Network unreachable for example
                err="oserror"
            except requests.exceptions.ConnectionError as e:
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
            except requests.packages.urllib3.exceptions.LocationParseError: # What the hell is this?
                lg.warning('LocationParseError: {}'.format(url))
                err='locationparse'
                #return res_nok, 
            else:
                #lg.info( 'In else: {} {}'.format(what, url) )

                res,err=self.validate(url, r)
                    
                if res == res_ok:
                    self.pp.set_status( self.p, st_proven ) # Mark proxy as working
                    return r
                elif res == res_continue: # err=(msg,time_to_sleep)
                    lg.warning(err[0])
                    sleep(err[1])
                    continue
                elif res == res_nok:
                    return res, err, r
                # Else continue (res_retry eg)
                        
            #lg.info( 'After try: {} {} {} {}'.format(what, url, err, status) )
            
            _safe_del(r)
            
            self.change_proxy(err, status)
            if reinit:
                self.setup_session() # Reinit session
            else:
                # If we're in setup_session, probably we need to restart the
                # function from the beginning
                return res_nok, err
