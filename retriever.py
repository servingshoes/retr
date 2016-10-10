#!/usr/bin/python3

'''
The minimal usage would be:
pp=proxypool(fn='proxies.txt')
rtv=retriever(pp)
r=rtv.request('get', 'www.google.com')
'''

from time import sleep
from logging import getLogger

import requests

from .proxypool import *

lg=getLogger(__name__)

# Result of the validate function
res_ok=0
res_nok=1
res_retry=2 # Retry with different proxy
res_continue=3 # Retry with the same proxy. Temporary condition

# Has its own Session object
class retriever():
    '''Retriever with proxy support, in case of a problem with a proxy, it switches to another proxy.'''
    def setup_session(self):
        '''To be overridden in the descendant classes'''
        pass

    def reset_session(self):
        '''As it is based on the requests.Session, which in turn haas connectionpool and persistent connections, remove previous session and make a totally new one to be safe.'''
        if self.s: del self.s
        self.s = requests.Session()
        self.s.headers.update(self.headers)
        a = requests.adapters.HTTPAdapter(max_retries=self.max_retries,
                                          pool_connections=100,
                                          pool_maxsize=100)
        self.s.mount('http://', a)
        self.s.mount('https://', a)
        
    def _ssetup(self):
        '''Session setup, using when changing proxy.'''
        self.reset_session()
        self.setup_session()

    def __init__(self, pp: proxypool, headers={}, discard_timeout=None, max_retries=1,
                 timeout=60):
        '''pp - proxypool to use
headers - the headers to use in the underlying requests.Session
max_retries and timeout - the same as in requests.Session.

discard_timeout set to True will disable proxies that had timeout, allegedly will increase the spead?
'''
        self.pp = pp # Proxypool
        self.p = None
        self.headers=headers # To use when downloading
        self.discard_timeout=discard_timeout
        self.max_retries=max_retries
        self.timeout=timeout
        self.s=None
        self._ssetup()

    def __del__(self):        
        self.pp.release_proxy(self.p) # Leave this good proxy for someone to use
        del self.s
        
    def change_proxy(self, err, status=0):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        lg.debug("Changing proxy ({}): {}".format(self.p, err))
        if status: self.pp.set_status(self.p, status)
        self.p=None # Will pick new proxy on next download

    def pick_proxy(self):
        '''Pick proxy manually, may be needed for cases like visa, where we need to know
        our IP to put into the requests.

        '''

        if self.p is None:
            self.p=self.pp.get_proxy()
            if self.p is None: raise LookupError
            lg.debug("proxy: {}".format(self.p))

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

Setting reinit to False is used in setup_session (to evade calling _ssetup() ad
inf.)

        '''
        r = None # r will live on this level
        retry=0 # For chunked retries

        def _safe_del(r):
            '''Sometimes r is not bound on return from the function.'''
            try: del r
            except: pass

        while True:
            self.pick_proxy()
            
            err=""
            try:
                proxies={ 'https': self.p[0], 'http': self.p[0] } if self.p else {}
                #lg.info( 'Before request: {} {}'.format(what, url) )

                # params['headers']['User-Agent']=self.ua.ff
                # sleep(10)

                r = self.s.request(what, url, proxies=proxies,
                                   timeout=self.timeout, **params)
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
                # Sometimes it's timeout, but next time it will be ok.
                err="timeout"
                if self.discard_timeout:
                    self.pp.set_status(self.p, st_disabled)
            except requests.exceptions.SSLError:
                err="sslerror"
                retry+=1
                if retry < self.max_retries:
                    _safe_del(r)
                    lg.debug( '{}: retrying'.format(err) )
                    continue # Try this file again
            except requests.exceptions.ContentDecodingError:
                err="decoding"
            except requests.exceptions.TooManyRedirects:
                self.pp.set_status(self.p, st_disabled)
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
                    self.pp.set_status(self.p, st_disabled)
                    self._ssetup() # Reinit session to be on the safe side
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
                        
            #lg.info( 'After try: {} {} {}'.format(what, url, err) )
            
            _safe_del(r)
            
            self.change_proxy(err)
            if reinit:
                self._ssetup() # Reinit session
            else:
                # If we're in setup_session, probably we need to restart the
                # function from the beginnig
                return res_nok, err

if __name__ == "__main__":
    pp=proxypool()
