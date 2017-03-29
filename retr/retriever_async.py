#!/usr/bin/python3

'''
The minimal usage would be:
pp=proxypool('proxies.txt')
rtv=retriever(pp)
r=rtv.request('get', 'www.google.com')
'''

#from time import sleep
from contextlib import suppress
from logging import getLogger
from aiohttp import ClientSession, errors
from async_timeout import timeout
from asyncio import TimeoutError as aio_TimeoutError, sleep as aio_sleep
from ssl import CertificateError
from pdb import set_trace

from .proxypool_common import st_disabled, st_proven
from .utils import CustomAdapter

class ValidateException(Exception):
# Used in the validate function. Argument:
# ('retry', reason) # Retry with different proxy
# ('continue', warning, sleep_in_seconds) # Retry with the same proxy. Temporary condition
    pass

class ProxyException(Exception):
# Used in the filter proxy engine. Argument: proxy
    pass

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})
        
# Has its own ClientSession object inside
class retriever:
    '''Retriever with proxy support, in case of a problem with a proxy, it switches to another proxy.'''
    async def setup_session(self):
        '''To be overridden in the descendant classes. This function is called after each change of the proxy, the intention is to prepare the session somehow. May login for example.'''
        pass

    # 'Accept-Encoding': 'gzip, deflate, sdch'
    def __init__(self, pp, headers={}, proxy_headers={},
                 max_retries=0, timeout=60, ca_certs=None,
                 discard_timeout=False):
        '''pp - proxypool to use
headers - the headers to use in the underlying requests.Session
max_retries and timeout - the same as in ClientSession.

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

    def __del__(self):        
        if self.pp and self.p: # Leave this good proxy for someone to use
            self.pp.release_proxy(self.p)
        if self.s:
            try:
                self.s.close()
            except: # Again as in proxypool, maybe everything is dismantled already
                pass
                
    def change_proxy(self, err, status=None):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        # We could use change_proxy in validate: self.p is None
        if not self.p: return

        lg.debug("Changing proxy ({}): {}".format(self.p, err))
        self.pp.set_status(self.p, status)
        self.p=None # Will pick new proxy on next download

    async def pick_proxy(self, regular=False):
        '''Pick proxy manually, may be needed for cases like visa, where we need to know
        our IP to put into the requests.

        '''

        if self.p is not None: return
        
        self.p=self.pp.get_proxy()
        lg.debug("proxy: {}".format(self.p))
        
        # I don't know what's underneath, probably connection pools and
        # stuff. So remove previous session and make a totally new one to be
        # safe.
        if self.s:
            self.s.close()
            #set_trace()
            del self.s
        self.s = ClientSession(headers=self.headers) # loop=loop
        if regular: await self.setup_session()

    # def clear_cookies(self):
    #     '''Clear the session cookies'''
    #     lg.warning( 'Clearing cookies' )
    #     if self.s:
    #         self.s.cookies=cookiejar_from_dict({})

    def validate(self, r):
        '''Sometimes the proxy returns good error code, but the page is wrong, for
example, requiring authorisation at the proxy, or telling we're over the limit.
This function validates the page, for example, looking for anchors. Basic
function checks only for status code. To signal some unusual condition, raise a ValidateException (see the doc)
This function may be superseded in the descendant.
r here is aiohttp Response object.

        '''
        if r.status in [200, 301]: return

        raise ValidateException('retry', 'bad result: {}'.format(r.status))

    # def update_request(self, req, regular):
    #     '''To be implemented in the descendant class. Call it to update the prepared request used within the request() cycle. For example setup_session may return some token to be used in subsequent calls'''
    #     pass
    
    async def request(self, what, url, regular=True, **params):
        '''Downloads individual URL, the signature is more or less the same as in client_session.request(). It manages proxies within the proxypool depending on the request outcome.

Setting regular to False is used in setup_session (to evade calling setup_session() ad inf., and for other things that differ)

        '''

        r = None # r will live on this level

        headers=self.headers
        with suppress(KeyError): headers.update(params.pop('headers'))

        while True:
            status=0 # Status to set should we change proxy

            err=''
            try:
                await self.pick_proxy(regular) # This will eventually call setup_session()

                #lg.info( 'Before request: {} {}'.format(what, url) )

                #proxy=self.p.p # if self.p else None
                if self.p.creds: # Optional proxy auth
                    headers['Proxy-Authorization']=self.p.creds
                    
                with timeout(self.timeout):
                    r = await self.s.request(what, url, proxy=self.p.p,
                                             **params)
                #lg.info( 'After request1: {} {}'.format(what, url) )
                await self.validate(r) # May require getting data
                self.pp.set_status( self.p, st_proven ) # Mark proxy as working
                #lg.info( 'After request2: {} {}'.format(what, url) )
                return r
            except errors.ProxyConnectionError as e:
                err='ProxyConnectionError: '+e.__str__()
                status=st_disabled
            except errors.HttpProxyError as e:
                err='HttpProxyError: '+e.__str__()
            except (aio_TimeoutError, TimeoutError):
                err='timeout'
                if self.discard_timeout:
                    status=st_disabled
            except errors.ClientOSError as e:
                err='ClientOSEError: '+e.__str__()
            except errors.ClientResponseError as e:
                err='ClientResponseError: '+e.__str__()
            # except exceptions.TooManyRedirects:
            #     err="redirects"
            #     status=st_disabled
            except errors.ServerDisconnectedError:
                err='ServerDisconnectedError'
            except CertificateError:
                err='CertificateError'
            # except ConnectionResetError:
            #     err='ConnectionResetError'

            # except exceptions.ConnectionError as e:
            #     err=e.__str__()
            #     if 'Connection reset by peer' in err or\
            #        'Read timed out' in err or\
            #        'EOF occurred in violation of protocol' in err or\
            #        'Too many open connections' in err or\
            #        'Temporary failure in name' in err:
            #         pass
            #     elif 'Caused by ProxyError' in err or\
            #          'No connections allowed from your IP' in err or\
            #          'Connection timed out' in err or\
            #          'Connection aborted' in err:
            #         status=st_disabled
            #     else:
            #         lg.warning('Unhandled exception in download')
            #         raise
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
                    pass # Fall to the bottom to change proxy and retry
                else:
                    raise # Unknown exceptions are propagated
            except Exception as e:
                if type(e) in (
                        errors.BadStatusLine,
                         ):
                    lg.exception('Exception: {} — {}'.format(
                        type(e), e.__str__()))
            finally:
                if r: r.close()
            #lg.info( 'After try: {} {} {} {}'.format(what, url, err, status) )

            # If we're one time (filtering), quit here
            if 'O' in self.p.flags:
                # This goes to the outer task_done. Depending on our needs we
                # can either remove the proxy from the master_list (so it'll be
                # gone forever), or mark it as bad ('B'). I discard totally
                # wrong proxies, say if the proxy itself reports an error, or
                # needs authorisation. And bad is for the proxies I still hope
                # can be brought back to senses, maybe it's temporarily blocked
                # or something.
                raise ProxyException(self.p.p)

            self.change_proxy(err, status)
