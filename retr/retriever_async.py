#!/usr/bin/python3

'''
The minimal usage would be:
TODO: update
# pp=proxypool('proxies.txt')
# rtv=retriever(pp)
# r=rtv.request('get', 'www.google.com')
'''

#from time import sleep
from contextlib import suppress
from logging import getLogger
from aiohttp import ClientSession, \
    ClientConnectorError, ClientResponseError, ClientOSError, \
    ClientHttpProxyError, ServerDisconnectedError, ClientProxyConnectionError
from async_timeout import timeout
from asyncio import TimeoutError as aio_TimeoutError, sleep, Task
from os import makedirs, path
from ssl import SSLError, SSLZeroReturnError, CertificateError
from pdb import set_trace

from retrying import retry

from .utils import CustomAdapter
from .proxypool_common import proxy_stats
from . import ValidateException

lg=getLogger(__name__)
#lg=CustomAdapter(lg, {})
        
# Has its own ClientSession object inside
class retriever:
    '''Retriever with proxy support, in case of a problem with a proxy, it switches to another proxy.'''
    async def setup_session(self):
        '''To be overridden in the descendant classes. This function is called after each change of the proxy, the intention is to prepare the session somehow. May login for example.'''
        pass

    # 'Accept-Encoding': 'gzip, deflate, sdch'
    def __init__(self, pp, headers={}, proxy_headers={},
                 max_retries=0, timeout=60, ca_certs=None,
                 disable_timeout=False):
        '''pp - proxypool to use
headers - the headers to use in the underlying requests.Session
max_retries and timeout - the same as in ClientSession.

disable_timeout set to True will disable timed out proxies. Should have some sort of refresh, or we'll run out of proxies
'''
        self.pp = pp # Proxypool
        self.p = None
        # If there's no function
        if not getattr(self, 'headers'): self.headers=headers # To use when downloading
        self.proxy_headers=proxy_headers # To use when downloading
        self.max_retries=max_retries
        self.timeout=timeout
        self.disable_timeout=disable_timeout
        self.ca_certs=ca_certs
        self.s=None

    def __del__(self):
        if self.pp and self.p: # Leave this good proxy for someone to use
            self.pp.release_proxy(self.p)
        if self.s:
            self.s.close()
                
    def __getattr__(self, name):
        '''Prepend name name to standard logger functions'''
        #set_trace()
        if name in ('debug', 'info', 'warning', 'error', 'exception'):
            # task id by default
            nm=getattr(self, 'name',
                       '{:04x}'.format(id(Task.current_task())%(1<<16)))
            return lambda msg: getattr(lg, name)('[{}] {}'.format(nm, msg))
                                   
    def change_proxy(self, err, status=None):
        '''Change proxy, optionally changing its status. The reason might have been that it was
bad or something, put it to the end of the list or mark as disabled.

        '''
        # We could use change_proxy in validate: self.p is None
        if not self.p: return

        self.debug('Changing proxy ({}): {}'.format(self.p, err))
        self.pp.set_status(self.p, status)
        self.p=None # Will pick new proxy on next download

    async def cached(self, fn, what, *args, **kwargs):
        '''Expects do_filter and cache members'''
        try:
            has_cache=self.cache
        except AttributeError:
            has_cache=False # By default

        try:
            with suppress(AttributeError): # If we don't have do_filter
                if self.do_filter: raise FileNotFoundError
                
            if has_cache:
                # Only create intermediate dirs if we use cache
                makedirs(path.dirname(fn), exist_ok=True)
                with open(fn, 'rb') as ff: text=ff.read()
            else:
                raise FileNotFoundError # Fire artificially to retrieve the page
            self.debug('Found {}: {}'.format(what, fn))
            r=None
        except:
            self.debug('Getting {}'.format(what))
            r=await self.request(*args, **kwargs)
            text=await r.text()
            if has_cache:
                with open(fn, 'w') as ff: ff.write(text)
        
        return text, r

    async def pick_proxy(self, regular=False):
        '''Pick proxy manually, may be needed for cases like visa, where we need to know
        our IP to put into the requests.

        '''

        if self.p is not None: return
        
        self.p=self.pp.get_proxy()
        self.debug("proxy: {}".format(self.p))
        
        # I don't know what's underneath, probably connection pools and
        # stuff. So remove previous session and make a totally new one to be
        # safe.
        if self.s:
            self.s.close()
            #set_trace()
            del self.s
        headers=self.headers() if callable(self.headers) else self.headers
        self.s = ClientSession(headers=headers) # loop=loop
        if regular: await self.setup_session()

    # def clear_cookies(self):
    #     '''Clear the session cookies'''
    #     self.warning( 'Clearing cookies' )
    #     if self.s:
    #         self.s.cookies=cookiejar_from_dict({})

    async def validate(self, r):
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

    def update_stats(self, host, start, end):
        # Get stats for the given proxy
        if host not in self.p.stats:
            self.p.stats[host]=proxy_stats()
        stats=self.p.stats[host]
        stats.latencies.append(end-start)
        stats.last_access=start
        print(stats)        
        
    async def request(self, what, url, regular=True, **params):
        '''Downloads individual URL, the signature is more or less the same as in ClientSession.request(). It manages proxies within the proxypool depending on the request outcome.

Setting regular to False is used in setup_session (to evade calling setup_session() ad inf., and for other things that differ)

        '''
        r = None
        status=None # Status to set should we change proxy

        headers={}
        with suppress(KeyError): headers=params.pop('headers')

        while True:
            try:
                await self.pick_proxy(regular) # This will call setup_session()

                #self.info( 'Before request: {} {}'.format(what, url) )

                #proxy=self.p.p # if self.p else None
                if self.p.creds: # Optional proxy auth
                    headers['Proxy-Authorization']=self.p.creds

                pass_params=params.copy()
                if self.p.p: # We have proxy
                    pass_params['proxy']=self.p.p

                if headers:
                    pass_params['headers']=headers
                    
                with timeout(self.timeout):
                    r = await self.s.request(what, url, **pass_params)
                #self.info( 'After request1: {} {}'.format(what, url) )
                await self.validate(r) # May require getting data

                self.pp.set_status( self.p, 'P' ) # Mark proxy as working

                return r
            except ClientProxyConnectionError as e:
                err='ClientProxyConnectionError: '+e.__str__()
                status='D'
            except (aio_TimeoutError, TimeoutError):
                err='timeout'
                if self.disable_timeout: status='D'
            except ValidateException as e:
                # first arg is retry/continue/...
                tp, err, *rest=e.args
                if tp == 'continue': # args=(msg,time_to_sleep)
                    self.warning(err)
                    await sleep(rest[0])
                elif tp == 'retry':
                    pass # Fall to the bottom to change proxy and retry
                else:
                    raise e # Unknown exceptions are propagated
            except (                    
                    ClientConnectorError, ClientResponseError, ClientOSError,
                    ClientHttpProxyError,
                    SSLError, SSLZeroReturnError, CertificateError,
                    ConnectionResetError, ServerDisconnectedError
            ) as e:
                err=type(e)
                self.debug('Exception: {} — {}'.format(
                    err, e.__str__()))
            else:
                raise # Unknown (yet) exception

            if r: r.close()

            # If we're one time (filtering), quit here
            if 'O' in self.p.flags:
                # This goes to the outer task_done. Depending on our needs we
                # can either remove the proxy from the master_list (so it'll be
                # gone forever), or mark it as bad ('B'). I discard totally
                # wrong proxies, say if the proxy itself reports an error, or
                # needs authorisation. And "bad" is for the proxies I still hope
                # can be brought back to senses, maybe it's temporarily blocked
                # or something.

                #self.exception('Error: {}'.format(err))
                #set_trace()
                raise ProxyException(self.p.p)

            self.change_proxy(err, status)

            #self.info( 'After request2: {} {}'.format(what, url) )
            
