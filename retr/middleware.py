# Copyright (C) 2013 by Aivars Kalvans <aivars.kalvans@gmail.com>
# Copyright (C) 2017 by Yury Pukhalsky <aikipooh@gmail.com>

from logging import getLogger

from .proxypool_common import proxypool

from twisted.internet.error import ConnectionRefusedError, TCPTimedOutError, \
    ConnectError, TimeoutError, NoRouteError
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed
from scrapy.core.downloader.handlers.http11 import TunnelError

lg = getLogger('scrapy.proxypool')

class RandomProxy:
    def __init__(self, settings):
        self.mode = settings.get('PROXY_MODE')
        self.proxy_list = settings.get('PROXY_LIST')
        if not self.proxy_list:
            raise KeyError('PROXY_LIST setting is missing')

        self.pp=proxypool(self.proxy_list)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
        
    def process_request(self, request, spider):
        lg.debug('in process_request: {}, {}'.format(request, request.meta))

        pa=request.meta.pop('proxy_action', None)
        if pa == 'disable':
            self.pp.set_status(request.meta['proxy'], 'disabled')
            del request.meta['proxy'] # Make it pick another proxy
            
        # Don't overwrite with a random one (server-side state for IP)
        if 'proxy' in request.meta: return # No fuss, we have a proxy already

        if self.mode == 'random':
            proxy = self.pp.get_proxy(True)        
        elif self.mode == 'sequential':
            proxy = self.pp.get_proxy()

        request.meta['proxy'] = proxy.p
        if proxy.creds: request.headers['Proxy-Authorization'] = proxy.creds

        lg.debug('Using proxy '+proxy.p)
        
        # Start setup_session anew wherever we are, fresh or recurring
        req=request.meta.get('ss_request')
        if req:
            # Store original request to use after the session is setup
            if 'original_request' not in request.meta:
                request.meta['original_request']=request
        else:
            req=request
            
        return req.replace(meta=request.meta, dont_filter=True)

    def process_exception(self, request, exception, spider):
        if 'proxy' not in request.meta: return

        mode=request.meta.get('proxy_mode', self.mode) # Possible override
        if mode == 'once': # Try once mode, quit here
            return
        
        self.pp.set_status(request.meta['proxy'], None) # Simple downvote
        del request.meta['proxy'] # Will pick new proxy on next request

        # List of conditions when we retry. Some of them may disable the proxy (TBD)
        if type(exception) in (
                ConnectionRefusedError, ConnectError, TimeoutError,
                TCPTimedOutError, NoRouteError, ResponseNeverReceived,
                ResponseFailed, TunnelError ):
            lg.error('{} on %s'.format(type(exception)), request.url)

            return request.replace(dont_filter = True)
