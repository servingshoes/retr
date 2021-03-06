# Copyright (C) 2013 by Aivars Kalvans <aivars.kalvans@gmail.com>
# Copyright (C) 2017 by Yury Pukhalsky <aikipooh@gmail.com>

from logging import getLogger
from urllib.parse import urlparse
from pdb import set_trace

from .proxypool_common import proxypool

from twisted.internet.error import ConnectionRefusedError, TCPTimedOutError, \
    ConnectError, TimeoutError, NoRouteError
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed

from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy import signals
from scrapy.exceptions import IgnoreRequest

lg = getLogger('scrapy.proxypool')

class RandomProxy:
    def __init__(self, settings):
        self.mode = settings.get('PROXY_MODE')
        self.proxy_flags = settings.get('PROXY_FLAGS', '')
        self.proxy_list = settings.get('PROXY_LIST')
        if not self.proxy_list: raise KeyError('PROXY_LIST setting is missing')

        self.pp=proxypool(self.proxy_list)

    @classmethod
    def from_crawler(cls, crawler):
        o=cls(crawler.settings)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        return o

    def spider_opened(self, spider):
        spider.pp=self.pp # Export proxylist to the spider
        
        self.plist_map={}
        # Scrapy 1.4 strips username:password, so we need to map proxies with and without to each other. Proxy entry in the master_plist has username/password

        for i in self.pp.master_plist:
            o=urlparse(i)
            self.plist_map['{0.scheme}://{0.hostname}:{0.port}'.format(o)]=i
            
        lg.debug('proxy middleware: {} mode, flags: {}, proxies: {}'.format(
            self.mode, self.proxy_flags,
            self.proxy_list if isinstance(self.proxy_list, str) \
            else len(self.proxy_list)))

    def spider_closed(self, spider):
        self.pp.write()

    def map_proxy(self, p):
        #set_trace()
        try: # Can be either stripped or not
            return self.pp.master_plist[p]
        except KeyError:
            return self.pp.master_plist[self.plist_map[p]]

    def process_request(self, request, spider):
        def set_auth(request, proxy):
            if proxy.creds:
                request.headers['Proxy-Authorization'] = proxy.creds

        lg.debug('in process_request: {}, {}'.format(request, request.meta))

        pa=request.meta.pop('proxy_action', None)
        if pa == 'disable':
            self.pp.set_status(self.map_proxy(request.meta['proxy']), 'D')
            del request.meta['proxy'] # Make it pick another proxy
        elif pa == 'release':
            proxy=self.map_proxy(request.meta['proxy'])
            self.pp.release_proxy(proxy)
            raise IgnoreRequest
            
        # Don't overwrite with a random one (server-side state for IP)
        if 'proxy' in request.meta:
            proxy=self.map_proxy(request.meta['proxy'])
            set_auth(request, proxy)
            return # No fuss, we have a proxy already

        if self.mode == 'random':
            proxy = self.pp.get_proxy(True)        
        elif self.mode == 'sequential':
            proxy = self.pp.get_proxy()

        request.meta['proxy'] = proxy.p
        set_auth(request, proxy)

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

        if isinstance(exception, IgnoreRequest): return # No problem
        
        mode=request.meta.get('proxy_mode', self.mode) # Possible override
        if mode == 'once': return # Try once mode, quit here
            
        # Simple downvote first
        self.pp.set_status(self.map_proxy(request.meta['proxy']), None)

        # List of conditions when we retry. Some of them may disable the proxy
        if isinstance(exception, (
                ConnectionRefusedError, ConnectError, NoRouteError,
                ResponseFailed, TunnelError)):
            self.pp.set_status(self.map_proxy(request.meta['proxy']), 'D')
        elif isinstance(exception, (
                TimeoutError, TCPTimedOutError, ResponseFailed)):
            if 'T' in self.proxy_flags: # Disable on timeout
                self.pp.set_status(self.map_proxy(request.meta['proxy']), 'D')
        else: raise

        lg.warning('{} on %s'.format(type(exception)), request.url)
        del request.meta['proxy'] # Will pick new proxy on next request

        return request.replace(dont_filter = True)
