#!/usr/bin/python3

from threading import Lock
from logging import getLogger, ERROR
from time import sleep
from datetime import datetime, timedelta

from requests import Session
from lxml import etree

#from .filter import filter
from .proxypool_common import proxypool as pp_common

lg=getLogger(__name__)
    
class proxypool(pp_common):
    '''This class contains a list of proxies, with functions to manipulate this list. With time the list is sorted by usefulness of the proxy: bad proxies go to the end, good proxies stay in the beginning. The access is thread-safe.'''

    def update_replenish(self):
        self.replenish_at=datetime.now()+timedelta(seconds=self.replenish_interval)

    def __init__(self, arg=None, max_retries=-1, replenish_interval=None,
                 replenish_threads=400, no_disable=False):
        self.lock=Lock()
        super().__init__(arg, max_retries, replenish_interval,
                         replenish_threads, no_disable)
        
    def set_status(self, p, st=None, *args):
        with self.lock: super().set_status(p, st, *args)
                
    def release_proxy(self, p):
        with self.lock: super().release_proxy(p)

    def get_proxy(self) -> 'proxy':
        with self.lock:
            return super().get_proxy()
            
    def write(self, exclude_disabled=False):
        with self.lock:
            super().write(self, exclude_disabled)
