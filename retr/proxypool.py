#!/usr/bin/python3

from threading import Lock
from logging import getLogger, ERROR
from datetime import datetime, timedelta

from .proxypool_common import proxypool as pp_common

lg=getLogger(__name__)
    
class proxypool(pp_common):

    def __init__(self, *args, **kwargs):
        self.lock=Lock()
        super().__init__(*args, **kwargs)
        
    def set_status(self, *args, **kwargs):
        with self.lock:
            super().set_status(*args, **kwargs)
                
    def release_proxy(self, *args):
        with self.lock:
            super().release_proxy(*args)

    # def active_number(self):
    #     with self.lock: super().active_number()

    def get_proxy(self) -> 'proxy':
        with self.lock:
            return super().get_proxy()
            
    def write(self, **kwargs):
        with self.lock:
            super().write(**kwargs)
