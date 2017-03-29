#!/usr/bin/python3

from logging import getLogger

from .proxypool_common import proxypool as pp_common
from .utils import CustomAdapter

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})

class proxypool(pp_common):
    pass # One to one the parent class, no locks                    
    
