#!/usr/bin/python

'''This module provides a class to check proxies for usability.

If you need a more sophisticated functionality, you may extend it and define the
function to setup the session, or to control the returned page for more than a
simple anchor. Anchor may be an iterable to test for several anchors, will work
if any of them are present.

Smallest possible use case would be:

    f=filter( url='http://www.bigbasket.com',
              anchor=('Best Online Grocery Store in India. Save Big on Grocery Shopping | bigbasket.com',),
              headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36'} )
    f.run(basedir)

Create add.txt in the proxies_dir and run it. After the script completes, there
will be three files: bad.txt, good.txt and timeout.txt. good.txt is better than
add.txt in terms of usability:)

'''

from logging import getLogger
import requests
import urllib.parse
import ipaddress
from os import _exit, access, R_OK
from .retriever import *
from .farm import *

from pdb import set_trace

lg=getLogger(__name__)

def normalise_proxy(i):
    'Removes leading zeros'
    p_raw=i.split(':')
    try:
        a=ipaddress.IPv4Address(p_raw[0])
    except ipaddress.AddressValueError:
        lg.critical('Problem with {}'.format(i))
        exit()
    return ':'.join((a.exploded,p_raw[1]))

def load_file(fn):
    s=set()
    try:
        with open(fn) as f:
            for i in f:
                p=normalise_proxy(i.strip())
                s.add(p)
        lg.warning('Loaded {} {}'.format(fn,len(s)))
    except FileNotFoundError:
        pass
    return s

def save_file(fn, s):
    lg.warning('Saving {} {}'.format(fn,len(s)))
    with open(fn,'w') as f:
        for i in s: f.write(i+'\n')

class handler(retriever):
    def __init__(self, par):
        super().__init__(par.pp, par.headers, timeout=par.timeout,
                         max_retries=par.max_retries)
        self.par=par

    def validate(self, url, r):
        '''Can be overridden in the custom class. Must return a tuple of result (res_ok if the proxy is good, res_nok otherwise) and reason. reason is one of: 'discard' — the proxy is discarded and won't get into the resulting sets, 'bad' — the proxy will get to the bad set.

I use discard to mark totally wrong proxies, say if the proxy itself reports an error, or needs authorisation. And bad for the proxies I still hope can be brought back to senses, maybe it's temporarily blocked or something.

This default method checks the status_code and the presence of anchor in the retrieved page.'''
        if r.status_code != 200:
            return res_nok, 'status_code {}'.format(r.status_code)

        if r:
            for i in self.par.anchor:
                if i in r.text:
                    lg.info( self.p[0]+' good' )
                    return res_ok, None
            else: # This is discarded.
                #lg.info(self.p[0]+' wrong response: {}'.format(r.text))
                return res_nok, 'discard'
        else:
            lg.info( self.p[0]+' download failed' )

        return res_nok, 'bad' # res_nok to quit request() right there

    def do(self, p):
        # The proxy is given, and we won't allow reinitialising from
        # master_plist (it's empty)
        self.p=[p, False]

        r=self.request('get', self.par.url,params=self.par.add_pars,
                       reinit=False)
        if type(r) is tuple:
            if r[1] == 'discard': return # Don't return at all
            
            yield 'p_timeout' if r[1] == 'timeout' else 'p_bad', p
        else:
            yield 'p_good', p
        
class filter():
    def __init__(self, url=None, anchor=[], add_pars={}, custom_handler=None,
                 max_retries=3, timeout=60,
                 headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0", 'Accept-Encoding': 'gzip, deflate, sdch'}):
        '''url — url to retrieve for checking
        anchor — the sequence of text strings to look for in the retrieved page. The page is valid if any of them is found in the downloaded page.
        add_pars — additional parameters to use for get() function
        custom_handler — class to base on handler, supersedes request functionality (don't forget to use reinit=False there). req() should return res,err.
        max_retries — retry the page that many times before considering it bad
        timeout — timeout to use in request_function
        headers — additional headers to use in requests
        '''
        self.add_pars=add_pars
        self.handler=custom_handler or handler
        self.url=url
        self.anchor=anchor
        self.max_retries=max_retries
        self.timeout=timeout
        self.headers=headers

    def run(self, basedir, num_threads=400):
        '''basedir — directory where the proxy lists are kept
If there is add.txt, it will process this file, adding to bad/good/timeout. If not, it will work with bad and timeout combined, possibly extracting new good proxies and rewriting all three files. Depending on time of the day different proxies may work, that's why it may be useful to run filter several times.'''

        self.p_good=load_file(basedir+'good.txt');
        self.p_bad=load_file(basedir+'bad.txt')
        self.p_timeout=load_file(basedir+'timeout.txt')

        self.p_work=self.p_bad|self.p_timeout

        if access(basedir+'add.txt', R_OK):    
            self.p_add=load_file(basedir+'add.txt')
            self.p_work=self.p_add-(self.p_work|self.p_good)
        else: # We're crunching bad and timeout sets, recreating them
            self.p_bad=set()
            self.p_timeout=set()

        lg.warning('{} to handle'.format(len(self.p_work)))

        if not len(self.p_work): exit()

        good_len=len(self.p_good)

        self.pp=proxypool(list(self.p_work))
        self.pp.master_plist=[] # Disable replenishing

        length=0
        if len(self.p_work) > 5000:
            step=1000
        elif len(self.p_work) > 500:
            step=100
        else:
            step=10

        # Argument is not needed, set to dummy
        f=farm(num_threads, lambda: self.handler(self), self.p_work)
        try:
            for s,p in f.run():
                length+=1
                if not length % step: lg.warning(length)
                getattr(self,s).add(p) # add to the respective set
        except KeyboardInterrupt: # Handle interrupt gracefully
            pass # Will write what we've got so far. Other will remain in the add.txt
        
        save_file(basedir+'good.txt', self.p_good)
        save_file(basedir+'bad.txt', self.p_bad)
        save_file(basedir+'timeout.txt', self.p_timeout)

        lg.warning('{} {}'.format(good_len,len(self.p_good)))

        _exit(0)
