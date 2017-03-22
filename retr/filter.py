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
will be two files: bad.txt and good.txt. good.txt is better than add.txt in
terms of usability:)

'''

from logging import getLogger
from contextlib import suppress
from ipaddress import IPv4Address, AddressValueError
from os import _exit, access, R_OK

from pdb import set_trace

from .retriever import retriever, ValidateException
from .proxypool import proxypool, proxy, NoProxiesException
from .farm import farm

lg=getLogger(__name__)

def normalise_proxy(i):
    'Removes leading zeros'
    p_raw=i.split(':')
    try:
        a=IPv4Address(p_raw[0])
    except AddressValueError:
        lg.critical('Problem with {}'.format(i))
        exit()
    return ':'.join((a.exploded,p_raw[1]))

def load_file(fn):
    s=set()
    with suppress(FileNotFoundError), open(fn) as f:
        for i in f:
            p=normalise_proxy(i.strip())
            s.add(p)
        lg.warning('Loaded {} {}'.format(fn,len(s)))
    return s

def save_file(fn, s):
    lg.warning('Saving {} {}'.format(fn,len(s)))
    with open(fn,'w') as f:
        for i in s: f.write(i+'\n')

class handler(retriever):
    def __init__(self, par):
        retriever.__init__(self, par.pp, **par.kwargs)
        self.par=par

    def validate(self, url, r):
        '''Can be overridden in the custom class. Must either pass if the proxy is good, or raise a ValidateException with the following arguments:
 ('discard', reason) — the proxy is discarded and won't get into the resulting sets,
 ('bad', reason) — the proxy will get to the bad set.

I use discard to mark totally wrong proxies, say if the proxy itself reports an error, or needs authorisation. And bad for the proxies I still hope can be brought back to senses, maybe it's temporarily blocked or something.

This default method checks the status_code and the presence of anchor in the retrieved page.'''
        if r.status_code != 200:
            raise ValidateException('bad', 'status_code {}'.format(r.status_code))

        if r:
            for i in self.par.anchor:
                if i in r.text: return
            else: # This is discarded.
                #lg.info(self.p.p+' wrong response: {}'.format(r.text))
                raise ValidateException('discard', 'noanchor', r)

        raise ValidateException('bad', 'download failed')

    def save_result(self, p, r):
        '''To help in debugging: saves the result of checking the proxy'''
        if not access('files', R_OK): return
        
        with open('files/'+p+'.html','w') as f: f.write(r.text)

    def _req(self, p, *args, **kwargs):
        try:
            r=self.request(*args, **kwargs)
            self.save_result(p, r) # there is r.text, store it
        except ValidateException as e:
            tp, reason, *args = e.args

            if reason == 'noanchor':
                self.save_result(p, args[0]) # there is r in the third arg

            return None, 'p_bad'
        except NoProxiesException:
            return None, 'p_bad'
        except KeyboardInterrupt:
            return None, None
        
        return r, None

    def do(self, p):
        # The proxy is given, and we won't allow reinitialising from
        # master_plist (proxypool is empty)
        set_trace()
        self.pp=proxypool([p])
        self.pick_proxy() # Will create the session and all that
        self.pp=proxypool([])

        r,s=self._req(p, 'get', self.par.url, params=self.par.add_pars)
        if r is None:
            if s is not None: yield s, p
            return                

        yield 'p_good', p
        
class filter:
    def __init__(self, url=None, anchor=[], add_pars={}, custom_handler=None,
                 **kwargs):
        '''url — url to retrieve for checking
        anchor — the sequence of text strings to look for in the retrieved page. The page is valid if any of them is found in the downloaded page.
        add_pars — additional parameters to use for get() function
        custom_handler — class to base on handler, supersedes request functionality (don't forget to use reinit=False there). req() should return res,err.

        Also all the named parameters of retriever are supported and passed
        intact to it.

        '''
        self.url=url
        self.anchor=anchor
        self.add_pars=add_pars
        self.handler=custom_handler or handler

        self.kwargs=kwargs
        if 'headers' not in kwargs:
            kwargs['headers']={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0',
                'Accept-Encoding': 'gzip, deflate, sdch'}

    def _run(self, num_threads):
        '''self.p_* are setup, run the requests'''
        self.pp=None

        length=0
        if len(self.p_work) > 5000:
            step=1000
        elif len(self.p_work) > 500:
            step=100
        else:
            step=10

        f=farm(num_threads, lambda: self.handler(self), self.p_work)
        with suppress(KeyboardInterrupt): # Handle interrupt gracefully
            for s,p in f.run():
                length+=1
                if not length % step: lg.warning(length)
                if s == 'p_good': print(p, 'good')
                getattr(self,s).add(p) # add to the respective set
        # Will write what we've got so far. Other will remain in the add.txt
    
    def run(self, basedir, num_threads=400):
        '''basedir — directory where the proxy lists are kept
If there is add.txt, it will process this file, adding to bad/good. If not, it will work with bad, possibly extracting new good proxies and rewriting two resulting files. Depending on time of day different proxies may work, that's why it may be useful to run filter several times.'''

        self.p_good=load_file(basedir+'good.txt');
        self.p_bad=load_file(basedir+'bad.txt')

        self.p_work=self.p_bad

        if access(basedir+'add.txt', R_OK):
            self.p_add=load_file(basedir+'add.txt')
            self.p_work=self.p_add-(self.p_work|self.p_good)
        else: # We're crunching bad set, recreating it
            self.p_bad=set()

        lg.warning('{} to handle'.format(len(self.p_work)))

        if not len(self.p_work): exit()

        good_len=len(self.p_good)

        self._run(num_threads)
        
        save_file(basedir+'good.txt', self.p_good)
        save_file(basedir+'bad.txt', self.p_bad)

        lg.warning('{} {}'.format(good_len,len(self.p_good)))

        _exit(0)

    def get_good(self, p_work, num_threads=300):
        self.p_work=p_work
        self.p_bad=set()
        self.p_good=set()
        
        self._run(num_threads)

        return self.p_good
