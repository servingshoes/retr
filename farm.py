#!/usr/bin/python

'''This module provides a class to run retrievers in several threads.

Least possible case would be:

f=farm(None, handle_letter, all_letters)
tuple(f.run())
pp.write() # Optional though
os._exit(0)

Where letter is extending retriever, and all_letters is an iterable to run do() of the class for each item in it. If num_threads is None, there will be len(it) threads, good for small arrays. pp is proxypool to serve retrievers, in this case it's global.
f.run() picks results yield-ed in do().
'''

from threading import Condition, Thread
import gc
from os import _exit
from logging import getLogger
from queue import Queue, Empty # Exception
from types import GeneratorType
from collections.abc import Iterable

lg=getLogger(__name__)

# from pympler.tracker import SummaryTracker
# from pympler import asizeof

class farm():
    '''init returns an object, we'll call o.run(i), then del o in the end.'''
    def __init__(self, num_threads, init, it, extendable=False, tn_tmpl=None,
                 reuse=None, handler=None):
        '''When extendable is True, it means that we need to leave threads ready in case the array is extended. Simpler code in the other case. reuse tells whether to stash worked sessions for future reuse. If it's a list, it's a global list of warm sessions. Handler is invoked each second, while the processing is postponed, so the function shouldn't take long to complete'''
        self.waiting=0
        self.arr=list(it) # If it was a generator for example. Make it real.
        self.extendable=extendable
        self.cond=Condition()
        self.num_threads=num_threads if num_threads else len(self.arr)
        lg.info("Total: {}".format(len(self.arr)))
        self.init=init
        self.tn_tmpl=tn_tmpl
        self.reuse=reuse
        self.handler=handler
        if type(reuse) is list:
            self.reuse_pool=reuse
        else:
            self.reuse_pool=None
            #if self.reuse: self.reuse_pool=[]
        
        self.q=Queue()

    def __del__(self):
        # Let the user delete them if it was user-supplied
        if type(self.reuse) is not list:
            if self.reuse:
                for i in self.reuse_pool: del i
            
    def extend(self, arr, end=False):
        '''If end is True, add to the end'''
        with self.cond:
            orig_len=len(self.arr)
            #if type(arr) is GeneratorType: arr=tuple(arr)
            if end or self.arr[-1] is not None:
                self.arr+=arr
            else: # When we're quitting
                self.arr[:0]=arr # Insert in the beginning, so poison pills won't get lost
            lg.info("Total after extend: {}".format(len(self.arr)))
            self.cond.notify( len(self.arr)-orig_len )

    def print_arr(self):
        '''Will be printed on farm exit. Beware to call it while the threads are running if you need the actual list.'''
        with self.cond:
            print(self.arr)
        
    def reusing(self):
        return type(self.reuse_pool) is list
    
    def handle_item(o, i):
        '''Handles the item with do() function of the class, passing parameters depending on the nature of the argument: can be presented as several arguments to make things easy. do() function must yield one or several results, that will be returned by farm.run()'''
        lg.info( 'Item: {}'.format(i) )
        if any(isinstance(i, j) for j in (tuple,list,set)): # Present as arguments
            yield from o.do(*i)
        else:
            yield from o.do(i)
        #lg.debug('do: {}'.format(res))
        #return res
        
    def do_extendable(self):
        '''When extendable is True, it means that we need to leave threads ready in case the array is extended. Otherwise we can quit right after do() has completed.'''
        o=self.init()

        while True:
            self.cond.acquire()
            self.waiting+=1
            lg.debug('waiting incremented: {}, len={}'.
                     format(self.waiting,len(self.arr)))
            if not len(self.arr):
                if self.waiting == self.num_threads:
                    # We would all wait for it, and there is noone to replenish
                    # it, finishing by adding poison pills
                    lg.info('Killing all')
                    self.arr+=[None]*(self.num_threads-1)
                    self.cond.notify(self.num_threads-1)
                    self.cond.release()
                    break
                else:
                    self.cond.wait() # Someone else will kill us

            lm=len(self.arr)
            if lm: # Another check for those who have left cond.wait()
                i=self.arr.pop()
            self.waiting-=1
            lg.debug('waiting decremented: '+str(self.waiting))
            self.cond.release()
            if not lm: continue # Someone has stolen our item, snap!

            if i is None: break # End of queue marker
            for j in farm.handle_item(o, i): self.q.put(j)

        del o

        self.q.put(None) # Mark the thread has finished
        lg.info("has finished")

    def do(self):
        '''if an item from the iterator is a tuple, we explode it to be arguments to the do(). Otherwise we pass it verbatim'''
        #tracker = SummaryTracker()
        
        o=None
        if self.reusing(): # Try to get warm session
            with self.cond:
                if len(self.reuse_pool): o=self.reuse_pool.pop()
                
        if not o: o=self.init()

        #lg.warning(len(self.arr))
        while True:
            with self.cond:
                if not len(self.arr): break
                i=self.arr.pop()

            if i is None: break # End of queue marker
            for j in farm.handle_item(o, i): self.q.put(j)

        #lg.error(asizeof.asizeof(o))
        if self.reusing():
            with self.cond: self.reuse_pool.append(o)
        else:
            del o # This will release proxy back to the pool

        self.q.put(None) # Mark the thread has finished
        lg.info("has finished")

        #tracker.print_diff()
                
    def run(self):
        tlist=[]
        for i in range(self.num_threads):
            tn=self.tn_tmpl.format(i) if self.tn_tmpl else None
            t=Thread(target=self.do_extendable if self.extendable else self.do, name=tn)
            tlist.append( t )
            t.start()

        cnt=self.num_threads # That many threads are running
        while True:
            try:
                res=self.q.get(timeout=1)
            except Empty:
                if self.handler: self.handler()
                continue # Go back to the get from normal q
            except KeyboardInterrupt:
                lg.warning( 'Trying to kill nicely, putting {} None'.format(cnt) )
                self.extend( [None]*cnt )
                try:
                    for i in tlist: i.join()
                except KeyboardInterrupt:
                    lg.warning( 'I obey.' )
                    break
                #_exit(0)
                
            #lg.warning('run: {}'.format(res))
            if res != None:
                yield res
                continue
            cnt-=1 # One thread has completed
            if not cnt: # All threads are completed
                lg.info('Completed, joining')
                for i in tlist: i.join()
                break

        #lg.error(asizeof.asizeof(self))
