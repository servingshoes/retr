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

from threading import Condition, Thread, Barrier, BrokenBarrierError
import gc
from os import _exit
from logging import getLogger
from queue import Queue, Empty # Exception
from types import GeneratorType
from itertools import repeat, chain
from contextlib import suppress
from time import sleep

lg=getLogger(__name__)

# from pympler.tracker import SummaryTracker
# from pympler import asizeof

class farm:
    '''init returns an object, we'll call o.run(i), then del o in the end.'''
    def __init__(self, num_threads, init, it, extendable=False, tn_tmpl=None,
                 reuse=None, handler=None):
        '''When extendable is True, it means that we need to leave threads ready in case the input list is extended (with extend()). Also the run() function can be invoked several times. The drawback is that underneath the input it is converted to a list, and then manipulated, so it's feasible to start with relatively small input data. Don't forget to call close(), it will clean up all the threads. Or you can use it as a context manager, so this will be called automatically upon __exit__().
If extendable is False, the code is simpler, but it supports iterables however big.
tn_tmpl is format() template with {} to be changed to thread's number.
 reuse tells whether to stash worked sessions for future reuse. If it's a list, it's a global list of warm sessions.
A function in the handler parameter is invoked each second, while the processing is postponed, so it shouldn't take long to complete'''
        self.waiting=0
        self.extendable=extendable

        if self.extendable:
            self.arr=list(it) # If it was a generator for example. Make it real.
            lg.info("Total: {}".format(len(self.arr)))
        else: # Otherwise leave it as it is, we'll treat it as iterator
            self.arr=iter(it)
            
        self.cond=Condition()
        self.num_threads=num_threads if num_threads else len(self.arr)
        # Barrier to wait on for the restart (when extendable)
        self.barr=Barrier(self.num_threads+1)
        self.init=init
        self.tn_tmpl=tn_tmpl
        self.reuse=reuse
        self.handler=handler
        # Objects living within threads, used to signal them to quit
        # (by setting quit_flag)
        self.objects=[]
        if type(reuse) is list:
            self.reuse_pool=reuse
        else:
            self.reuse_pool=None
            #if self.reuse: self.reuse_pool=[]
        
        self.q=Queue()
        
        self.tlist=[]

    def __del__(self):
        # Let the user delete them if it was user-supplied
        if type(self.reuse) is not list:
            if self.reuse:
                for i in self.reuse_pool: del i

    def close(self):
        # Join our threads
        if self.extendable:
            lg.debug('Cancelling threads (break the barrier)')
            self.barr.abort() # Let those waiting on the barrier to quit
        
        lg.info('Joining threads')
        for i in self.tlist: i.join()
        lg.info('Finished joining threads')

    def __enter__(self):
        return self
    
    def __exit__(self, *exc):
        self.close()

    def extend(self, arr, end=False):
        '''If end is True, add to the end'''
        if not self.extendable:
            lg.error('The farm is not extendable, "extendable" parameter is False')
            return
        
        with self.cond:
            orig_len=len(self.arr)
            #if type(arr) is GeneratorType: arr=tuple(arr)
            if end or self.arr and self.arr[-1] is not None:
                self.arr+=arr
            else: # When we're quitting
                self.arr[:0]=arr # Insert in the beginning, so poison pills won't get lost
            lg.info("Total after extend: {}".format(len(self.arr)))
            #lg.info("Notifying: {}".format(len(self.arr)-orig_len))
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
        if isinstance(i, (tuple,list,set)): # Present as arguments
            yield from o.do(*i)
        else:
            yield from o.do(i)
        #lg.debug('do: {}'.format(res))
        #return res
        
    def do_extendable(self):
        '''We need to leave threads ready in case the array is extended. Otherwise we can quit right after do() has completed. Also we can use this farm several times, the objects remain live, so we can extend and invoke another run() to gather the results as many times as we want.'''
        o=self.init()
        with suppress(AttributeError):
            if not o.f: o.f=self # Set to the current farm
        with self.cond: self.objects.append(o)
            
        while True:
            self.cond.acquire()
            
            self.waiting+=1
            lg.debug('waiting incremented: {}, len={}'.
                     format(self.waiting,len(self.arr)))
            if not len(self.arr):
                if self.waiting == self.num_threads:
                    # No threads left to replenish the array, we should all quit
                    # Adding poison pills
                    if 1:
                        lg.info('Killing all')
                        # Put poison pills for everyone including us
                        self.arr+=[None]*(self.num_threads)
                        self.cond.notify(self.num_threads) # Wake up other threads
                    else:
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

            if i is None:
                self.q.put(None) # Mark we're done
                # Sleep on the condition to let other threads get their pills
                lg.debug('Sleeping on barrier')
                try:
                    self.barr.wait()
                except BrokenBarrierError: # We're to quit
                    break
                lg.debug('Continuing after barrier')
                continue # then restart processing the queue
            for j in farm.handle_item(o, i): self.q.put(j)

        with self.cond: self.objects.remove(o)
        del o

        lg.info("has finished")

    def do(self):
        '''if an item from the iterator is a tuple, we explode it to be arguments to do(). Otherwise we pass it verbatim'''
        #tracker = SummaryTracker()
        
        o=None
        if self.reusing(): # Try to get warm session
            with self.cond:
                if len(self.reuse_pool): o=self.reuse_pool.pop()
                
        if not o: o=self.init()
        with suppress(AttributeError):
            if not o.f: o.f=self # Set to the current farm
        with self.cond: self.objects.append(o)

        #lg.warning(len(self.arr))
        while True:
            with self.cond:
                try:
                    i=next(self.arr)
                except StopIteration: # empty
                    break                    

            if i is None: break # End of queue marker
            for j in farm.handle_item(o, i): self.q.put(j)

        #lg.error(asizeof.asizeof(o))
        with self.cond:
            self.objects.remove(o)
            if self.reusing():
                self.reuse_pool.append(o)
            else:
                del o # This will release proxy back to the pool
            
        self.q.put(None) # Mark the thread has finished
        lg.info("has finished")

        #tracker.print_diff()

    def cancel(self, cnt=0):
        '''Cancels all the threads in the farm'''
        # Put poison pills and then signal the threads to stop
        if not cnt: cnt=self.num_threads # That many threads are running

        with self.cond:
            if self.extendable:
                self.arr+=[None]*cnt
            else:
                self.arr=chain(repeat(None,cnt), self.arr)
            self.cond.notify( cnt )
            for _ in self.objects: _.quit_flag=True
        
    def run(self):
        '''Main function to invoke. When KeyboardInterrupt is received, it sets the quit_flag in all the objects present, retrievers then raise an exception. It's the problem of the do() function to handle it and possibly extend the main list with the item that wasn't handled to show it in the end (for possible restart)
self.handler() function is invoked each second if there are no items in the queue to allow for some rudimental auxiliary activity'''
        # Now start the threads, only once
        if self.tlist:
            lg.debug('Restarting threads')
            self.barr.wait()
        else:
            for i in range(self.num_threads):
                tn=self.tn_tmpl.format(i) if self.tn_tmpl else None
                t=Thread(target=self.do_extendable if self.extendable else self.do,
                         name=tn)
                self.tlist.append(t)
                t.start()

        cnt=self.num_threads # That many threads are running

        while True:
            try:
                res=self.q.get(timeout=1)
            except Empty:
                if self.handler: self.handler()
                continue # Go back to suck the queue
            except KeyboardInterrupt:
                lg.warning( 'Trying to kill nicely, putting {} None'.format(cnt) )
                self.cancel(cnt)
                res=None
                
            #lg.warning('run: {}'.format(res))
            if res != None:
                yield res
                continue
            cnt-=1 # One thread has completed
            if not cnt: # All threads are completed (but may still be processing the quit_flag), we'll join them in close()
                return

        #lg.error(asizeof.asizeof(self))
