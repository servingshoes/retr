#!/usr/bin/python3

'''This module provides a class to run retrievers in several coroutines asynchronously.

See real world examples in the samples/.
'''

from asyncio import ensure_future, Queue, Task, CancelledError, \
    InvalidStateError
from os import _exit
from logging import getLogger

from .utils import CustomAdapter
from .retriever_async import ProxyException
# from pympler.tracker import SummaryTracker
# from pympler import asizeof

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})

class farm:
    num_extend_print=10 # Number of items to print on massive extends
    
    '''init() returns an object o, we'll call o.run(i), then del o in the end.
'''

    def __init__(self, it, init, num_tasks=100, loop=None, done_task=None):
        '''Initialise the data and start initial futures'''
        self.num_objects=num_tasks # How many objects we can create
        
        self.loop=loop
        if done_task: self.done_task=done_task
            
        #self.cond=Condition()
        self.init=init
        self.objects=Queue(num_tasks)
        self.args={} # Arguments by futures, they're redeemed on KeyboardInterrupt so we can restart the process at a later time
        
        self.extend(it) # Put initial number of tasks there
        #    self.loop.stop() # If there were no items, quit instantly

    def __del__(self):
        # TODO: Should delete objects
        # while True:
        #     o=await self.objects.get()
        #     del o
        return

    # def __enter__(self):
    #     return self
    
    # def __exit__(self, *exc):
    #     self.close()

    def done_task(self, future):
        '''Default one, we don't care about results'''
        # If only the current task (finished btw) has left
        if len(Task.all_tasks()) == 1: self.loop.stop()
        
    def extend(self, arr):
        '''Enqueues another bunch of items, returns the number enqueued'''
        
        for num, i in enumerate(arr):
            task=ensure_future(self.do(i, num))
            task.add_done_callback(self.done_task)
            self.args[task]=i
            #lg.debug('added task {} {}'.format(id(task), i))
        else: num=0
        
        if num > self.num_extend_print:
            lg.info('Item: and {} more.'.format(num-self.num_extend_print))
        lg.info('Queue len: {}'.format(len(Task.all_tasks())-1))
        return num
        
    async def do(self, i, num):
        '''We have a pool of retriever objects. First we get one (or wait until it's ready), then invoke o.do(). We can use this farm several times — just by extending it (the objects remain live) — to gather the results as many times as we want.
num is to count enqueued items, to evade much spam if we add thousands of items at once. We limit to first self.num_extend_print.
'''
        if num < self.num_extend_print:
            lg.info('Item: {}'.format(i))

        # Get the object, probably reusing
        if self.objects.empty() and self.num_objects:
            # We can create another one
            o=self.init()
            self.num_objects-=1
        else:
            o=await self.objects.get()
        
        try:
            # Handle the item with do() function of the class, passing
            # parameters depending on the nature of the argument: can be
            # presented as several arguments to make things easier. do()
            # function must return one result, that will be collected with
            # respective done_task()
            if isinstance(i, (tuple, list)): # Present as arguments
                res=await o.do(*i)
            else:
                res=await o.do(i)
        except CancelledError:
            lg.debug('do(): cancelled')
            return None
        except ProxyException as e:
            raise # Propagate to task_done()
        except:
            lg.exception('Exception in farm')
            res=None
        finally: # Return the object, so others may continue
            await self.objects.put(o) # We'll wait here to evade uncontrolled spawning
            del self.args[Task.current_task()] # Prune unneeded arguments

        return res

    async def stopall(self, future):
        lg.warning('In stopall')

        curtask=Task.current_task()
        res=[]
        for i in Task.all_tasks():
            lg.debug(i)
            if i == curtask: continue
            if i not in self.args:
                lg.debug('Not our task')
                continue
            res.append(self.args[i])
            try:
                result=i.result()
                lg.info('Future result: {}'.format(result))
            except InvalidStateError: # Was running
                i.cancel()
                lg.debug('after cancel')
                try:
                    result=await i
                    lg.debug('Res from await: {}'.format(result))
                except CancelledError:
                    lg.warning('cancel retrieved')
            except CancelledError:
                lg.critical('Task cancelled already?')
                _exit(1)
        future.set_result(res)
        
