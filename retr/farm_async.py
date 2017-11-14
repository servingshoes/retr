#!/usr/bin/python3

'''This module provides a class to run retrievers in several coroutines asynchronously.

See real world examples in the samples/.
'''

from asyncio import ensure_future, wait, Queue, Task, Semaphore, \
    CancelledError, InvalidStateError
from os import _exit
from contextlib import suppress
from logging import getLogger

from pdb import set_trace

from .utils import CustomAdapter
from .retriever_async import ProxyException
# from pympler.tracker import SummaryTracker
# from pympler import asizeof

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})

class farm:
    '''init() returns an object o, we'll call o.run(i), then del o in the end.
'''

    def __init__(self, it, init, num_tasks=100, loop=None, done_task=None):
        '''Initialise the data and start initial futures'''
        self.num_objects=num_tasks # How many objects we can create
        
        self.loop=loop
        if done_task: self.done_task=done_task
            
        self.init=init
        self.objects=Queue(num_tasks, loop=loop) # Birth control device
        self.args={} # Arguments by futures, they're redeemed on KeyboardInterrupt so we can restart the process at a later time

        self.toget=Queue(loop=loop)
        self.initial_toget=it
        # The task that feeds the working queue
        self.extender_task=ensure_future(self.extender())
        self.args[self.extender_task]='extender'

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

    async def grimreaper(self):
        lg.debug('In grimreaper')

        await self.toget.join()

        # Extender's to die too
        self.extender_task.cancel()
        with suppress(CancelledError):
            await wait([self.extender_task], loop=self.loop) # Discard

        lg.debug('Closing the loop')
        self.loop.stop()

        lg.debug('Out grimreaper')

    async def extender(self):
        '''Enqueues another bunch of items, returns the number enqueued'''

        # Putting initial requests array
        await self.extend(self.initial_toget)

        # Now start the task which will wait for all the items to be processed
        self.grimreaper_task=ensure_future(self.grimreaper())
        self.args[self.grimreaper_task]='grimreaper'

        while True:
            lg.debug('Extender before item get')
            try:
                i=await self.toget.get() # Get the item to process
            except CancelledError:
                lg.debug('Quitting extender')
                return
            
            # Get the object, probably reusing
            if self.objects.empty() and self.num_objects:
                # We can create another one
                o=self.init()
                self.num_objects-=1
            else:
                try:
                    o=await self.objects.get()
                except CancelledError:
                    lg.debug('Returning retrieved item back to toget: {}'.format(i))
                    await self.toget.put(i)
                    return # Leave the routine right here

            lg.debug('Extender after objects.get()')
            task=ensure_future(self.do(o, i))
            task.add_done_callback(self.done_task)
            self.args[task]=i
            #lg.debug('added task {} {}'.format(id(task), i))
        
    async def extend(self, arr):
        for i in arr: await self.toget.put(i)
        lg.info('Queue len: {}'.format(self.toget.qsize()))
        
    async def do(self, o, i):
        '''We have a pool of retriever objects. First we get one (or wait until it's ready), then invoke o.do(). We can use this farm several times — just by extending it (the objects remain live) — to gather the results as many times as we want.
'''
        lg.info('Item: {}'.format(i))

        interrupted=False
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
        except CancelledError: # Don't discard the args of these two exceptions
            lg.debug('do(): cancelled')
            interrupted=True
            return None
        except KeyboardInterrupt:
            lg.debug('do(): keyboard interrupt')
            interrupted=True
            raise
        except ProxyException as e:
            raise # Propagate to task_done()
        except:
            lg.exception('Exception in farm')
            res=None
        finally: # Return the object, so others may continue
            await self.objects.put(o) # We'll wait here to evade uncontrolled spawning
            self.toget.task_done()
            if not interrupted: # Leave to retrieve
                del self.args[Task.current_task()] # Prune unneeded arguments

        return res

    async def deplete_queue(self, loop):
        q=[]
        while not self.toget.empty(): q.append(await self.toget.get())

        return q
        
    def stopall(self, loop):
        '''Cancels all tasks and gets puts unretrieved input items back into the queue''' 
        lg.warning('In stopall')

        results=[]
        cancelled=[] # Tasks cancelled, we'll wait for their return
        # TODO: Need to cancel only our threads!
        for i in Task.all_tasks():
            lg.debug('TASK {:04x}: {}'.format(id(i)%(1<<16), i))
            if i.done(): # Finished
                try:
                    res=i.result()
                    lg.debug('RESULT: {}'.format(res))
                    results.append(res)
                except KeyboardInterrupt: # The task that got interrupt
                    lg.debug('INTERRUPTED: {}'.format(self.args.get(i)))
                continue

            #res.append(self.args[i])
            i.cancel()
            cancelled.append(i)

        print('CANCELLED: {} {}'.format(len(cancelled), len(Task.all_tasks())))
        for i in cancelled:
        #for i in Task.all_tasks():
            lg.debug('TASK {:04x}: {}'.format(id(i)%(1<<16), i))
            try:
                res=loop.run_until_complete(i)
            except KeyboardInterrupt:
                lg.debug('after ruc: interrupt: {}'.format(self.args.get(i)))
                continue
            except CancelledError:
                lg.debug('after ruc: cancel')
                continue
            lg.info('AFTER RUC: {}'.format(res))
            results.append(res)

        # Results have been through the done_task already, so actually there's no need in them
        if 0:
            lg.info('results: {}, args: {}'.format(len(results), len(self.args)))
        else:
            lg.debug('results: {}, args: {}'.format(results, self.args))
        lg.info('toget: {}'.format(self.toget.qsize()))

        toget_new=[v for k, v in self.args.items()
                   if k not in (self.grimreaper_task, self.extender_task)]

        lg.info('Retrieving queue')
        task=ensure_future(self.deplete_queue(loop))
        res=loop.run_until_complete(task)
        toget_new.extend(res)

        return results, toget_new

