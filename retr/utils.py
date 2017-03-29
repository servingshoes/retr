from logging import LoggerAdapter
from asyncio import Task

class CustomAdapter(LoggerAdapter):
    '''Print a footprint of task_id before the message to aid in debugging'''
    
    def process(self, msg, kwargs):
        # if 'extra' not in kwargs:
        #     kwargs['extra']={}
        # kwargs['extra']['connid']=id(Task.current_task())%(1<<32)
        return '[{:x}] {}'.format(id(Task.current_task())%(1<<16), msg), kwargs
