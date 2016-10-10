#!/usr/bin/python3

from threading import Lock
from logging import getLogger
from time import sleep

lg=getLogger(__name__)

# status of the proxy
st_proven=1 # We've got something through this proxy
st_disabled=2 # Disable for life (for example we're over daily limit and no chance to sleep it off)

class proxypool():
    '''This class contains a list of proxies, with functions to manipulate this list. With time the list is sorted by usefulness of the proxy: bad proxies go to the end, good proxies stay in the beginning. The access is thread-safe.'''
    
    def __init__(self, mlist:list=None, fn:str=None):
        '''Can be initialised from the list (mlist) or from the file (fn)'''
        self.master_plist=[]
        self.lock=Lock()
        self.fn=fn
        if mlist:
            self.master_plist=[ [key,0] for key in mlist ]
        else:
            lg.info( 'Loading '+fn )
            with open(self.fn) as f:
                for i in f:
                    a=i.strip()
                    val=[a,0] # Status is neutral
                    # Prune duplicates
                    if val not in self.master_plist:
                        self.master_plist.append(val) 
        self.plist=self.master_plist.copy()

        lg.info( "{} proxies loaded".format(len(self.master_plist)))
                    
    def set_status(self, p, st=None):
        '''Sets the proxy status. Depending on it we may move the proxy within the list

If st is None, it will put it into end of the list (to be unlikely picked by the next get_proxy) and remove proven status: we have somehow spoiled this proxy, let it rest.

If st is st_proven, prove the proxy. This proxy was ok to download through, set its state as proven in master_plist. Move it closer to the head to be picked with priority
        '''
        with self.lock:
            if not st:
                lg.debug('downvoting {}'.format(p))
                self.master_plist.remove(p)
                self.master_plist.append(p) # Put it to the end
                p[1]=0
            elif st == st_proven:
                if p[1] == st_proven: return # Already proven
                # Moving it closer to the beginning
                if p in self.master_plist:
                    self.master_plist.remove(p)
                    #logging.error('inserting {}'.format(p))
                    self.master_plist.insert(0, p)
                p[1]=st_proven
            elif st == st_disabled:
                p[1]=st_disabled
                
    def release_proxy(self, p):
        '''Returns the proxy back to the list. Good warm proxy, to be picked by
        next get_proxy().
        '''
        if not p: return # Proxy wasn't initialised
        with self.lock:
            lg.debug('releasing {}'.format(p))
            if self.plist: self.plist.insert(0, p)

    def get_proxy(self) -> 'proxy':
        '''Gets proxy from the beginning of the list. In fact we have two lists, one is master_list, all proxies that were read on initialisation, and working list, where we pluck the proxies from. When working list becomes empty, we replenish it again from the master_list.'''
        p=None
        with self.lock:
            lg.debug( 'self.plist: {}'.format(len(self.plist) if self.plist else 'Empty'))
            while True:
                try:
                    p=self.plist.pop(0)
                except (IndexError,AttributeError):
                    if not len(self.master_plist):
                        lg.info( "No more proxies left" )
                        break
                    # There was a proxy list, but we change proxy. Useful in
                    # case of 1 proxy list, we'll wait after encountering a
                    # problem with this sole proxy.
                    if self.plist!=None and len(self.master_plist) == 1:
                        lg.info('sleeping')
                        sleep(60)
                    self.plist=self.master_plist.copy()
                    lg.info('Replenished from master_plist ({}/{})'.format(
                        len(tuple(i for i in self.plist if i[1] != st_disabled)),
                        len(self.plist)))
                    continue
                if p[1] == st_disabled: continue # Skip it
                break
        return p
            
    def write(self, exclude_disabled=False):
        '''Write master proxy list to a file. New file will be created with the same
order as in master list, good proxies first, bad last. So it's advised to run it
before the program exit, for proxies to be sorted in better order.

Optionally we can remove disabled proxies from the saved file but setting exclude_disabled to True

        '''
        if not self.fn: return
        lg.info('Writing new proxies to: '+self.fn)
        with open(self.fn,'w') as f, self.lock:
            # Put True first, they're proven to work
            for i in sorted(self.master_plist, key=lambda i:i[1], reverse=True):
                if not exclude_disabled or i[1] != st_disabled:
                    f.write("{}\n".format(i[0]))
