from logging import getLogger, ERROR
#from ipaddress import IPv4Address, AddressValueError
from contextlib import suppress
from csv import reader, writer
from collections import OrderedDict
from urllib.parse import urlparse
from base64 import b64encode
from random import choice
from pdb import set_trace

class NoProxiesException(Exception):
    pass

lg=getLogger(__name__)

class proxy:
    '''Proxy with state'''

    def __init__(self, p, flags=''):
        p=p.strip()

        self.tries=0 # Number of timeout retries
        self.wake_time=None
        
        # None: usual proxy, or before being filtered
        # 'O': the proxy will be used once, and then we'll have a result or an Exception (used for filtering).
        # 'G': good proxy (marked by filter)
        # 'B': bad proxy (marked by filter)
        # 'P': in addition to 'G', proven proxy. We've got something through this proxy.
        # 'D': Disable for life (for example we're over daily limit and no chance to sleep it off)
        # 'C': Chilling out, the proxy needs to rest, wake_time will tell when 
        self.flags=flags
        
        self.creds=None

        if p:
            if not p.startswith('http://'): p='http://'+p # Hack for now
            o=urlparse(p)

            if not o.port:
                raise ValueError('Problem with {}'.format(p))
            # TODO: Remove possible leading zeros from hostname (use IPv4Address)

            # If there's user/pass, make the header here
            if o.username:
                self.creds='Basic ' + b64encode((o.username+':'+o.password).encode()).decode()
                # Will turn port 03128 to 3128 among other things
                self.p='{0.scheme}://{0.username}:{0.password}@{0.hostname}:{0.port}'.format(o)
            else:
                self.p='{0.scheme}://{0.hostname}:{0.port}'.format(o)
        else:
            # Allow direct connections. Can be intermixed with normal (if we add
            # proxies to lower the risk of banning our IP for example, but still
            # want to use it)
            self.p=''
            
    def addflag(self, flag):
        self.flags+=flag
        
    def __repr__(self):
        return '{0.p} ({0.flags} {0.tries})'.format(self)

class proxypool:
    '''This class contains a list of proxies, with functions to manipulate this list. With time the list is sorted by usefulness of the proxy: bad proxies go to the end, good proxies stay in the beginning. The access is NOT thread-safe, suitable for aiohttp but the legacy version should use external lock.'''

    def update_replenish(self):
        self.replenish_at=datetime.now()+timedelta(seconds=self.replenish_interval)

    def load(self, arg):
        '''Always load all proxies, we'll be able to prune and rearrange later
    '''

        self.master_plist=OrderedDict()
        if isinstance(arg, str): # File name    
            lg.info('Loading {}'.format(arg))
            with suppress(FileNotFoundError), open(arg) as f:
                rd=reader(f, delimiter='\t')
                for row in rd: # (proxy, status)
                    status=row[1] if len(row) > 1 else ''
                    p=proxy(row[0], status)
                    self.master_plist[p.p]=p                
            lg.warning('Loaded {} {}'.format(arg,len(self.master_plist)))
        else: # Iterable of proxies
            for i in arg:
                p=proxy(i, 'G') # Mark as good
                self.master_plist[p.p]=p
            lg.warning('Loaded {} proxies from iterable'.format(len(self.master_plist)))

    def __init__(self, arg, max_retries=-1, replenish_interval=None,
                 replenish_threads=400, no_disable=False):
        '''Is initialised from arg. It can be iterable, string (treated as filename), or filter instance (to use for the proxies returned by gatherproxy)
If number of errors through the proxy is more than max_retries (and max_retries is not -1), the proxy will be disabled. If arg is None, work without proxy.

If there's only one proxy in the list, it's a special case. We don't disable this proxy.
replenish_interval: how many seconds must pass between replenishes from gatherproxy.
replenish_threads: how many threads to use when replenishing (now there's only gatherproxy, so it's a number of threads the farm will use
no_disable set to True is good for the providers like crawlera or proxyrack, they have one address of the proxy to use, and rotate proxies behind the scene.
'''
        self.max_retries=max_retries
        self.retry_no=0 # Retries of the disabled pool
        self.arg=arg # For the write()
        self.has_filter=False
        self.replenish_interval=replenish_interval
        self.replenish_threads=replenish_threads
        self.no_disable=no_disable
        
        if type(arg).__name__ == 'filter': # Ugly! But cannot import filter because of circular dependency
            self.has_filter=True
            self.replenish_at=datetime.now()
            self.retry_no=self.max_retries # To make it replenish right away
            self.master_plist=OrderedDict()
        else: # It's an iterable or a file name
            self.load(arg)
                
        if len(self.master_plist) > 1:
            lg.info( "{} proxies loaded".format(len(self.master_plist)))

    def active_number(self):
        '''Gets active (ie not disabled) proxies count'''
        return len(tuple(k for k,v in self.master_plist.items()
                         if v.flags not in ('D','B')))
    
    def set_status(self, p, st=None, *args):
        '''Sets the proxy status. Depending on it we may move the proxy inside the list

If st is None, it will put it into end of the list (to be unlikely picked by the next get_proxy) and remove proven status: we have somehow spoiled this proxy, let it rest.

If st is 'proven', prove the proxy. This proxy was ok to download through, set its flags as proven in master_plist. Move it closer to the head to be picked with priority

args vary depending on st. If it's chillout, the arg must be the time to wake up.
        '''
        # One proxy (or even less - for the filter), let it live how it is
        if len(self.master_plist) <= 1: return

        # Possible conversion from str to proxy (scrapy middleware)
        if not isinstance(p, proxy): p=self.master_plist[p]
        
        if not st:
            lg.debug('downvoting {}'.format(p))
            # We could have had replenish in between, so it's not there
            with suppress(KeyError): self.master_plist.move_to_end(p)
            p.flags=p.flags.replace('P','') # Remove proven flag
            p.tries+=1
            #set_trace()
            if self.max_retries != -1 and p.tries >= self.max_retries: st='D'
        elif st == 'P':
            p.tries=0 # Reset the tries counter
            if 'P' in p.flags: return # Already proven, no fuss
            
            self.master_plist[p.p]=p # It works in any case, don't lose it
            self.master_plist.move_to_end(p.p, False) # Move to the beginning
            p.flags=p.flags.replace('D','')+'P' # Remove disabled flag
        elif st == 'C':
            p.wake_time=args[0]
            p.flags+='C'

        if st == 'D':
            # TODO: maybe put it to rest?
            if not self.no_disable: p.flags='D'
            
        #lg.debug('master_plist: {}'.format(self.master_plist))
                
    def release_proxy(self, p):
        '''Returns the proxy back to the list. Good warm proxy, to be picked by
        next get_proxy().
        '''
        if not p: return # Proxy hasn't been initialised
        try:
            lg.debug('releasing {}'.format(p))
        except:
            pass # Strange error, happens on shutdown. Maybe lg is done with already?
        if self.plist:
            self.plist[p.p]=p
            self.plist.move_to_end(p.p, False) # Move to the beginning

    def get_proxy(self, random=False) -> 'proxy':
        '''Gets proxy from the beginning of the list. In fact we have two lists, one is master_list, all proxies that were read on initialisation, and working list, where we pluck the proxies from. When working list becomes empty, we replenish it again from the master_list.
Set random to True to get random proxy from the available proxies. 
'''
        def replenish(self):
            '''Replenish plist from master_plist. Several cases here'''

            #set_trace()
            active_num=self.active_number()
            #if not active_num and self.flt:
            if False: #  self.has_filter: # Doesn't work for now
                # First check if we have something in the disabled pool, we'll put them back into the main pool (master_plist) and see if they have repented. Up to max_retries times, otherwise they burn in hell.                
                if datetime.now() < self.replenish_at: #self.retry_no < self.max_retries:
                    if len(self.disabled):
                        lg.info('Putting back disabled set into master_plist')
                        self.master_plist=list(self.disabled|set(self.master_plist))
                        self.retry_no+=1
                else:
                    # Squash and then restore the farm messages
                    while True:
                        try:
                            gp_lst=self.gatherproxy()
                        except Exception as e:
                            #lg.warning('Exception in gatherproxy')
                            lg.exception('Exception in gatherproxy')
                            continue
                        if gp_lst is None:
                            lg.warning('Error in gatherproxy')
                            continue
                        break
                            
                    lg.warning('Filtering gp data: {}'.format(len(gp_lst)))
                    retr_lg=getLogger('retr.farm')
                    old_level=retr_lg.getEffectiveLevel()
                    retr_lg.setLevel(ERROR)

                    self.master_plist=list(map(proxy, self.arg.get_good(gp_lst, self.replenish_threads)))
                    self.disabled=set()
                    retr_lg.setLevel(old_level)
                    self.update_replenish()
                    #lg.warning('Finished replenishment: {} {} {}'.format(len(self.master_plist), len(self.disabled), len(self.plist)))

            self.plist=self.master_plist.copy()
            if len(self.master_plist) > 1: # Don't spam with trivial cases
                lg.info('Replenished from master_plist ({}/{}).'.format(
                    active_num, len(self.plist)))
            
        p=None
        #lg.debug( 'self.plist: {}'.format(len(self.plist) or 'Empty'))
        #lg.error('get_proxy: pp={}, plist={}, master_plist={}'.format(self, self.plist, self.master_plist))
        #lg.error('get_proxy: pp={}, master_plist={}'.format(self, self.master_plist))
        while True:
            try:
                if random:
                    proxy = choice(tuple(self.plist.keys()))
                    del plist[proxy]
                else:
                    dummy,p=self.plist.popitem(False) # Get from the beginning
                    if 'D' in p.flags:
                        lg.error('Got disabled proxy')
                    elif 'B' in p.flags:
                        pass # lg.warning('Got bad proxy')
                    else:
                        break
                    continue
            except (KeyError, AttributeError):
                if not len(self.master_plist) and not self.has_filter:
                    raise NoProxiesException
                replenish(self) # Local function
                continue
            # TODO
            # if 'C' in p.flags and ...:
            #    continue # Skip it
            break
        return p

    def gatherproxy(self):
        lg.info('Getting from gatherproxy.com')

        s=Session()
        parser = etree.HTMLParser()

        repl={'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
              'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'zero': '0',
              'minus': '-', 'plus': '+', 'multiplied': '*', 'x': '*', '=': '=' }
        prefix='http://gatherproxy.com'

        login_url=prefix+'/subscribe/login'

        r=s.get(login_url)
        
        tree = etree.fromstring(r.text, parser)
        q=tree.xpath('//span[@class="blue"]/text()')[0]
        lg.info(q)
        q=''.join(repl.get(i.lower(), i) for i in q.split()[:-1])
        a=eval(q)
        lg.info(a)

        r=s.post(login_url, data={
            'Username': "aikipooh+ot@gmail.com",
            'Password': "=[*[#.ON",
            'Captcha': a })    

        tree = etree.fromstring(r.text, parser)
        h=tree.xpath('//a[contains(@href,"downloadproxylist")]/@href')[0]
        r=s.get(prefix+h)

        #http://gatherproxy.com/proxylist/downloadproxylist/?sid=4579995
        sid=h.split('=')[-1]
        r=s.post(prefix+h, data={ 'ID': sid, 'C': '', 'P': '', 'T': '', 'U': '0'})
        
        return r.text.splitlines()
            
    def write(self, exclude_disabled=False):
        '''Write master proxy list to a file. New file will be created with the same
order as in master list, good proxies first, bad last. So it's advised to run it
before the program exit, for proxies to be sorted in better order.

Optionally we can remove disabled proxies from the saved file by setting exclude_disabled to True. Otherwise disabled will be written as good again.

        '''
        if not self.arg or type(self.arg) is not str: return
        
        lg.info('Writing new proxies to: '+self.arg)
        with open(self.arg, 'w') as f:
            # Put proven first
            for k, v in sorted(self.master_plist.items(),
                               key=lambda _: 'P' in _[1].flags):
                # Remove temporary flag (if it was there) and write
                flags=set(v.flags)
                if 'D' in flags:
                    if exclude_disabled:
                        continue
                    else: # Mark them good again
                        flags=flags-{'D'}+{'G'}
                flags-={'O', 'P'} # Remove technical marks
                f.write('{}\t{}\n'.format(v.p, ''.join(flags)))

# Several useful functions for filtering
def set_plist_for_filter(pp):
    global length, step
    # proxies file will be rewritten with marks ('B' and 'G'). If you need to
    # test new proxies, add them in the beginning, they'll be normalised and
    # OrderedDict will take care about retaining the marks.

    # You may make it work with bad proxies, marking them with O and running the
    # filter process. It may find new good proxies in there, because depending
    # on time of day different proxies may work, that's why it may be useful to
    # run the filter several times.

    new_plist=OrderedDict({k:v for k,v in pp.master_plist.items()
                           if v.flags == ''})
    if not new_plist: # There are no proxies with unknown status
        # Let's recheck all bad ones, leaving good ones as they are
        new_plist=OrderedDict({k:v for k,v in pp.master_plist.items()
                               if v.flags != 'G'})
    old_master_plist, pp.master_plist=pp.master_plist, new_plist        

    for v in pp.master_plist.values():
        v.addflag('O') # Set one time flag on them all
        v.flags=v.flags.replace('B', '') # Remove bad flag

    length=0
    if len(pp.master_plist) > 5000:
        step=1000
    elif len(pp.master_plist) > 500:
        step=100
    else:
        step=10

    return old_master_plist

def print_length():
    global length, step
    length+=1
    if not length % step: lg.warning(length)
