#!/usr/bin/python3
# aikipooh@gmail.com
'''
This searches google for the linkedin profiles for the given name. pubdir.lst has search queries, one per line. For example: "John Smith" site:www.linkedin.com
Uses async version of retr
'''

from logging import basicConfig, getLogger, DEBUG, INFO, WARNING, ERROR
from os import path, access, R_OK, makedirs
from asyncio import Task, ensure_future, Future, CancelledError, get_event_loop
from urllib.parse import urlparse, parse_qs, urlsplit
from pdb import set_trace

from lxml import etree

from retr.utils import CustomAdapter
from retr.farm_async import farm
from retr.retriever_async import retriever, ValidateException, ProxyException
from retr.proxypool_common import proxypool

ll=WARNING
ll=INFO
ll=DEBUG

basicConfig( format='{asctime} {module:15s}: {message}', datefmt="%H:%M:%S",
             style='{', level=ll)

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})

num_tasks=2 #200
use_cache=1
infile='pubdir.lst'

pp=proxypool('proxyrack1_pass.lst')
    
class google(retriever):
    def __init__(self):
        etree.set_default_parser(etree.HTMLParser())
        ua='Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0'
        super().__init__(pp, {'User-Agent': ua}, timeout=180)
        
    @staticmethod
    def makefn(q, prefix, postfix):
        fn='{0}/{1[0]}/{1}.{2}'.format(prefix, q.replace('"',''), postfix)[:200]
        # print(prefix, postfix, fn, path.dirname(fn), sep='\t')
        if not access(path.dirname(fn), R_OK): makedirs(path.dirname(fn))
        
        return fn

    async def validate(self, r):
        # We'll use this decoded content anyway
        self.content=(await r.content.read()).decode()
    
        super().validate(r) # Check only status code

        # No check for good content for now.

        # if not self.content[-100:].rstrip().endswith('</html>'):
        #     raise ValidateException ('retry', 'truncated')

    async def do(self, url):
        if url.startswith('/search'): # One of the pages
            o=urlsplit(url)
            qs=parse_qs(o.query, True)

            start=int(qs['start'][0])
            q=qs['q'][0]
        else: # Initial string
            q=url
            url='/search?q={}&newwindow=1&client=firefox-b&num=100&start=0'.format(url)
            start=0
        fn=self.makefn(q+' {:0.0f}'.format(start/10), 'tmp/google_pd', 'html')

        #set_trace()
        try:
            with open(fn) as ff: self.content=ff.read()
            lg.debug('Found {}'.format(q))
        except:
            #await aio_sleep(60) # This way we'll probably not trigger the rate limit (40 seems ok too)
            lg.debug('Getting {}'.format(q))
            
            r=await self.request('get', 'https://www.google.ru'+url)

            if use_cache:
                with open(fn,'w') as ff: ff.write(self.content)
            r.close()

        tree=etree.fromstring(self.content)
        # start: 0. Will give 9 additional pages
        # start: 90. Will give 4 additional pages (to 130), etc
        if not start or (start >= 90 and not (start-10) % 40):
            pages=tree.xpath('//table[@id="nav"]//td[@class="cur"]/following-sibling::td[not(@class)]/a/@href')
            #for i in toext: print(i)
            f.extend(pages)

        #print(tree.xpath('//div[@id="resultStats"]/text()')[0])        
        return [_ for _ in tree.xpath('//div[@class="g"]//h3/a/@href')
                if _.startswith('https://www.linkedin.com/in/')]

loop=get_event_loop()

def done_task(future):
    try:
        it=future.result()
    except CancelledError:
        lg.warning('main done_task: cancel retrieved')
        it=None

    lg.debug('done_task: {}'.format(it))
    for i in it or []: # May encounter None here, let's do nothing
        fo.write(i+'\n')
    # All tasks are done one way or another, we can stop
    if all(map(Future.done, Task.all_tasks())): loop.stop()

with open(infile) as fi, open('output.lst','w') as fo:
    toget=fi.read().splitlines()
    f=farm(toget, google, num_tasks, loop, done_task=done_task)
    try:
        loop.run_forever()
        res=[]
    except KeyboardInterrupt:
        future = Future()
        try:
            ensure_future(f.stopall(future))
        except:
            lg.exception('upper')
        loop.run_until_complete(future)
        res=future.result()

exit()
old_master_plist.update(pp.master_plist) # Maybe flags have changed?
pp.master_plist=old_master_plist
pp.write()
