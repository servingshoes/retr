#!/usr/bin/env python3
#aikipooh@gmail.com

# Sample of the async scraper.

from logging import basicConfig, getLogger, DEBUG, INFO, WARNING, ERROR
from os import access, R_OK, mkdir
from contextlib import suppress
from sys import argv
from math import ceil
from asyncio import get_event_loop, set_event_loop, new_event_loop, wait, \
    sleep as aio_sleep, ensure_future, Future, CancelledError
from urllib.parse import urlparse, parse_qs, urlencode, urlsplit, urlunsplit
from pdb import set_trace
from csv import DictWriter

from lxml import etree

from retr.utils import CustomAdapter
from retr.farm_async import *
from retr.retriever_async import retriever, ValidateException, ProxyException
from retr.proxypool_common import proxypool

ll=WARNING
ll=INFO
#ll=DEBUG

basicConfig( format='{asctime} {module:15s}: {message}', datefmt="%H:%M:%S",
             style='{', level=ll)

lg=getLogger(__name__)
lg=CustomAdapter(lg, {})

input_fn='toget.lst'
output_fn='output.csv'
num_tasks=100
download_only=0

#pp=proxypool(['54.94.149.34:80'])
do_filter=0 # To filter proxies
use_cache=1 # Cache downloaded files. Good idea, but takes about 30G of space
#set_trace()
pp=proxypool(['']) # No proxy
#pp=proxypool('proxies.csv')

# ------------ NO CONFIGURATION PAST THIS LINE ------------------

if do_filter:
    from retr.proxypool_common import set_plist_for_filter, print_length
    use_cache=0
    old_master_plist=set_plist_for_filter(pp)
    toget=['https://yellow.local.ch/de/d/Weggis/6353/Brennerei/Brennerei-Stalder-7IVRlnfIhM72dw8GWmkDPQ']*len(pp.master_plist)
else:
    with open(input_fn) as ff: toget=ff.read().splitlines()

if 1:
    for _ in ('prefix', 'company'):
        with suppress(FileExistsError): mkdir('tmp/'+_)
    from string import ascii_lowercase, ascii_uppercase, digits
    for _ in ascii_lowercase+ascii_uppercase+digits+'-_':
        with suppress(FileExistsError): mkdir('tmp/company/'+_)

if 0: # create toget.lst for all the companies downloaded into tmp/company
    from os import walk,path

    toget=[]
    for root, dirs, files in walk('tmp/company'):
        # Strip .html
        toget.extend([
            'https://yellow.local.ch/de/d/Weggis/6353/Brennerei/'+_[:-5]
            for _ in files])
    with open(input_fn, 'w') as fo:
        for i in toget: fo.write(i+'\n')

    exit()

class ypch(retriever):
    unique=set()

    def __init__(self):
        etree.set_default_parser(etree.HTMLParser())
        ua='Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0'
        super().__init__(pp, {'User-Agent': ua}, timeout=180)
        
    async def validate(self, r):
        self.content=(await r.content.read()).decode()
        #set_trace()
        
        # To help in debugging: saves the result of checking the proxy
        if do_filter and access('files', R_OK):
            # Strip initial 'http://'
            with open('files/{}.html'.format(self.p.p[7:]),'w') as f:
                f.write(self.content)

        if r.status == 410 and '410 Diese Seite existiert nicht mehr.' in self.content:
            return # Valid 410
        
        await super().validate(r)

        self.tree=etree.fromstring(self.content)

        if self.tree.xpath('//meta[@name="msapplication-starturl"]/@content')[0] != 'https://www.local.ch/de':
        # if 'Das offizielle Firmenverzeichnis und Branchenverzeichnis der Schweiz' not in self.content and \
        #    'Find phone numbers and addresses for businesses in your neighborhood and your region with local.ch' not in self.content:
            with open('noanchor.html','w') as f: f.write(self.content)
        
            raise ValidateException ('retry', 'noanchor')

    async def do(self, q):        
        # https://yellow.local.ch/de/q/*/000*.html
        
        o=urlsplit(q)        

        if '/d/' in q: # It's company page
            fn='tmp/company/{0[0]}/{0}.html'.format(o.path.split('/')[-1])
        else: # /q/ in the url, listing page (some listings are companies!)
            fn='tmp/prefix/{}'.format(q.split('/')[-1])
            
        #set_trace()

        if do_filter: # We need to reset the proxy, so it will be picked anew
            self.change_proxy('filter reset')

        self.tree=None
        try:
            with open(fn) as ff: text=ff.read()
            lg.debug('Found {}'.format(q))
        except:
            lg.debug('Getting {}'.format(q))
            
            r=await self.request('get', q)

            if use_cache:
                with open(fn,'w') as ff: ff.write(self.content)
            text=self.content

        self.tree=etree.fromstring(text)
        
        if do_filter: # We've got a page here, proxy's good
            return ('G', self.p.p)

        try:
            real_url=self.tree.xpath('//link[@rel="alternate"]/@href')[0]
        except IndexError:
            lg.warning('No link: {}'.format(q))
            return
            # Listing with one company redirects to the company page
            
        company='/d/' in real_url
        
        if company:
            if self.tree.xpath('//title/text()')[0] == '410, 410 Diese Seite existiert nicht mehr. - local.ch':
                lg.info('410: '+q)
                return # Page disappeared. Happens sometimes.

            if download_only: return

            intro=self.tree.xpath('//h1[@class="intro-h1"]/text()')
            if intro and intro[0] == 'Unerwünschtes Telefonmarketing!':
                return # Phone marketing special page
            
            try:
                addr=tuple(filter(None,
                                  (_.strip() for _ in
                                   self.tree.xpath('//p[@class="vcard-p"]//text()'))))
                
                # can be missing
                email=self.tree.xpath('//span[@class="email vcard-deemphasized vcard-row"]//a/text()')

                if len(addr) == 0:
                    addr=('', ' ') # Totally empty
                elif len(addr) == 1:
                    addr=('', addr[0]) # Only zip+city
                elif len(addr) == 3: # Village|Address|ZIP City
                    addr=(addr[1]+', '+addr[0], addr[2])                    
                    
                addr1=addr[1].partition(' ')
                # To check that we don't have LXML fat values
                # for k,v in j.items(): print(k, type(v))
                j={
                    'name': str(self.tree.xpath('normalize-space(//h1[@class="listing-name"]/text())')),
                    'url': str(real_url),
                    'site': '|'.join(self.tree.xpath('//span[@class="url vcard-deemphasized vcard-row"]//a/text()')), # Can be several
                    'categories':
                    '|'.join(self.tree.xpath('//h2[@class="categories"]/a/text()')),
                    'phone': '|'.join(self.tree.xpath('//span[@class="phone vcard-row"]//a/text()')),
                    'zip': addr1[0],
                    'city': addr1[2],
                    'address': addr[0],
                    'email': str(email[0]) if email else '',
                    'werbung': 'Yes' if self.tree.xpath('//span[@class="star vcard-star"]') else 'No'
                }
            except:
                lg.exception('Problem with {}'.format(q))
                return
            return j
        else: # Listing page
            num_res=self.tree.xpath('//span[@class="print"]/following-sibling::h1/text()')
            if num_res:
                num_res_int=int(num_res[0].split()[0])
            else:
                if 'Keine Treffer für' in text:
                    return
                elif 'Überprüfen Sie die Schreibweise' in text:
                    lg.error('BAD LISTING '+q)
                    return # May happen when initially there were 501 listing, and this page was stored, but with time one listing has disappeared, so page 51 gives an error
                else:
                    # https://yellow.local.ch/de/q/*/037*.html (But we don't care?)
                    lg.error('BAD LISTING '+q)
                    return
                
            # There are less than 1000 results, parse companies
            #if 'page' not in qs: #
            if '?page=' not in q: # First add all the pages thereof
                if num_res_int > 1000: # Recursively retry
                    lg.info(num_res)
                    await f.extend([
                        '{0[0]}{1}{0[1]}{0[2]}'.format(q.partition('*.'), _)
                        for _ in range(10)])
                    return

                num_pages=ceil(num_res_int/10)
                lg.debug('num_pages: {}'.format(num_pages))
                await f.extend([q+'?page={}'.format(_)
                           for _ in range(2, num_pages+1)])

            # Check the company wasn't seen before
            extending={str(_.xpath('.//h2/a/@href')[0])
                       for _ in self.tree.xpath('//div[@data-listing-id]')}
            len1=len(extending)
            extending-=self.unique
            diff=len1-len(extending)
            if diff: lg.debug('SAVED: {} ({})'.format(diff, len(self.unique)))
            self.unique|=extending
            await f.extend(extending)
        
loop = new_event_loop()
set_event_loop(loop)
#loop=get_event_loop()
if len(argv) > 1:
    prefix=argv[1]
    output_fn=prefix+'.csv'

fo=open(output_fn, 'w')
wr=DictWriter(fo, fieldnames=('name', 'url', 'site', 'categories', 'phone',
                              'zip', 'city', 'address', 'email', 'werbung'))
wr.writeheader()

def done_task(future):
    try:
        it=future.result()
    except CancelledError:
        lg.warning('main done_task: cancel retrieved')
        it=None
    except ProxyException as e:
        it=('B', e.args[0])

    lg.debug('done_task: {}'.format(it))
    if it:
        if do_filter:
            pp.master_plist[it[1]].addflag(it[0]) # Set good flag
            if it[0] == 'G': lg.info('Good: {}'.format(it[1]))
            print_length()
        else:
            wr.writerow(it)

f=farm(toget, ypch, num_tasks, loop, done_task=done_task)
try:
    loop.run_forever()
    args=[] # All done
except KeyboardInterrupt:
    lg.info('Caught interrupt')
    future = Future()
    results, args=f.stopall(loop)
    print(len(results), len(args))
    print(results[:10])

if do_filter:            
    old_master_plist.update(pp.master_plist) # Maybe flags have changed?
    pp.master_plist=old_master_plist
else: # Put queries that were not processed back into the file
    with open(input_fn, 'w') as fo:
        for i in args: fo.write(i+'\n')

pp.write() # Update the proxy list too
