#!/usr/bin/env python
#pooh@aikipooh.name

# newark.com scraper

from pdb import set_trace
from logging import basicConfig, DEBUG, INFO, WARNING, ERROR, getLogger
from csv import DictWriter
from asyncio import get_event_loop, Future, CancelledError
from math import ceil
from os import path #, remove, rename
from pprint import PrettyPrinter
from time import time
from urllib.parse import urlsplit #, parse_qs
# from contextlib import suppress

from lxml import etree

from retr.proxypool_common import proxypool        
from retr.retriever_async import retriever
from retr.farm_async import farm
from retr import ValidateException

basedir='tmp'
num_tasks=2 #00
output_fn='output.csv'

ll=WARNING
ll=INFO
#ll=DEBUG
basicConfig(format='{asctime}: {message}', datefmt="%H:%M:%S", style='{',
            level=ll)
lg=getLogger(__name__)

#pp=proxypool('gp.csv')
pp=proxypool([''])

########## No configuration past this line ###############

# Warming up

out_fieldnames=(
    'Source', 'Category', 'Competitor', 'Manufacturer Part Number',
    'Newark Part Number', 'Manufacturer', 'Description', 'Stock Value',
    'Availability', 'Factory Lead Time',
    'Quantity1', 'Price1', 'Quantity2', 'Price2', 'Quantity3', 'Price3',
    'Quantity4', 'Price4', 'Quantity5', 'Price5', 'Quantity6', 'Price6',
    'Quantity7', 'Price7', 'Quantity8', 'Price8', 'Quantity9', 'Price9',
    'Quantity10', 'Price10', 'Currency', 'URL', 'Status', 'Status Message',
    'CacheTime')
    
ppr=PrettyPrinter(indent=2)

class newark(retriever):
    baseurl='http://www.newark.com'
    
    def __init__(self):
        etree.set_default_parser(etree.HTMLParser())
        super().__init__(pp, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0',
#            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
#            'Accept-Encoding': 'gzip, deflate',
        }, timeout=180)
        self.cache=basedir

    async def validate(self, r):
        self.text=await r.text()

        o=urlsplit(str(r.url))
        
        self.tree=etree.fromstring(self.text)
        
        urls=o.path.split('/')
        #set_trace()
        # Check if we're /c/fasteners-mechanical/desiccators, total must be valid
        # Or we can be transferred to a product page
        if len(r.history): # One item?
            self.info('Single page: {}'.format(r.url))
            return
        
        if len(urls) >= 4:
            total=self.tree.xpath('//span[@id="titleProdCount"]')
            if not total:
                raise ValidateException('retry', 'no prod count')
                                                            
        await super().validate(r)

        return
    
        #set_trace()

        l=len(self.text)
        if o.path == '/rest/v1/session/startSession' and l == 48 or \
           o.path == '/shippingquote/SetZip.jsp' and l == 465:
            return
        elif o.path == self.freighturl:
            etree.fromstring(self.text, parser_xml)
            return
        elif o.path == '/search' and 'src="https://tirerack.resultspage.com/rac/sli-rac' in self.text:
            return
        elif o.path == '/shippingquote/getZip.jsp' and '/images/tires/images/sm_flag_canada.gif' in self.text:
            return
        elif 'https://www.tirerack.com/akam/10/pixel_' in self.text or \
             'SERVICE WITH A SMILE' in self.text:
            return        
        else:
            if o.path == '/cart/HoldingArea.jsp':
                set_trace()
            with open('noanchor.html', 'w') as f: f.write(self.text)
            raise ValidateException ('retry', 'no anchor for '+o.path)

    async def do(self, q):
        # qs=parse_qs(o.query, True)

        #set_trace()
        #mpc=q['MPC']
        #if isinstance(q, str): # Initial request
        qs=urlsplit(q)
        #else: # q is an item already
        
        # /browse-for-products
        dirn=qs.path.lstrip('/')
        fn=dirn+('?'+qs.query if qs.query else '')
        self.name=fn
        text, r=await self.cached(
            path.join(basedir, '{}.html'.format(fn)), qs.path, 'get', q
        )

        if not r: self.tree=etree.fromstring(text) # load the tree
        
        #for i in tree.xpath('//div[@class="catHeader"]/h2/a/@href'):
        subcats=self.tree.xpath('//nav[@class="filterCategoryLevelOne"]//a/@href')
        if subcats:
            await self.f.extend(subcats) # Drill down
            return
        
        cat=self.tree.xpath('//*[@id="breadcrumb"]//ul/li[position()=last()]/a/text()')[0]
        base_item={
            'Source': 'newark.com', 'Competitor': 'Newark',
            'Factory Lead Time': '', 'Currency': 'USD', 'Category': cat,
            'CacheTime': int(time())
        }

        def get_avail(el):
            return '\n'.join(
                filter(None,
                       map(lambda _: _.strip(),
                           el.xpath('.//div[@class="bodyarea"]//text()')))
            )
        
        def strip_qty(txt): return txt.strip().replace('+','')
        def strip_price(txt): return txt.strip().replace('$','').replace(',','')
        
        canlink=self.tree.xpath('//link[@rel="canonical"]/@href')[0]
        if not canlink.startswith(self.baseurl+'/c/'):
            # http://www.newark.com/berker/09-4185-25-02/socket-schuko-euro-white/dp/08P2180
            base_item.update({
                'URL': canlink,
                'Manufacturer Part Number': self.tree.xpath(
                    './/dd[@itemprop="mpn"]/text()')[0].strip(),
                'Newark Part Number': self.tree.xpath(
                    './/dd[@itemprop="http://schema.org/sku"]/text()')[0].strip(),
                'Manufacturer': self.tree.xpath(
                    '//span[@itemprop="http://schema.org/manufacturer"]/text()')[0],
                'Description': self.tree.xpath(
                    '//span[@itemprop="name"]/text()')[0],
                'Stock Value': self.tree.xpath(
                    '//div[@class="avalabilityContainer"]/p/text()')[0].strip(),
                'Availability': get_avail(
                    self.tree.xpath('//div[@class="avalabilityContainer"]')[0])
            })
            
            for num, price in enumerate(self.tree.xpath('//table[@class="tableProductDetailPrice pricing threeCol"]/tbody/tr')):
                #set_trace()
                base_item.update({ # Two spans here
                    'Quantity'+str(num+1): strip_qty(price[0].text),
                    'Price'+str(num+1): strip_price(price[1].xpath('.//text()')[1])
                })
            # 'URL', 'Status', 'Status Message', 'CacheTime'
            #ppr.pprint(base_item)
            return [base_item]
        
        total=int(self.tree.xpath('//span[@id="titleProdCount"]/text()')[0].replace(',',''))
            
        #http://www.newark.com/c/automation-process-control/automation-signaling/audio-signal-indicator-units?pageSize=100
        if '?pageSize=' not in q:
            if total > 25: # Get all of them
                # Get all the pages
                await self.f.extend(
                    q+'/prl/results/{}?pageSize=100'.format(_+1)
                    for _ in range(ceil(total/100))
                )
                return

        #return
        # Now get the items
        results=[]
        for item in self.tree.xpath('//table[@id="sProdList"]/tbody/tr'):
            it=base_item.copy()
            desc=item.xpath('td[@class="description"]/a/p/text()')[0]
            descp=desc.partition(' - ')
            avail_el=item.xpath('td[@class="availability" or @class="availability avnetAvailability"]')[0]
            stock=avail_el.xpath('.//span[@class="inStockBold"]/text()')
            if not stock: stock=avail_el.xpath('p/text()') # 0 there 

            mpn=item.xpath('td[@class="productImage mftrPart"]/a')[0]
            it.update({
                'URL': mpn.attrib['href'],
                'Manufacturer Part Number': mpn.text.strip(),
                'Newark Part Number': item.xpath(
                    './/p[@class="sku"]/a/text()')[0],
                'Manufacturer': descp[0],
                'Description': descp[2].strip(),
                'Stock Value': stock[0].strip(),
                'Availability': get_avail(avail_el)
            })

            #set_trace()
            for num, price in enumerate(item.xpath('(td[@class="listPrice"])[2]/p[@id="priceList"]//span[@class="priceBreak"]')):
                it.update({ # Two spans here
                    'Quantity'+str(num+1): strip_qty(price[0].text),
                    'Price'+str(num+1): strip_price(price[1].text)
                })
            # 'URL', 'Status', 'Status Message', 'CacheTime'
            results.append(it)
            #ppr.pprint(it)

        return results
        
    def done_task(self, future):
        #set_trace()
        try:
            it=future.result()
        except CancelledError:
            self.warning('main done_task: cancel retrieved')
            it=None
        except KeyboardInterrupt:
            self.warning('main done_task: keyboard interrupt retrieved')
            it=None
        except ProxyException as e:
            it=('B', e.args[0])

        if not it: return
        
        self.debug('done_task: {}'.format(len(it)))
        wr.writerows(it)
        #for res in it: self.info(ppr.pformat(res))

if 0:
    toget=['http://www.newark.com/c/electrical']
    #toget=['http://www.newark.com/c/electrical/switches-socket-outlets']
else:
    toget=[newark.baseurl+'/browse-for-products']
loop=get_event_loop()

f=farm(toget, newark, num_tasks)

with open(output_fn, 'w', newline='') as fo:
    wr=DictWriter(fo, fieldnames=out_fieldnames)
    wr.writeheader()
    try:
        loop.run_forever()
        args=[] # All done
    except KeyboardInterrupt:
        lg.info('Caught interrupt')
        future = Future()
        results, args=f.stopall(loop)
        print(len(results), len(args))
        #print(results[:10])

fo.close()
f.close()
            
pp.write()
