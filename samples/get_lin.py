#!/usr/bin/python3

'''Real world example of how to use the engine, scrapes linkedin (which is tough to scrape, so it rotates the proxies all the time:)

Get some proxy file (from gatherproxy.com for example) and save it as proxies.lst
'''

# Author: aikipooh@gmail.com

from logging import basicConfig, getLogger, DEBUG, INFO, WARNING, ERROR
from os import _exit, mkdir
from string import ascii_lowercase, digits
from contextlib import suppress
from json import loads, dump
from time import time, sleep
from re import search
from base64 import b64encode
from random import randint
from urllib.parse import urlparse

from requests.packages.urllib3 import disable_warnings
from lxml import etree

from retr.farm import *
from retr.retriever import retriever, ValidateException
from retr.proxypool import proxypool

lg=getLogger(__name__)

getLogger('requests.packages.urllib3').setLevel(ERROR)
disable_warnings()

ll=WARNING
ll=INFO
#ll=DEBUG

basicConfig( format='{asctime} {threadName:11s}: {message}', datefmt="%H:%M:%S",
             style='{', level=ll)

num_threads=100
get_lists=1 # 1 to get lists only, 0 to get companies' data
use_cache=1 # 1 to cache files, 0 to disable it (save MUCH space)
basedir='tmp/'
get_people=1 # 1 to get people instead of companies
infile='toget.lst'

pp=proxypool('proxies.lst')
timeout=10

########## No configuration past this line ###############

if use_cache:
    with suppress(FileExistsError): mkdir(basedir+'lists')
    with suppress(FileExistsError): mkdir(basedir+'pubdir')
    for _ in ascii_lowercase+digits+'_-%&':
        with suppress(FileExistsError): mkdir(basedir+_)
        with suppress(FileExistsError): mkdir(basedir+'lists/'+_)
        with suppress(FileExistsError): mkdir(basedir+'pubdir/'+_)
    with suppress(FileExistsError): mkdir(basedir+'output')

class linkedin(retriever):    
    # For luminati
    # pp=proxypool(['zproxy.luminati.io:22225'])
    # def get_auth(self):
    #     uname=username+'-session-{}'.format(self.sessid)
    #     lg.info('New uname: {}'.format(uname))
    #     self.proxy_headers={'Proxy-Authorization': 'Basic '+b64encode(
    #         (uname+':'+password).encode()).decode()}
        
    def __init__(self):
        etree.set_default_parser(etree.HTMLParser())
        #self.sessid=randint(0, 1000000)
        super().__init__(pp, {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0'
        },
                         proxy_headers=proxy_headers, timeout=timeout)
        self.baseurl=None
        #self.get_auth()
        
    @staticmethod
    def makefn(q, prefix, postfix):
        return '{0}/{1[0]}/{1}.{2}'.format(prefix, q.replace('/','_'), postfix)[:200]

    def validate(self, url, r):
        o=urlparse(url)
        if url == self.baseurl+'/directory/' and \
           'For touch.www.linkedin.com or tablet.www.linkedin.com' in r.text:
            return
        elif ( o.path == '/uas/login' or o.path == '/start/join' ) and \
             'https://static.licdn.com/scds/concat/common' in r.text:
            return

        if 1:
            super().validate(url, r)            
            if 'https://static.licdn.com/scds/concat/common' not in r.text:
                raise ValidateException ('retry', 'noanchor')
            if not r.text[-100:].rstrip().endswith('</html>'):
                raise ValidateException ('retry', 'truncated')
        # except:
        #     self.sessid+=1
        #     self.get_auth() # Update proxy_headers
        #     raise
            
    def setup_session(self):
        while True:
            try:                
                r=self.request('get', self.baseurl+'/directory/', regular=False)
                if get_people:
                    r=self.request('get', self.baseurl+'/start/join',
                               params={
                                   'trk': 'login_reg_redirect',
                                   'source': 'gf',
                                   'session_redirect': self.dirprefix},
                               regular=False)
                else:
                    r=self.request('get', self.baseurl+'/uas/login',
                                   params={
                                       'trk': 'sentinel_org_block',
                                       'trkInfo': 'sentinel_org_block',
                                       'session_redirect': self.dirprefix},
                                   regular=False)
            except ValidateException:
                continue
            except KeyboardInterrupt:
                return
            break

    def do(self, q):        
        o=urlparse(q)

        if o.path.startswith('/in/'): # Person
            if get_lists:
                yield q
                return # Stop here, we don't need people data, only URL
            fn=self.makefn(o.path.replace('/in/',''), basedir, 'html')
        elif o.path.startswith('/pub/dir/'): # Several people in one directory
            fn=self.makefn(o.path.replace('/pub/dir/',''), basedir+'pubdir',
                           'html')
        elif o.path.startswith('/company/'): # Company
            if get_lists:
                yield q
                return # Stop here, we don't need companies data, only URL
            fn=self.makefn(o.path.replace('/company/',''), basedir, 'html')
        else: # <...>/directory
            fn=self.makefn(o.path.replace('/directory/',''), basedir+'lists',
                           'html')

        # For the setup_session
        self.baseurl=o.scheme+'://'+o.hostname
        self.dirprefix=self.baseurl+'/directory/'
        #print(fn)
        
        try:
            with open(fn) as ff: text=ff.read()
            lg.debug('Found {}'.format(q))
        except:
            lg.debug('Getting {}'.format(q))

            sleep(60)
            try:
                r=self.request('get', q)
            except KeyboardInterrupt:
                f.extend([q]) # Return the item back to be printed in the end
                return

            if use_cache:
                with open(fn,'w') as ff: ff.write(r.text)
            text=r.text

        if o.path.startswith('/company/'):
            if "We're sorry, but the company you are looking for does not exist" in text:
                lg.warning("Doesnt't exist: {}".format(q))
                return
            if "We're sorry, but the company you are looking for is not active" in text:
                lg.warning("Not active: {}".format(q))
                return
            if "We're sorry, but there was an unexpected error.  Please try your request again." in text: # Doesn't help however many times I try
                lg.warning('Unexpected: {}'.format(q))
                return

            if '<div class="edu-wrapper" data-edu-tracking' in text:
                lg.warning("University: {}".format(q))
                return
                
            m=search(r'<code id="stream-footer-embed-id-content"><!--(.*?)--></code>', text)
            if m:
                txt=m.groups()[0]
                j=loads(txt)
            else:
                tree=etree.fromstring(text)
                try:
                    yf=tree.xpath('//li[@class="founded"]/p/text()')
                    ind=tree.xpath('//p[@class="industry"]/text()')
                    ct=tree.xpath('normalize-space(//li[@class="type"]/p/text())')
                    ws=tree.xpath('//li[@class="website"]//a/text()')
                    sz=tree.xpath('//p[@class="company-size"]/text()')
                    spec=tree.xpath('//div[@class="specialties"]/p/text()')
                    j={
                        'companyName': tree.xpath('//span[@itemprop="name"]/text()')[0],
                        'size': sz[0] if sz else '',
                        'industry': ind[0] if ind else '',
                        'companyType': ct[0] if ct else '',
                        'website': ws[0] if ws else '',
                        'specialties': map(str.strip,
                                           (spec[0] if spec else '').split('\n')),
                        'yearFounded': yf[0] if yf else '',
                        'homeUrl': tree.xpath('//link[@rel="canonical"]/@href'),
                    }
                except:
                    lg.exception('Problem with {}'.format(q))
                    return
            yield j
        elif o.path.startswith('/pub/dir/'):
            tree=etree.fromstring(text)
            total_str=tree.xpath('//a[@class="join-now-banner hide-desktop"]/text()')
            # https://it.linkedin.com/pub/dir/evelina/marvulli
            # Only one person
            if total_str:
                # Iscriviti a LinkedIn per vedere tutti i profili (42).
                total_num=int(total_str[0].split('(')[-1][:-2])
                all_profiles=tree.xpath('//div[@class="profile-card"]//h3/a/@href')
                lg.warning('TOTAL: str: {}, total_num: {}, len: {}'.format(
                    total_str[0], total_num, len(all_profiles)))
                 if get_lists:                
                    if len(all_profiles) != total_num: # Emit pub/dir
                        yield q
                    else:
                        yield from all_profiles
                else:
                    lg.critical('Getting information for pub/dir is not supported')
            else: # It has canonical link though
                yield tree.xpath('//link[@rel="canonical"]/@href')[0]

        else:
            tree=etree.fromstring(text)
            if o.path in ('companies/', 'people/'):
                # Need only toplevel letters from this url
                f.extend(tree.xpath('//ol[@class="bucket-list"]/li/a/@href'))
            else:
                # link_url=tree.xpath('//link[@rel="canonical"]/@href')[0]
                # print(link_url, qi)
                # o=urlparse(link_url)
                arr=tree.xpath('//ul[@class="column dual-column"]/li/a/@href')
                # Need to prepend the base url, they're relative
                f.extend(o.scheme+'://'+o.hostname+_ if _.startswith('/') else _
                         for _ in arr)
            return # Recurse

with open(infile) as ff: toget=ff.read().splitlines()

f=farm(num_threads, linkedin, toget, extendable=True)
cnt=0
if get_lists:
    fo=open('toget_urls.lst','w')

def write_json(res):
    with open(basedir+'output/{}.json'.format(int(time())), 'w') as fo:
        dump(res, fo)

with open(infile) as fi:
    res=[] # Output json
    for it in f.run():
        if get_lists: # This is url
            fo.write(it+'\n')
            continue
        # Make output item
        out={}
        for _ in ('website', 'size', 'industry', 'yearFounded', 'companyType',
                  'specialties',
                  'companyName', 'homeUrl'):
            out[_]=it.get(_, '') # Fill in missing fields

        out['specialties']=it.get('specialties', [])
        res.append(out)
        #print(it)
        if len(res) == 1000:
            write_json(res)
            res[:]=[]

write_json(res) # Last batch

f.close()
if get_lists: fo.close()
    
# with open(infile, 'w') as fo:
#     for i in f.arr: fo.write(i+'\n')

#pp.write()
