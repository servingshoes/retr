lg=getLogger(__name__)

def normalise_proxy(i):
    'Removes leading zeros'
    p_raw=i.split(':')
    try:
        a=IPv4Address(p_raw[0])
    except AddressValueError:
        lg.critical('Problem with {}'.format(i))
        exit()
    return ':'.join((a.exploded,p_raw[1]))

def load_file(fn):
    s=set()
    with suppress(FileNotFoundError), open(fn) as f:
        for i in f:
            p=normalise_proxy(i.strip())
            s.add(p)
        lg.warning('Loaded {} {}'.format(fn,len(s)))
    return s

def save_file(fn, s):
    lg.warning('Saving {} {}'.format(fn,len(s)))
    with open(fn,'w') as f:
        for i in s: f.write(i+'\n')
