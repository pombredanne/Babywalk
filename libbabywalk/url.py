import multiprocessing
import logging
import urllib.parse
import uuid
import re

def validate(iterator):

    pool = multiprocessing.Pool()
    for seed in pool.imap_unordered(_normalize_url, iterator, 100):
        if 'error' in seed:
            logging.error(seed)
        else:
            yield seed
    pool.close()
    pool.join()


def _normalize_url(seed):

    url = normalize(seed['url'])
    if not url:
        seed.update({'error': 'url is broken'})
    else:
        uid = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
        seed.update({'url': url, 'uid': uid})
    return seed


def normalize(url):

    atoms = urllib.parse.urlsplit(url)

    if not atoms.scheme:
        return normalize('http://' + url)
    elif atoms.scheme.lower() not in {'http', 'https'}:
        return None

    netsplit = atoms.netloc.split(':') if atoms.netloc else ['']
    if len(netsplit) > 2:
        return None

    hostname = netsplit[0]
    if hostname:
        if len(hostname) < 1 or len(hostname) > 253:
            return None
        allowed = re.compile(r'^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$',
                             re.IGNORECASE)
        if not all(allowed.match(x) for x in hostname.split('.')):
            return None

    port = netsplit[1] if len(netsplit) == 2 else None
    if port and not re.match(r'^\d+$', port):
        return None

    path = atoms.path if atoms.path else '/'

    return urllib.parse.urlunsplit((atoms.scheme.lower(),
                                    atoms.netloc.lower(), path,
                                    atoms.query, atoms.fragment))
