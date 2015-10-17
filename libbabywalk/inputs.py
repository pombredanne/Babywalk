import csv
import logging
import itertools
import urllib.parse


def rows_from_tsv(filename):

    with open(filename, mode='r', encoding='utf-8') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        for row in reader:
            logging.debug('file: %s, row: %s', filename, row)
            yield row


def filter_ppid_and_seed(iterator):
    def normalize(url):
        atoms = urllib.parse.urlsplit(url)
        path = atoms.path if atoms.path else '/'
        return urllib.parse.urlunsplit((atoms.scheme, atoms.netloc, path,
                                        atoms.query, atoms.fragment))

    for item in iterator:
        yield {'seed': normalize(item['homepage']), 'ppid': item['place_id']}


def group_seeds_by_hostname(iterator):
    def hostname(item):
        # better to use tldextract or tld package for this
        atoms = urllib.parse.urlsplit(item['seed'])
        frags = atoms.hostname.split('.')
        return atoms.hostname if frags[0] != 'www' else '.'.join(frags[1:])

    for _, vs in itertools.groupby(sorted(iterator,
                                          key=hostname),
                                   key=hostname):
        yield list({item['seed'] for item in vs})
