import csv
import logging
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
        yield {'url': normalize(item['homepage']), 'ppid': item['place_id']}
