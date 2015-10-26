import csv
import urllib.parse


def _rows_from_tsv(filename):

    with open(filename, mode='r', encoding='utf-8') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        for row in reader:
            yield row


def _filter_ppid_and_seed(iterator):
    def normalize(url):
        atoms = urllib.parse.urlsplit(url)
        if not atoms.scheme:
            return normalize('http://' + url)
        path = atoms.path if atoms.path else '/'
        return urllib.parse.urlunsplit((atoms.scheme, atoms.netloc, path,
                                        atoms.query, atoms.fragment))

    return ({
        'url': normalize(item['homepage']),
        'ppid': item['place_id']
    } for item in iterator)


def read_seeds_from_file(filename):

    return _filter_ppid_and_seed(_rows_from_tsv(filename))
