import logging
import pymongo
import urllib.parse


class State(object):

    Requested, Running, Completed = range(3)


def record_request(db, seed, request):
    """ seed: {'url': '<url>', 'ppid': <identifier>}
        request: {
            'fetch': { 'url': '<url>', 'depth': <number> },
            'upload': { 'bucket': '<bucket>', 'object': '<filename>' }
        }
    """

    def hostname(url):

        # better to use tldextract or tld package for this
        atoms = urllib.parse.urlsplit(url)
        frags = atoms.hostname.split('.')
        return atoms.hostname if frags[0] != 'www' else '.'.join(frags[1:])

    url = seed['url']
    result = db.places.update_one(
        {
            'seed': url
        }, {
            '$setOnInsert': {
                'seed': url,
                'host': hostname(url)
            },
            '$addToSet': {
                'ppids': seed['ppid'],
                'requests': request
            },
            '$max': {
                'state': State.Requested
            }
        }, True)
    logging.debug('mongo update result: %s', result)


def get_request(db):

    def flatten(iterator):
        return (item for sublist in iterator for item in sublist)

    pivot = db.places.find_one({'state': State.Requested})
    if not pivot:
        return None

    filter_ = { 'host': pivot['host'], 'state': State.Requested }

    result = db.places.find(filter_, projection={'requests': True})
    requests = list(flatten(entry['requests'] for entry in result))

    db.places.update_many(filter_, { '$set': { 'state': State.Running } })

    return requests


def set_completed(db, request, result):

    entry = db.places.find_one({'seed': request['fetch']['url']})
    if not entry:
        logging.error('fetch completed for seed which was not found')
    else:
        state = State.Running if len(entry['requests']) > 1 else State.Completed
        db.places.update_one(
            {
                'seed': request['fetch']['url']
            }, {
                '$set': {
                    'state': state
                },
                '$pull': {
                    'requests': request
                },
                '$addToSet': {
                    'results': {
                        'request': request,
                        'result': result
                    }
                }
            })


def query_completed(db, state):

    return db.places.find({'state': state}, projection={'_id': False, 'results': True})
