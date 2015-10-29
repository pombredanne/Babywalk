import logging
import pymongo
import urllib.parse

""" The schema of the schemaless db:

    collection seeds: {
        _id:    <uuid3 of the seed url>,
        seed:   <the seed url>,
        ids:    [ <some id> ],
        tags:   [ <some tag> ]
    }
    collection requests: {
        _id:    <request id>,
        name:   <request name>,
        target: <bucket name>,
        groups: [
            {
                tag: <tag name>,
                depth: <number>
            }
        ]
    }
    collection results: {
        _id:    <result id>,
        request: {
            seed:   <url>,
            depth:  <number>,
            id:     <request id>
        }
        result: {
            location: {
                bucket: <bucket name>,
                object: <object name>
            },
            statistic: [
                {
                    url: <page url>,
                    status: <http status codes>,
                    mime: <mime type of response>
                }
            ]
        }
    }

    collection request_queue: {
        _id:    <uuid3 of the seed url>,
        status: <boolean sent to worker or not>
        host:   <hostname from the url>,
        requests: [ {
            fetch: {
                seed:   <url>,
                depth:  <number>,
                id:     <request id>
            },
            upload: {
                bucket: <target bucket name>
                object: <uuid3.requestid.warc.gz>
            }
        } ]
    }
"""


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

    filter_ = {'host': pivot['host'], 'state': State.Requested}

    result = db.places.find(filter_, projection={'requests': True})
    requests = list(flatten(entry['requests'] for entry in result))

    db.places.update_many(filter_, {'$set': {'state': State.Running}})

    return requests


def set_completed(db, request, result):

    db.places.update_one(
        {
            'seed': request['fetch']['url']
        }, {
            '$set': {
                'state': State.Completed
            },
            '$pull': {
                'requests': request
            },
            '$addToSet': {
                'results': {
                    'request': request['fetch'],
                    'result': result
                }
            }
        })


def query_completed(db):

    for entry in db.places.find(
        {'state': State.Completed},
        projection=
        {'_id': False,
         'seed': True,
         'results': True,
         'ppids': True}):
        for result in entry['results']:
            yield {
                'seed': entry['seed'],
                'ppids': entry['ppids'],
                'results': result['result']
            }
