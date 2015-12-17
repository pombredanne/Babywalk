import logging
import pymongo
import uuid
import datetime
import urllib.parse

""" The schema of the schemaless db:

    collection seeds: {
        _id:    <uuid3 of the seed url>,
        url:    <the seed url>,
        tags:   [ <some tag> ]
    }

    collection requests: {
        _id:    <some id>,
        status: <boolean sent to worker or not>
        when:   <date and time>
        host:   <hostname from the url>,
        requests: [ {
            fetch: {
                seed:   <url>,
                depth:  <number>
            },
            upload: <s3 location uri>
        } ]
    }

    collection results: {
        _id:    <result id>,
        when:   <date and time>
        request: {
            seed:   <url>,
            depth:  <number>
        }
        result: {
            location: <s3 location uri>,
            error:    <error message>,
            statistic: [
                {
                    url:    <page url>,
                    offset: <offset within warc file>,

                    status: <http status codes>,
                    mime:   <mime type of response>,
                    links:  [ { 'from': <page url>, 'text': <link text> } ]
                }
            ]
        }
    }
"""

def _require(checked, *names):

    def _require_single(entry, name):
        for element in name.split('.'):
            if element in entry:
                entry = entry[name]
            else:
                return False
        return True

    if not any(_require_single(checked, name) for name in names):
        raise AssertionError('%s expected, but not found' % list(names))


def record_seeds(db, seeds):

    for seed in seeds:
        _require(seed, 'url')
        _require(seed, 'uid')
        _require(seed, 'tag')

        result = db.seeds.update_one(
            {
                '_id': seed['uid']
            }, {
                '$setOnInsert': {
                    '_id': seed['uid'],
                    'url': seed['url']
                },
                '$addToSet': {
                    'tags': seed['tag']
                }
            }, True)


def create_requests(db, request):
    _require(request, 'depth')
    _require(request, 'upload')

    def hostname(url):
        # better to use tldextract or tld package for this
        atoms = urllib.parse.urlsplit(url)
        frags = atoms.hostname.split('.')
        return atoms.hostname if frags[0] != 'www' else '.'.join(frags[1:])

    for seed in db.seeds.find():
        location = '{}/{}.warc.gz'.format(request['upload'], seed['_id'])
        result = db.requests.update_one(
            { 'host': hostname(seed['url']) },
            {
                '$setOnInsert': {
                    'status': False,
                    'when': 'not yet',
                    'host': hostname(seed['url'])
                },
                '$addToSet': {
                    'requests': {
                        'upload': location,
                        'fetch': {
                            'seed': seed['url'],
                            'depth': request['depth']
                        }
                    }
                }
            }, True)


def get_request(db):

    while True:
        request = db.requests.find_one_and_update(
            { 'status': False },
            { '$set': { 'status': True, 'when': str(datetime.datetime.now()) } },
            return_document=pymongo.ReturnDocument.AFTER
        )
        if not request:
            break
        yield request


def set_completed(db, request, result):
    _require(request, 'seed')
    _require(request, 'depth')
    _require(result, 'location', 'error')

    url = request['seed']
    uid = str(uuid.uuid3(uuid.NAMESPACE_URL, url))

    db.results.insert_one(
        {
            '_id': uid,
            'when': str(datetime.datetime.now()),
            'request': request,
            'result': result
        }
    )


def query_completed(db):

    for result in db.results.find():
        if result['result'].get('location'):
            seed = db.seeds.find_one({'_id': result['_id']})
            if seed:
                yield {
                    'seed': seed['url'],
                    'result': result['result']['location'],
                    'tags': seed['tags']
                }
            else:
                logging.error('no seed for result %s', result)
        else:
            logging.warning('failed fetch %s', result['result']['error'])
