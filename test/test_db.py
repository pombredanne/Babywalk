import libbabywalk.db as sut
import nose.tools


CLIENT = None
DB = None


def setup_module():
    import pymongo
    global CLIENT
    global DB
    CLIENT = pymongo.MongoClient()
    DB = CLIENT.crawling


def teardown_module():
    global CLIENT
    global DB
    CLIENT = None
    DB = None


@nose.tools.nottest
def cleanup_db():
    global DB
    for name in DB.collection_names(include_system_collections=False):
        DB.drop_collection(name)


@nose.tools.nottest
def create_seed(url):
    import uuid
    return {'url': url, 'tag': str(uuid.uuid4())}


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_seed():
    url = 'http://index.hu/'
    sut.record_seeds(DB, [create_seed(url)])

    result = DB.seeds.find_one({'url': url})
    nose.tools.ok_(result)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_seed_multiple_():
    url = 'http://index.hu/'
    sut.record_seeds(DB, [create_seed(url), create_seed(url)])

    result = DB.seeds.find_one({'url': url})
    nose.tools.eq_(len(result['tags']), 2)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming():
    url = 'http://index.hu/'
    sut.record_seeds(DB, [create_seed(url)])
    sut.create_requests(DB, {'depth': 2, 'upload': 'location'})

    called = 0
    for request in sut.get_requests(DB):
        called += 1
        nose.tools.ok_(request)
        nose.tools.eq_(url, request['requests'][0]['fetch']['seed'])
        nose.tools.eq_(2  , request['requests'][0]['fetch']['depth'])
    nose.tools.eq_(1, called)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming_multiple():
    sut.record_seeds(DB, [create_seed('http://index.hu/'),
                          create_seed('http://index.hu/')])
    sut.create_requests(DB, {'depth': 2, 'upload': 'location'})
    for request in sut.get_requests(DB):
        nose.tools.ok_(request['host'] == 'index.hu')


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming_different_multiple():
    sut.record_seeds(DB, [create_seed('http://index.hu/'),
                          create_seed('http://444.hu/')])
    sut.create_requests(DB, {'depth': 2, 'upload': 'location'})
    for request in sut.get_requests(DB):
        nose.tools.ok_(request['host'] == 'index.hu' or
                       request['host'] == '444.hu')


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_completion_report():
    sut.record_seeds(DB, [create_seed('http://index.hu/')])
    sut.create_requests(DB, {'depth': 2, 'upload': 'location'})
    sut.get_requests(DB)
    sut.set_completed(DB,
        {
            'seed': 'http://index.hu/',
            'depth': 2
        }, {
            'location': 's3://bucket/path/id.warc.gz'
        }
    )

    for result in sut.query_completed(DB):
        nose.tools.eq_('http://index.hu/', result['seed'])
        nose.tools.eq_('s3://bucket/path/id.warc.gz', result['result'])
        nose.tools.eq_(1, len(result['tags']))
