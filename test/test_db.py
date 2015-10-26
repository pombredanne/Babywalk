import libbabywalk.db as sut
import nose.tools


CLIENT = None
DB = None
COLLECTION = None


def setup_module():
    import pymongo
    global CLIENT
    global DB
    global COLLECTION
    CLIENT = pymongo.MongoClient()
    DB = CLIENT.crawling
    COLLECTION = DB.places


def teardown_module():
    global CLIENT
    global DB
    global COLLECTION
    CLIENT = None
    DB = None
    COLLECTION = None


@nose.tools.nottest
def cleanup_db():
    COLLECTION.delete_many({})


@nose.tools.nottest
def create_request(url):
    return  {
        'fetch': { 'url': url, 'depth': 2 },
        'upload': { 'bucket': '<bucket>', 'object': '<filename>' }
    }

@nose.tools.nottest
def create_seed(url):
    import uuid
    return {'url': url, 'ppid': str(uuid.uuid4())}


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_seed():
    url = 'http://index.hu/'
    sut.record_request(DB, create_seed(url), create_request(url))

    result = COLLECTION.find_one({'seed': url})
    nose.tools.ok_(result)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_seed_multiple_():
    url = 'http://index.hu/'
    sut.record_request(DB, create_seed(url), create_request(url))
    sut.record_request(DB, create_seed(url), create_request(url))

    result = COLLECTION.find_one({'seed': url})
    nose.tools.eq_(len(result['ppids']), 2)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming():
    url = 'http://index.hu/'
    request = create_request(url)
    sut.record_request(DB, create_seed(url), request)

    result = sut.get_request(DB)
    nose.tools.assert_equals(result, [request])
    result = sut.get_request(DB)
    nose.tools.assert_equals(result, None)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming_multiple():
    url = 'http://index.hu/'
    request = create_request(url)
    sut.record_request(DB, create_seed(url), request)
    sut.record_request(DB, create_seed(url), request)

    result = sut.get_request(DB)
    nose.tools.assert_equals(result, [request])
    result = sut.get_request(DB)
    nose.tools.assert_equals(result, None)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming_different_multiple():
    url1 = 'http://index.hu/'
    request1 = create_request(url1)
    sut.record_request(DB, create_seed(url1), request1)

    url2 = 'http://slashdot.org/'
    request2 = create_request(url2)
    sut.record_request(DB, create_seed(url2), request2)

    result = sut.get_request(DB)
    nose.tools.ok_(result == [request1] or result == [request2])
    result = sut.get_request(DB)
    nose.tools.ok_(result == [request1] or result == [request2])
    result = sut.get_request(DB)
    nose.tools.assert_equals(result, None)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_request_consuming_same_host_multiple():
    url1 = 'http://www.slashdot.org/'
    request1 = create_request(url1)
    sut.record_request(DB, create_seed(url1), request1)

    url2 = 'http://slashdot.org/'
    request2 = create_request(url2)
    sut.record_request(DB, create_seed(url2), request2)

    result = sut.get_request(DB)
    nose.tools.assert_equals(result, [request1, request2])
    result = sut.get_request(DB)
    nose.tools.assert_equals(result, None)


@nose.tools.with_setup(cleanup_db, cleanup_db)
def test_completion_report():
    url = 'http://slashdot.org/'
    request = create_request(url)
    sut.record_request(DB, create_seed(url), request)

    sut.get_request(DB)

    status = { 'wget_status_code': 2 }
    sut.set_completed(DB, request, status)

    result = COLLECTION.find_one({'seed': url})
    nose.tools.eq_(result['requests'], [])
    nose.tools.eq_(result['results'], [{'result': status, 'request': request}])
