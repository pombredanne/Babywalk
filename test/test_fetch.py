import libbabywalk.fetch as sut
import nose.tools
import threading
import http.server
import socketserver
import tempfile
import os.path


SERVER = None
SERVER_THREAD = None


def setup_module():
    global SERVER
    global SERVER_THREAD

    SERVER = socketserver.TCPServer(('localhost', 0), TreeGenerator)
    SERVER_THREAD = threading.Thread(target=lambda: SERVER.serve_forever())
    SERVER_THREAD.daemon = True
    SERVER_THREAD.start()


def teardown_module():
    global SERVER
    global SERVER_THREAD

    SERVER.shutdown()
    SERVER_THREAD.join()

    SERVER = None
    SERVER_THREAD = None


class TreeGenerator(http.server.BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        content = '<html><head><title>Title goes here.</title></head>'
        content += '<body><p>You accessed path: {}</p>'.format(self.path)
        for x in range(4):
            content += '<a href="{}{}/">{}</a>'.format(self.path, str(x), str(x))
        content += '</body></html>'
        self.wfile.write(content.encode('utf-8'))


@nose.tools.istest
def test_fetch():
    global SERVER
    with tempfile.TemporaryDirectory() as tmpdir:
        host, port = SERVER.server_address
        request = { 'url': 'http://{}:{}/'.format(host, str(port)), 'depth': 1 }
        warcfile = sut.fetch_warc(request, tmpdir)
        nose.tools.ok_(os.path.exists(warcfile))
