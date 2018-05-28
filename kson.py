"""
    kson rpc: json rpc with kubernetes like objects

"""

import json
import threading
import socket
import traceback
import sys
import urllib.request

from wsgiref.simple_server import make_server, WSGIRequestHandler

class Registry:
    KIND = 'kind'
    API_VERSION = 'apiVersion'
    METADATA = 'metadata'
    STATE = 'state'
    ATTRIBUTES = 'attributes'
    def __init__(self):
        self.classes = {}
        self.tags = {}

    def add(self, kind, apiVersion):
        def _decorate(fn):
            if kind not in self.classes:
                self.classes[kind] = {}
            if apiVersion in self.classes[kind]:
                raise Exception('Duplicate for {} {}'.format(kind, apiVersion))
            self.classes[kind][apiVersion] = fn
            self.tags[fn] = (kind, apiVersion)
            return fn
        return _decorate

    def parse(self, buf):
        document = json.loads(buf.decode('utf-8'))
        kind, apiVersion = document.pop(self.KIND, None), document.pop(self.API_VERSION, None)
        if kind is None or apiVersion is None:
            raise Exception('Invalid JSON document: missing {}, {}'.format(self.KIND, self.API_VERSION))

        cls = self.classes[kind][apiVersion]
        return cls(**document)

    def dump(self, obj):
        document = {}
        kind, apiVersion = self.tags[obj.__class__]
        document[self.KIND] = kind
        document[self.API_VERSION] = apiVersion
        for k,v in obj.__dict__.items():
            if not k.startswith('_') and not k in (self.KIND, self.API_VERSION,):
                document[k] = v
        return json.dumps(document).encode('utf-8')

registry = Registry()

class wire:
    @registry.add('Request', 'kson/v1')
    class Request:
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes

    @registry.add('Response', 'kson/v1')
    class Response:
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes

    @registry.add('Service', 'kson/v1')
    class Service:
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes

    @registry.add('Collection', 'kson/v1')
    class Collection:
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes
    
    @registry.add('Cursor', 'kson/v1')
    class Cursor:
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes
    @registry.add('Future', 'kson/v1')
    class Future: 
        def __init__(self, metadata, state, attributes):
            self.metadata = metadata
            self.state = state
            self.attributes = attributes

class server:
    class RequestHandler(WSGIRequestHandler):
        def log_request(self, code='-', size='-'):
            pass

    class Server(threading.Thread):
        def __init__(self, app, host="", port=0, request_handler=None):
            if request_handler is None:
                request_handler = server.RequestHandler
            threading.Thread.__init__(self)
            self.daemon=True
            self.running = True
            self.server = make_server(host, port, app,
                handler_class=request_handler)

        @property
        def url(self):
            return u'http://%s:%d/'%(self.server.server_name, self.server.server_port)

        def run(self):
            self.running = True
            while self.running:
                self.server.handle_request()

        def stop(self):
            self.running =False
            if self.server and self.is_alive():
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(self.server.socket.getsockname()[:2])
                    s.send(b'\r\n')
                    s.close()
                except IOError:
                    import traceback
                    traceback.print_exc()
            self.join(5)

class Endpoint:
    def __init__(self, prefix='/', version='v1'):
        self.prefix = prefix
        self.version = version

    def __call__(self, environ, start_response):
        start_response('200 Ok', [])
        return [dump(wire.Response({},{}, {}))]

parse, dump = registry.parse, registry.dump

def serve(endpoint):
    return server.Server(app=endpoint, port=1729)

def fetch(url):
    if isinstance(url, str):
        request =  urllib.request.Request(url)
    elif isinstance(url, wire.Request):
        pass

    with urllib.request.urlopen(request) as response:
        data = response.read()
        obj = parse(data)

    if isinstance(obj, wire.Response):
        pass
    elif isinstance(obj, wire.Collection):
        pass
    elif isinstance(obj, wire.Cursor):
        pass


    else:
        pass
    return obj
    pass

if __name__ == '__main__':

    class MyEndpoint(Endpoint):

        def rpc_one(self, args):
            return {'args': args}

    thread = serve(MyEndpoint())
    thread.start()

    url = thread.url

    service = fetch(url)

    # response = fetch(service.rpc_one())

    # print(response)

    

