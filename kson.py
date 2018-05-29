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

class ServerThread(threading.Thread):
    class RequestHandler(WSGIRequestHandler):
        def log_request(self, code='-', size='-'):
            pass

    def __init__(self, app, host="", port=0, request_handler=None):
        if request_handler is None:
            request_handler = self.RequestHandler
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

registry = Registry()

class wire:
    @registry.add('Request', 'kson/v1')
    class Request:
        """ A request to a service """
        def __init__(self, metadata, content):
            self.metadata = metadata
            self.content = content

    @registry.add('Response', 'kson/v1')
    class Response:
        """ A response from a service """
        def __init__(self, metadata, content):
            self.metadata = metadata
            self.content = content

    @registry.add('Service', 'kson/v1')
    class Service:
        """ Remote service with methods

            metadata:
                url,
                links = ['name']
                actions ={'name':[args]}
            attributes:
                user-def
        """
        def __init__(self, metadata,  attributes):
            self.metadata = metadata
            self.attributes = attributes

    @registry.add('Collection', 'kson/v1')
    class Collection:
        """
            metadata:
                url
                fields = ['create', 'args']
                keys = ['key','key2'] # which fields can index onn
                key = "name of key" # primary key
                
        """
        def __init__(self, metadata, attributes):
            self.metadata = metadata
            self.attributes = attributes
    
    @registry.add('Cursor', 'kson/v1')
    class Cursor:
        """
            metadata:
                url
                collection
                selector
                next
        """
        def __init__(self, metadata, attributes):
            self.metadata = metadata
            self.attributes = attributes

    @registry.add('Future', 'kson/v1')
    class Future: 
        """
            metadata:
                url
                wait_seconds
        """
        def __init__(self, metadata):
            self.metadata = metadata

parse, dump = registry.parse, registry.dump

def rpc(safe=False):
    def _decorator(fn):
        fn.rpc = True
        return fn
    return _decorator

class RetryLater(Exception):
    def __init__(self, wait_seconds=30):
        self.wait_seconds = wait_seconds
        Exception.__init__(self)

class Endpoint:
    def __init__(self):
        pass

    class Handler:
        def __init__(self, url, obj):
            self.url = url
            self.obj = obj

        def handle(self, method, path, query, data):
            return wire.Request(metadata={}, content=data)

        def describe(self):
            metadata = dict(
                url = "...",
                links = {},
                actions = {},
                embeds = {},
            )
            return wire.Service(metadata={}, attributes={})
            

class Model:        
    pass
    "/id/x /list?... /new /delete"

def make_app(endpoint):
    # inspect object
    # gather list of methods, arguments
    # and nested models, endpoints
    # make up Service/Collection objects
    # annotate them with __url__

    handler_class = endpoint.Handler

    handler  = handler_class("/", endpoint)

    def app(environ, start_response):
        method = environ['REQUEST_METHOD'].upper()

        if method == "POST":
            body = environ.get('wsgi.input', None)
            content_length = int(environ.get('CONTENT_LENGTH', 0))
            content_type = environ.get('CONTENT_TYPE', '')
            if body and content_length > 0:
                data = parse(body.read(content_length))
        else:
            data = None

        path = environ.get('PATH_INFO', '')
        query = environ.get('QUERY_STRING', '')

        out = None
        try:
            out = handler.handle(method, path, query, data)
        except Exception:
            raise

        if out is not None:
            start_response('200 Ok', [])
            return [dump(out)]
        else:
            start_response('204 None', [])
            return []

    return app

class RemoteService:
    def __init__(self, url, response, fetch):
        self.url = url
        self.response = response
        self.fetch = fetch

class RemoteModel:
    def __init__(self, url, response, fetch):
        self.url = url
        self.response = response
        self.fetch = fetch

def fetch(url):
    if isinstance(url, str):
        request =  urllib.request.Request(url)
    elif isinstance(url, wire.Request):
        pass

    with urllib.request.urlopen(request) as response:
        data = response.read()
        url = response.geturl()
        print(url)
        obj = parse(data)

    if isinstance(obj, wire.Response):
        # check ok, extract content
        obj = obj.content
        pass
    elif isinstance(obj, wire.Service):
        obj = RemoteService(url, obj, fetch)
        pass
        # build up fake object with similar methods

    elif isinstance(obj, wire.Collection):
        pass
    elif isinstance(obj, wire.Cursor):
        pass
    elif isinstance(obj, wire.Future):
        pass


    else:
        pass
    return obj
    pass

if __name__ == '__main__':

    class MyEndpoint(Endpoint):

        @rpc()
        def one(self, args):
            return {'args': args}

    endpoint = MyEndpoint()

    thread = ServerThread(app=make_app(endpoint), port=1729)
    thread.start()

    url = thread.url

    service = fetch(url)

    print(service.content)

    # response = service.rpc_one()

    # print(response)

    

