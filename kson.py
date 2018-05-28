"""
    kson rpc: json rpc with kubernetes like objects


"""

import json

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

    def parse(self, text):
        document = json.loads(text)
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
        return json.dumps(document)

registry = Registry()
parse, dump = registry.parse, registry.dump


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

