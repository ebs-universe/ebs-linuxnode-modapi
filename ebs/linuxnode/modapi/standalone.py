

from twisted.internet.defer import inlineCallbacks
import treq


class StandaloneActual(object):
    @inlineCallbacks
    def http_get(self, url, **kwargs):
        d = yield treq.request('get', url, **kwargs)
        return d

    @inlineCallbacks
    def http_post(self, url, **kwargs):
        d = yield treq.post(url, **kwargs)
        return d


class StandaloneConfig(object):
    def __init__(self, *args, **kwargs):
        pass
