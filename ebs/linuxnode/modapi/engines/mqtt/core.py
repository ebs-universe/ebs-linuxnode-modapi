

from twisted.internet.defer import inlineCallbacks
from ebs.linuxnode.mq.mqtt import PahoMQTTService
from ..exceptions import WaitingForConnectionError
from ..base import ModularApiEngineBase


class MQTTCoreEngine(ModularApiEngineBase):
    _api_probe = '_probe'
    _api_announce = None
    _api_tasks = []
    # Reconnection is actually handled by the Pika Service. We make a token
    # attempt every so often here just to avoid rewriting a bunch of other
    # existing logic. These attempts would typically not have any effect.
    _api_reconnect_frequency = 300
    _pqueue_enabled = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._service = None
        self.mqtt = None
        self._is_connected = False

    @inlineCallbacks
    def mq_token(self):
        if not self.master:
            raise NotImplementedError("We only support Authenticated AMPQ Engines, authenticated by "
                                      "a token, and slave to a HTTP Api Engine which deals with all "
                                      "the token management. Consider alternate use cases for "
                                      "implementation.")
        token = yield self.master.api_token
        return token

    @inlineCallbacks
    def connection_parameters(self):
        raise NotImplementedError

    def create_bindings(self):
        self.mqtt = self._service

    @inlineCallbacks
    def start_service(self):
        parameters = yield self.connection_parameters()
        self._service = PahoMQTTService(parameters=parameters, postfix=self._prefix,
                                        connectionStateHandler=self.handleConnectionState)
        self._actual.reactor.callWhenRunning(self._service.startService)

    def handleConnectionState(self, state):
        if self._is_connected != state:
            self._is_connected = state
            if state:
                self.api_engine_activate()
            else:
                self.api_engine_reconnect()

    @inlineCallbacks
    def _probe(self, *_):
        # The reconnection cycle is controlled by the service itself once it is started.
        # We just need to be able to track it. So if the service is connected, return normally.
        # If it isn't, raise something.
        if not self._service:
            yield self.start_service()

        if not self._is_connected:
            raise WaitingForConnectionError()

        # Warning any durable binding might cause issues here.
        self.create_bindings()
