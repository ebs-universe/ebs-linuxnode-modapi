

import os
from twisted import logger
from twisted.internet.defer import succeed
from twisted.internet.task import LoopingCall

from .exceptions import ConnectionRequirementsNotReady
from .exceptions import ServerReportsNotReady
from .exceptions import WaitingForConnectionError
from ..primitives import ApiPersistentActionQueue


class ModularApiEngineBase(object):
    _prefix = ""
    _api_probe = None
    _api_announce = None
    _api_tasks = []
    _api_reconnect_frequency = 30
    _pqueue_enabled = True

    def __init__(self, actual, config=None, master=None):
        self._log = None
        self._config = config
        self._actual = actual
        self._master = master
        self._api_reconnect_task = None
        self._api_engine_active = False
        self._api_endpoint_connected = None
        if self._pqueue_enabled:
            self._api_queue = ApiPersistentActionQueue(self)
        else:
            self._api_queue = None

    @property
    def name(self):
        return self._prefix

    @property
    def master(self):
        return self._master

    @property
    def node_id(self):
        return self._actual.id

    """ Proxy to Core Engine """

    @property
    def cache_dir(self):
        return os.path.join(self._actual.cache_dir, self._prefix)

    @property
    def log(self):
        if not self._log:
            self._log = logger.Logger(namespace="modapi.{0}".format(self._prefix), source=self)
        return self._log

    @property
    def config(self):
        if self._config:
            return self._config
        else:
            return self._actual.config

    """ API Connection Status Primitives """
    @property
    def api_endpoint_connected(self):
        return self._api_endpoint_connected

    @api_endpoint_connected.setter
    def api_endpoint_connected(self, value):
        self._actual.modapi_signal_api_connected(value, self._prefix)
        self._api_endpoint_connected = value

    """ API Task Management """
    @property
    def api_tasks(self):
        return self._api_tasks

    def install_task(self, task, period):
        self._api_tasks.append((task, period))

    def _api_start_all_tasks(self, _):
        for task, period in self.api_tasks:
            t = getattr(self, task)
            if not t.running:
                self.log.info("Starting {task} with period {period}",
                              task=task, period=period)
                t.start(period)
        return succeed(True)

    def _api_stop_all_tasks(self, _):
        for task, _ in self._api_tasks:
            t = getattr(self, task)
            if t.running:
                self.log.info("Stopping {task}", task=task)
                t.stop()
        return succeed(True)

    """ API Connection Management """
    @property
    def api_reconnect_task(self):
        if self._api_reconnect_task is None:
            self._api_reconnect_task = LoopingCall(self.api_engine_activate)
        return self._api_reconnect_task

    def api_engine_activate(self):
        self.log.debug("Attempting to activate {0} API engine.".format(self._prefix))

        def _enter_reconnection_cycle(failure):
            self.log.error("Can't connect to {0} API endpoint".format(self._prefix))

            # TODO Should this be a HTTP API Engine check function or is here fine?
            if failure.check(ServerReportsNotReady):
                self._actual.modapi_signal_api_server_not_ready(failure.value, self._prefix)
            elif failure.check(ConnectionRequirementsNotReady):
                self._actual.modapi_signal_api_params_not_ready(failure.value, self._prefix)
            elif failure.check(WaitingForConnectionError):
                pass
            else:
                self.log.failure("Connection Failure : ", failure=failure)

            self.api_endpoint_connected = False
            if not self.api_reconnect_task.running:
                self.api_engine_reconnect()
            return failure

        if self._api_announce:
            d = getattr(self, self._api_announce)()
            d.addErrback(_enter_reconnection_cycle)
        else:
            d = succeed(True)

        d.addCallback(getattr(self, self._api_probe))

        def _made_connection(_):
            self.log.debug("Made connection")
            self.api_endpoint_connected = True
            self._api_engine_active = True
            if self.api_reconnect_task.running:
                self.api_reconnect_task.stop()
            if self._pqueue_enabled:
                self.log.info("Triggering process of {0} API persistent queue".format(self._prefix))
                self._api_queue.process()
            return

        d.addCallbacks(
            _made_connection,
            _enter_reconnection_cycle
        )

        def _error_handler(failure):
            if self.api_reconnect_task.running:
                return
            else:
                return failure

        d.addCallbacks(
            self._api_start_all_tasks,
            _error_handler
        )
        return d

    def api_engine_reconnect(self):
        if self._api_engine_active:
            self.api_endpoint_connected = False
            self.log.info("Lost connection to {0} API server. Attempting to reconnect."
                          "".format(self._prefix))
        self._api_engine_active = False
        if not self.api_reconnect_task.running:
            self._api_stop_all_tasks(True)
            self.api_reconnect_task.start(self._api_reconnect_frequency)

    def api_engine_stop(self):
        self._api_engine_active = False
        for task, _ in self._api_tasks:
            if getattr(self, task).running:
                getattr(self, task).stop()
        if self.api_reconnect_task.running:
            self.api_reconnect_task.stop()

    @property
    def api_engine_active(self):
        return self._api_engine_active

    def start(self):
        self.api_engine_activate()

    def stop(self):
        self.api_engine_stop()
