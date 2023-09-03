

import os
import shutil
import pickle
import pqueue
from enum import Enum
from persistqueue import Queue
from twisted import logger
from twisted.internet.defer import succeed


class ApiMessageCollector(object):
    def __init__(self, engine, ep_func, discriminator, bulk_ep_func):
        self._api_queue_dir = None
        self._log = None
        self._engine = engine
        self.discriminator = discriminator
        self.container = {}
        self.ep_func = ep_func
        self.bulk_ep_func = bulk_ep_func

    @property
    def api_queue_dir(self):
        return os.path.join(self._api_queue_dir, self.ep_func)

    @api_queue_dir.setter
    def api_queue_dir(self, value):
        self._api_queue_dir = value

    def queue(self, discr='default'):
        if isinstance(discr, Enum):
            discr = discr.value
        p = os.path.join(self.api_queue_dir, discr)
        if not os.path.exists(p):
            os.makedirs(p)
        tp = os.path.join(p, 'tmp')
        if not os.path.exists(tp):
            os.makedirs(tp)
        return Queue(path=p, tempdir=tp)

    @property
    def log(self):
        if not self._log:
            self._log = logger.Logger(namespace="apimc.{0}".format(self.ep_func), source=self)
        return self._log

    def declare_descr_values(self, known_descr_values):
        for descr in known_descr_values:
            self.get_queue(descr)

    def get_queue(self, discr='default'):
        if discr not in self.container.keys():
            self.container[discr] = self.queue(discr)
        return self.container[discr]

    def add_message(self, **kwargs):
        if not self.discriminator:
            discr = 'default'
        else:
            discr = kwargs.pop(self.discriminator)
        if isinstance(discr, Enum):
            discr = discr.value
        queue = self.get_queue(discr)
        queue.put(kwargs)

    def api_requests(self):
        for discr, queue in self.container.items():
            reports = []
            while not queue.empty():
                reports.append(queue.get())
            if len(reports):
                self.log.info("Have {l} '{discr}' messages to dispatch",
                               discr=discr, l=len(reports))
                yield discr, reports, queue.task_done


class ApiPersistentActionQueue(object):
    def __init__(self, api_engine, prefix=None):
        self._collectors = {}
        self._prefix = prefix
        self._api_engine = api_engine
        self._api_queue = None

    def install_collector(self, collector: ApiMessageCollector):
        self._collectors[collector.ep_func] = collector
        collector.api_queue_dir = self._api_queue_dir

    def process(self):
        while True:
            try:
                api_func_name, args = self.api_queue.get_nowait()
                self._api_engine.log.info(
                    "Executing persistent API action : {func_name}, {args}",
                    func_name=api_func_name, args=args
                )
                getattr(self._api_engine, api_func_name)(*args)
            except pqueue.Empty:
                break
            except pickle.UnpicklingError:
                self._api_queue = None
                shutil.rmtree(self._api_queue_dir, ignore_errors=True)
                self._api_engine.log.warn("API persistent queue pickle corrupted. Clearing.")
                break
            except Exception as e:
                # TODO Remove this broad exception
                self._api_queue = None
                shutil.rmtree(self._api_queue_dir, ignore_errors=True)
                self._api_engine.log.warn("Unhandled error in api queue get. \n {} ".format(e))
                break
        return succeed(True)

    def enqueue_action(self, api_func_name, **kwargs):
        if api_func_name not in self._collectors.keys():
            self._api_engine.log.info(
                "Enqueuing API action to disk : {func_name}, {kwargs}",
                func_name=api_func_name, kwargs=kwargs
            )
            self.api_queue.put((api_func_name, kwargs))
        else:
            self._collectors[api_func_name].add_message(**kwargs)

    @property
    def api_queue(self):
        if not self._api_queue:
            self._api_queue = pqueue.Queue(
                self._api_queue_dir,
                tempdir=os.path.join(self._api_queue_dir, 'tmp')
            )
        return self._api_queue

    @property
    def _api_queue_dir(self):
        dir_name = 'apiqueue'
        if self._prefix:
            dir_name = '-'.join([self._prefix, dir_name])
        _api_queue_dir = os.path.join(self._api_engine.cache_dir, dir_name)
        _api_queue_tmp_dir = os.path.join(_api_queue_dir, 'tmp')
        os.makedirs(_api_queue_tmp_dir, exist_ok=True)
        return _api_queue_dir
