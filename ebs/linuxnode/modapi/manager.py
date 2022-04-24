

from ebs.linuxnode.core.basenode import BaseIoTNode
from ebs.linuxnode.sysinfo import SysinfoMixin


class ModularApiEngineManagerMixin(SysinfoMixin, BaseIoTNode):
    def __init__(self, *args, **kwargs):
        super(ModularApiEngineManagerMixin, self).__init__(*args, **kwargs)
        self._api_engines = []
        self._api_primary = None

    def modapi_install(self, engine, primary=False):
        self.log.info("Installing Modular API Engine {0}".format(engine))
        self._api_engines.append(engine)
        if primary:
            self._api_primary = engine

    def modapi_activate(self):
        for engine in self._api_engines:
            self.log.info("Starting Modular API Engine {0}".format(engine))
            engine.start()

    def modapi_engine(self, name):
        for engine in self._api_engines:
            if engine.name == name:
                return engine

    def modapi_stop(self):
        for engine in self._api_engines:
            self.log.info("Stopping Modular API Engine {0}".format(engine))
            engine.stop()

    def start(self):
        super(ModularApiEngineManagerMixin, self).start()
        self.modapi_activate()

    def stop(self):
        self.modapi_stop()
        super(ModularApiEngineManagerMixin, self).stop()
