

from .core import MQTTCoreEngine
from ..mq.commands import CommandProcessorMixin


class MQTTCommandProcessor(MQTTCoreEngine, CommandProcessorMixin):
    _actions = {}

    def command_extractor(self, cmd):
        return cmd.payload.decode()

    def create_bindings(self):
        super().create_bindings()
        key = self.mq_key_tc
        self.log.info(f"Binding to MQ TC channel with topic {key}")
        self.mqtt.subscribe(key, self.handle_command)
