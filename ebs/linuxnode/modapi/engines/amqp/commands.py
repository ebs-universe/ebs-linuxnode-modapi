

from .core import AMQPCoreEngine
from ..mq.commands import CommandProcessorMixin


class AMQPCommandProcessor(AMQPCoreEngine, CommandProcessorMixin):
    _actions = {}

    def command_extractor(self, cmd):
        return cmd.body

    def create_bindings(self):
        super().create_bindings()
        key = self.mq_key_tc
        exchange = self.mq_exchange
        self.log.info(f"Binding to MQ TC channel with key {key} on exchange {exchange}")
        self.amqp.read_messages(exchange, key, self.handle_command)
