

from twisted.internet.defer import inlineCallbacks
from ebs.linuxnode.mq.amqp import CantDoThis
from .core import AMQPCoreEngine
import json


class CommandProcessor(AMQPCoreEngine):
    _actions = {}

    @property
    def mq_key_tc(self):
        raise NotImplementedError

    @property
    def mq_exchange(self):
        raise NotImplementedError

    @inlineCallbacks
    def handle_command(self, command):
        cmd = json.loads(command.body)
        self.log.debug(f"Got Command with body {json.dumps(cmd)}")
        if 'action' not in cmd.keys():
            self.log.warn("MQ Command {cmd} Mangled. No 'action' key.", cmd=cmd)
            raise CantDoThis()
        try:
            handler = getattr(self, self._actions[cmd['action']])
            yield handler(cmd)
        except KeyError:
            self.log.warn("MQ Command '{cmd}' Not Supported.", cmd=cmd['action'])
            raise CantDoThis()

    def create_bindings(self):
        super().create_bindings()
        key = self.mq_key_tc
        exchange = self.mq_exchange
        self.log.info(f"Binding to MQ TC channel with key {key} on exchange {exchange}")
        self.amqp.read_messages(exchange, key, self.handle_command)
