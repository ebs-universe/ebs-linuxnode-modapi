

from twisted.internet.defer import inlineCallbacks
from ebs.linuxnode.mq.common import CantDoThis
import json


class CommandProcessorMixin(object):
    _actions = {}

    @property
    def log(self):
        raise NotImplementedError

    @property
    def mq_key_tc(self):
        raise NotImplementedError

    @property
    def mq_exchange(self):
        raise NotImplementedError

    def command_extractor(self, cmd):
        return cmd

    @inlineCallbacks
    def handle_command(self, command):
        cmd = json.loads(self.command_extractor(command))
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
        raise NotImplementedError
