

from dataclasses import dataclass
from twisted import logger
from twisted.internet import reactor
from twisted.application import service
from paho.mqtt import client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion
from paho.mqtt.enums import CallbackAPIVersion


@dataclass
class MQTTConnectionParameters:
    host: str = 'localhost'
    port: int = 1883
    client_id: str = 'unknown'
    protocol_version: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
    reconnect_on_failure: bool = True
    manual_ack: bool = False
    username: str = None
    password: str = None


# NOTE :
#  Paho runs in its own thread and maintains its own loop. We don't interfere with it
#  or its connections in any way. We just have a thin wrapper Service around it which
#  lets us control it and get callbacks when something interesting happens.

class PahoMQTTService(service.MultiService):
    _name = 'paho.service'

    def __init__(self, parameters: MQTTConnectionParameters, connectionStateHandler=None, postfix=None):
        self._postfix = postfix
        self._log = None
        self._client : mqtt.Client = None
        self._subscriptions = {}
        self._parameters = parameters or MQTTConnectionParameters()
        self.connectionStateHandler = connectionStateHandler
        service.MultiService.__init__(self)

    @property
    def name(self):
        if self._postfix:
            return f'{self._name}:{self._postfix}'
        else:
            return self._name

    @property
    def log(self):
        if not self._log:
            self._log = logger.Logger(namespace=self.name, source=self)
        return self._log

    def onConnect(self, client, userdata, flags, rc, properties):
        self.log.info(f"Connected to MQTT broker {self._postfix}")
        if self.connectionStateHandler:
            reactor.callFromThread(self.connectionStateHandler, True)
        # Create all subscriptions on connection so that if there is a disconnect,
        #  the subscriptions will be recreated when we reconnect to the broker.
        for topic in self._subscriptions.keys():
            self.log.debug(f"Subscribing to topic {topic}")
            self._client.subscribe(topic)

    def onDisconnect(self, client, userdata, flags, rc, properties):
        self.log.info(f"Disconnected from MQTT broker {self._postfix}")
        if self.connectionStateHandler:
            reactor.callFromThread(self.connectionStateHandler, False)

    def onMessage(self, client, userdata, msg):
        self.log.debug(f"Received message from {msg.topic}: {msg.payload.decode()}")
        if msg.topic in self._subscriptions.keys():
            reactor.callFromThread(self._subscriptions[msg.topic], msg)

    def subscribe(self, topic, handler):
        if topic in self._subscriptions.keys():
            raise ValueError("Already Subscribed (?)")
        self._subscriptions[topic] = handler
        if self._client:
            # TODO There are probably scary race conditions involved here.
            #   Ensure that paho will do the right thing if the same topic
            #   is subscribed to multiple times. Also make sure that when
            #   we do have a self._client, self._client.subscribe will work
            #   even if we aren't connected to the broker. An alternative may
            #   be to make self._subscriptions reliably reflect server state
            #   with on_subscribe, on_unsubscribe, on_disconnect, etc. but
            #   that'll be a pain
            self.log.debug(f"Subscribing to topic {topic}")
            self._client.subscribe(topic)

    def publish(self, topic, payload):
        # TODO Review QoS requirements on publish. We presently don't really
        #   care if messages from the server are lost so QoS 0 is appropriate.
        #   When we are sending messages to the server, though, we might need
        #   some kind of delivery guarantee, or at least guaranteed correct
        #   information about whether the message was delivered or not.
        #   Note that Changing QoS will have implications on RMQ scopes
        #   as well.
        raise NotImplementedError

    def buildClient(self):
        self.log.debug("Building MQTT Client")
        self._client = mqtt.Client(
            client_id=self._parameters.client_id,
            clean_session=True,
            protocol=self._parameters.protocol_version,
            reconnect_on_failure=self._parameters.reconnect_on_failure,
            manual_ack=self._parameters.manual_ack,
            callback_api_version=CallbackAPIVersion.VERSION2
        )
        self._client.username_pw_set(
            username=self._parameters.username,
            password=self._parameters.password,
        )
        self._client.on_connect = self.onConnect
        self._client.on_disconnect = self.onDisconnect
        self._client.on_message = self.onMessage

    def connect(self):
        self.log.debug("Connecting to MQTT server")
        self._client.connect(host=self._parameters.host, port=self._parameters.port)
        self._client.loop_start()

    def startService(self):
        self.log.info(f"Starting MQTT service '{self._name}'")
        self.buildClient()
        self.connect()
        service.MultiService.startService(self)


if __name__ == "__main__":
    import sys
    import logging
    from twisted.python import log
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    observer = log.PythonLoggingObserver()
    observer.start()
    mqtt_service = PahoMQTTService(parameters=MQTTConnectionParameters(
        host = 'localhost',
        port = 1883,
        client_id = 'a0cec86a6386',
        username = 'a0cec86a6386',
        password = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjJCaS05UWJVRS1ZMVdoT1ZUcHNFSiJ9.eyJodHRwczovL3RlbmRyaWwubGluay9zY2hlbWEvYXV0aDAvZW1haWwiOiJhMGNlYzg2YTYzODZAc3htLnN0YXJ4bWVkaWEuaW4iLCJ1bmFtZSI6ImEwY2VjODZhNjM4NiIsImlzcyI6Imh0dHBzOi8vc3RhcnhtZWRpYS5ldS5hdXRoMC5jb20vIiwic3ViIjoiYXV0aDB8NjYxODQ4YzFjM2I2MGVmZmZmMDMzOTdkIiwiYXVkIjoiaHR0cHM6Ly9lZGdlLnN0YXJ4bWVkaWEuaW4vdjEiLCJpYXQiOjE3MTM0OTk2NzEsImV4cCI6MTcxMzU4NjA3MSwiZ3R5IjoicGFzc3dvcmQiLCJhenAiOiJBeVdZb2JTV3dwWUtVVkt1c2FudGd0OFF3N1JBUndPMiIsInBlcm1pc3Npb25zIjpbIm1xOi8vZGV2aWNlIl19.chLf5ZD72ixX_jceK4beR7JclYgva53ONi8KVJwhmqIBU-41RLCZ3lPZHQ9_s74JA54_SkI7qPE8eS_t6Vr-85YxfOIWFAG3_adch5uEww6RAOWG6-uFHXR8EUnJZ9gphjZEYt-_8IaCusK9n8VoS0wXLELJAV1rz4540UwFhyImm8A6BwqaWWTLBtid7R_X1h_EU6WGFd_ObUlHDxi4NGsJrX1ons6J02YvF0vp-qAH9N5iWuQgRSfTE2cX7Ya4fpGwL6QpP3WAZprXNC1YWOgtoY8vSU-kvEqb-NKxFF4YgrYqsJ5g5zHaE7snIrNF9QqZf3IjHArqLJn3cJnoWg'
    ))

    def _message_handler(msg):
        print(msg)
    mqtt_service.subscribe('device/to/a0cec86a6386', _message_handler)

    application = service.Application("TestMQTTApplication")
    mqtt_service.setServiceParent(application)
    reactor.callWhenRunning(mqtt_service.startService)
    reactor.run()
