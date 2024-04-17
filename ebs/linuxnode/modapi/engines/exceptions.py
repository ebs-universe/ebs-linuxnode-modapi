

class ServerReportsNotReady(Exception):
    def __init__(self, msg, data=None):
        self.msg = msg
        self.data = data or {}


class ConnectionRequirementsNotReady(Exception):
    def __init__(self, msg, data=None):
        self.msg = msg
        self.data = data or {}


class PrimaryAuthenticationFailure(Exception):
    pass


class TokenAuthenticationFailure(Exception):
    pass


class WaitingForConnectionError(Exception):
    pass
