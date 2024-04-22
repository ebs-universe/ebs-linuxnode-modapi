

from copy import copy
from ebs.linuxnode.core.http import HTTPError
from .base import ModularApiEngineBase


class ModularHttpApiEngine(ModularApiEngineBase):
    _api_baseurl = ''
    _api_headers = {}

    _auth_url = ''
    _auth_headers = {}

    def __init__(self, actual, config=None):
        super(ModularHttpApiEngine, self).__init__(actual, config)
        self._api_token = None
        self._internet_connected = False
        self._internet_link = None

    """ Proxy to Core Engine """
    @property
    def http_get(self):
        return self._actual.http_get

    @property
    def http_post(self):
        return self._actual.http_post

    @property
    def network_info(self):
        return self._actual.network_info

    """ Network Status Primitives """

    # TODO Consider moving the core network status primitives
    #  entirely into the manager instead

    @property
    def internet_connected(self):
        return self._internet_connected

    @internet_connected.setter
    def internet_connected(self, value):
        self._actual.modapi_signal_internet_connected(value, self._prefix)
        self._internet_connected = value

    @property
    def internet_link(self):
        return self._internet_link

    @internet_link.setter
    def internet_link(self, value):
        self._actual.modapi_signal_internet_link(value, self._prefix)
        self._internet_link = value

    """ API Engine Management """
    def api_engine_activate(self):
        # Probe for internet
        d = self.http_get('https://www.google.com')

        def _get_internet_info(maybe_failure):
            ld = self.network_info

            def _set_internet_link(l):
                if l:
                    if isinstance(l, str):
                        self.internet_link = l
                    else:
                        self.internet_link = l.decode('utf-8')
            ld.addCallback(_set_internet_link)
            return maybe_failure
        d.addBoth(_get_internet_info)

        def _made_connection(_):
            self.log.debug("Have Internet Connection")
            self.internet_connected = True

        def _enter_reconnection_cycle(failure):
            self.log.error("No Internet!")
            self.internet_connected = False
            if not self.api_reconnect_task.running:
                self.api_engine_reconnect()
            return failure

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
            lambda _: ModularApiEngineBase.api_engine_activate(self),
            _error_handler
        )
        return d

    @property
    def api_token(self):
        raise NotImplementedError

    def api_token_reset(self):
        raise NotImplementedError

    def _strip_auth(self, headers):
        if b'Authorization' in headers.keys():
            rv = copy(headers)
            rv[b'Authorization'] = rv[b'Authorization'][:10] + b'...'
            return rv
        return headers

    """ Core HTTP API Executor """
    def _api_execute(self, ep, request_builder, response_handler):
        url = "{0}/{1}".format(self.api_url, ep)

        d = request_builder()

        def _get_response(req: dict):
            language = req.pop('_language', 'JSON')
            language = language.upper()
            method = req.pop('_method', 'POST')
            method = method.upper()
            headers = copy(self._api_headers)
            bearer_token = req.pop('_token', None)
            if bearer_token:
                headers[b'Authorization'] = b'Bearer ' + bearer_token.encode('ascii')
            self.log.debug("Executing {language} API {method} Request to {url} \n"
                           "   with content '{content}'\n"
                           "   and headers '{headers}'",
                           url=url, content=req, headers=self._strip_auth(headers),
                           method=method, language=language)
            params = req.pop('_query', [])
            files = req.pop('_files', {})
            for key, fpath in files.items():
                files[key] = open(fpath, 'rb')
            request_structure = {
                'json': req,
                'params': params,
                'files': files
            }
            request_structure = {k: v for k, v in request_structure.items() if v}
            if method == 'POST':
                r = self.http_post(url, timeout=120, headers=headers,
                                   **request_structure)
            elif method == 'GET':
                r = self.http_get(url, timeout=120, headers=headers,
                                  **request_structure)
            else:
                raise ValueError("Method {} not recognized".format(method))

            def _close_files(*args):
                for f in files.values():
                    f.close()
                return args

            if files:
                r.addBoth(_close_files)
            if language == 'JSON':
                r.addCallbacks(
                    self._parse_json_response
                )
            return r
        d.addCallback(_get_response)

        def _error_handler(failure):
            self.log.failure("Attempting to handle API Error for API request to "
                             "endpoint '{endpoint}'", failure=failure, endpoint=ep)
            if isinstance(failure.value, HTTPError) and \
                    failure.value.response.code in [401, 403]:
                self.log.info(f"Encountered {failure.value.response.code} Error. "
                              f"Attempting API Token Reset.")
                self.api_token_reset()
            if not self.api_reconnect_task.running:
                self.log.debug("Starting API Reconnect Task")
                self.api_engine_reconnect()
            return failure
        d.addCallbacks(response_handler, _error_handler)
        return d

    @property
    def api_url(self):
        if self._api_baseurl.startswith('config'):
            cft = self._api_baseurl.split(':')[1]
            return getattr(self.config, cft)
        return self._api_baseurl

    @property
    def auth_url(self):
        if self._auth_url.startswith('config'):
            cft = self._auth_url.split(':')[1]
            return getattr(self.config, cft)
        return self._auth_url

    def start(self):
        super(ModularHttpApiEngine, self).start()

    def stop(self):
        super(ModularHttpApiEngine, self).stop()

    # API Language Support Infrastructure

    # JSON
    def _parse_json_response(self, response):
        self.log.debug("Attempting to extract JSON from response {r}", r=response)
        return response.json()
