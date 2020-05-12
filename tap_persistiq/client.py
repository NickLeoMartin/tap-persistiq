import time
import backoff
import requests
from requests.exceptions import ConnectionError
from singer import metrics, utils
import singer

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class PersistIQError(Exception):
    pass


class PersistIQBadRequestError(PersistIQError):
    pass


class PersistIQUnauthorizedError(PersistIQError):
    pass


class PersistIQNotFoundError(PersistIQError):
    pass


class PersistIQTooManyRequestsError(PersistIQError):
    pass


class PersistIQInternalServiceError(PersistIQError):
    pass


# Error codes: http://apidocs.persistiq.com/#errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: PersistIQBadRequestError,
    401: PersistIQUnauthorizedError,
    404: PersistIQNotFoundError,
    429: PersistIQTooManyRequestsError,
    500: PersistIQInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, PersistIQError)


def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since PersistIQ has neither sent
                # us a 2xx response nor a response content.
                return
            response_json = response.json()
            status_code = response.status_code
            LOGGER.error('RESPONSE: {}'.format(response_json))
            # Error Message format:
            # http://apidocs.persistiq.com/#errors
            if response_json.get('status') == 'error':
                message = ''
                for err in response_json['error']:
                    error_message = err.get('message')
                    error_reason = err.get('reason')
                    ex = get_exception_for_error_code(status_code)
                    if status_code == 401:
                        LOGGER.error(error_message)
                    message = '{}: {}\n{}'.format(
                        error_reason, error_message, message)
                raise ex('{}'.format(message))
            raise PersistIQError(error)
        except (ValueError, TypeError):
            raise PersistIQError(error)


class PersistIQClient(object):
    def __init__(self,
                 access_token,
                 user_agent=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        # Rate limit initial values, reset by check_access_token headers
        self.__session = requests.Session()
        self.__verified = False
        self.base_url = 'https://api.persistiq.com/v1'

    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    #TODO: check rate limiting setup: http://apidocs.persistiq.com/#errors
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1000, 60)
    def check_access_token(self):
        if self.__access_token is None:
            raise Exception('Error: Missing access_token.')
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['x-api-key'] = self.__access_token
        headers['Accept'] = 'application/json'
        response = self.__session.get(
            # Simple endpoint that returns 1 Account record (to check API/access_token access):
            url='{}/{}'.format(self.base_url, 'users'),
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            resp = response.json()
            if 'type' in resp:
                return True
            else:
                return False

    #TODO: check rate limiting setup: http://apidocs.persistiq.com/#errors
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1000, 60)
    def request(self, method, path=None, url=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['x-api-key'] = self.__access_token
        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
