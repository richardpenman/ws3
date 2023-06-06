
import random, time
from datetime import datetime, timedelta
import requests
from . import pdict, settings


SUCCESS_STATUS = (200, )
NON_RETRIABLE_STATUS = (404, )


class Response:
    def __init__(self, text, status_code, reason):
        self.text = text
        self.status_code = status_code
        self.reason = reason



class Download:
    def __init__(self, cache_file='', session=None, delay=0, max_retries=1):
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file)
        self.session = session
        self.delay = 0
        self.max_retries = max_retries
        self.last_time = datetime.now()

    def _format_headers(self, url, headers, user_agent):
        headers = headers or {}
        if user_agent:
            headers['User-Agent'] = user_agent
        for name, value in settings.default_headers.items():
            #if name == 'Referer':
            #    value = url
            headers[name] = value
        return headers


    def _should_retry(self, response, num_failures=0):
        if response.status_code in SUCCESS_STATUS or response.status_code in NON_RETRIABLE_STATUS:
            return False
        else:
            return num_failures < self.max_retries


    def _throttle(self, delay):
        delay = self.delay if delay is None else delay
        seconds = delay * (0.5 + random.random())
        next_time = datetime.now() + timedelta(seconds=seconds)
        if next_time > self.last_time:
            time.sleep(seconds)
        self.last_time = next_time
            

    def get(self, url, delay=None, max_retries=None, user_agent='', headers=None):
        try:
            response = self.cache[url]
            if not isinstance(response, Response):
                response = Response(response, 200, '')
            if self._should_retry(response):
                raise KeyError()

        except KeyError:
            if self.session is None:
                session = requests.session()
            else:
                session = self.session
            headers = self._format_headers(url, headers, user_agent)
            max_retries = self.max_retries if max_retries is None else max_retries
            for num_failures in range(max_retries or self.max_retries):
                self._throttle(delay)
                request_response = session.get(url, headers=headers)
                print('Download:', url, request_response.status_code)
                response = Response(request_response.text, request_response.status_code, request_response.reason)
                if not self._should_retry(response, num_failures):
                    break
            self.cache[url] = response
        return response


