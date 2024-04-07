
import json, random, re, time, os, urllib.parse
from datetime import datetime, timedelta
import requests
from . import pdict, services, settings, xpath


SUCCESS_STATUS = (200, )
NON_RETRIABLE_STATUS = (404, )


class Response:
    def __init__(self, text, status_code, reason):
        self.text = text
        self.status_code = status_code
        self.reason = reason
        self.tree = None

    def get(self, path):
        if self.tree is None:
            self.tree = xpath.Tree(self.text)
        return self.tree.get(path)

    def search(self, path):
        if self.tree is None:
            self.tree = xpath.Tree(self.text)
        return self.tree.search(path)

    def regex(self, r):
        return re.search(r, self.text)

    def findall(self, r):
        return re.findall(r, self.text)

    def json(self):
        return json.loads(self.text)

    def __str__(self):
        return '{}: {}'.format(self.status_code, self.text[:100] if self.text else '')


class Download:
    def __init__(self, cache_file='', session=None, delay=1, max_retries=1, proxy_file=None, cache_expires=None, timeout=30):
        self.cache = pdict.PersistentDict(cache_file or settings.cache_file, expires=cache_expires)
        self.session = session
        self.delay = delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.last_time = {}
        self.proxies = open(proxy_file).read().splitlines() if proxy_file and os.path.exists(proxy_file) else None


    def _format_headers(self, url, headers, user_agent):
        headers = headers or {}
        if user_agent:
            headers['User-Agent'] = user_agent
        for name, value in settings.default_headers.items():
            headers[name] = value
        return headers


    def _should_retry(self, response, num_failures=0):
        if response.status_code in SUCCESS_STATUS or response.status_code in NON_RETRIABLE_STATUS:
            return False
        else:
            return num_failures < self.max_retries


    def _throttle(self, delay, ip):
        delay = self.delay if delay is None else delay
        seconds = delay * (0.5 + random.random())
        last_time = self.last_time.get(ip, datetime.now() - timedelta(seconds=seconds))
        next_time = last_time + timedelta(seconds=seconds)
        while next_time > datetime.now():
            time.sleep(0.1)
        self.last_time[ip] = next_time
            

    def get(self, url, delay=None, max_retries=None, user_agent='', read_cache=True, write_cache=True, headers=None, data=None, ssl=True):
        if isinstance(data, dict):
            data = urllib.parse.urlencode(sorted(data.items()))
        key = self.get_key(url, data)
        try:
            if not read_cache:
                raise KeyError()
            response = self.cache[key]
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
            for num_failures in range(max_retries + 1):
                proxies = self.get_proxy()
                try:
                    if data:
                        request_response = session.post(url, headers=headers, data=data, verify=ssl, proxies=proxies, timeout=self.timeout)
                    else:
                        request_response = session.get(url, headers=headers, verify=ssl, proxies=proxies, timeout=self.timeout)
                except Exception as e:
                    print('Download error:', e)
                    response = Response('', 500, str(e))
                else:
                    print('Download:', url, request_response.status_code)
                    content = request_response.content if not request_response.encoding else request_response.text
                    response = Response(content, request_response.status_code, request_response.reason)
                    if not self._should_retry(response, num_failures):
                        break
                finally:
                    self._throttle(delay, proxies['http'] if proxies else None)
            if write_cache:
                self.cache[key] = response
        return response


    def get_proxy(self):
        if self.proxies:
            proxy = self.proxies.pop()
            self.proxies = [proxy] + self.proxies
            return {
                'http': proxy,
                'https': proxy,
            }


    def get_key(self, url, data=None):
        """Create key for caching this request
        """
        key = url
        if data:
            key = '{} {}'.format(key, data)
        return key

    def geocode(self, address, api_key):
        gm = services.GoogleMaps(self, api_key)
        return gm.geocode(address)
