
import json, random, re, time, os, urllib.parse
import concurrent
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Callable
import requests
from . import pdict, services, settings, xpath


SUCCESS_STATUS = (200, )
NON_RETRIABLE_STATUS = (404, )


@dataclass
class Request:
    url: str
    headers: dict = None
    data: str = None
    callback: Callable = None

    def get_key(self):
        """Create key for caching this request
        """
        key = self.url
        if self.data:
            key = '{} {}'.format(key, self.data)
        return key


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

    def __bool__(self):
        return self.status_code in SUCCESS_STATUS


class Throttle:
    def __init__(self, delay=0):
        self.delay = delay
        self.last_time = {}

    def __call__(self, delay, ip):
        delay = self.delay if delay is None else delay
        seconds = delay * (0.5 + random.random())
        last_time = self.last_time.get(ip, datetime.now() - timedelta(seconds=seconds))
        next_time = last_time + timedelta(seconds=seconds)
        while next_time > datetime.now():
            time.sleep(0.1)
        self.last_time[ip] = next_time


class Download:
    def __init__(self, cache_file='', cache=None, session=None, delay=1, max_retries=1, proxy_file=None, proxies=None, cache_expires=None, timeout=30):
        self.cache = cache or pdict.PersistentDict(cache_file or settings.cache_file, expires=cache_expires)
        self.session = session
        self.timeout = timeout
        self.max_retries = max_retries
        self.proxies = open(proxy_file).read().splitlines() if proxy_file and os.path.exists(proxy_file) else proxies
        self._throttle = Throttle(delay)

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


    def get(self, url, delay=None, max_retries=None, user_agent='', read_cache=True, write_cache=True, headers=None, data=None, ssl_verify=True, auto_encoding=True):
        if isinstance(data, dict):
            data = urllib.parse.urlencode(sorted(data.items()))
        key = Request(url, data=data).get_key()
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
                self._throttle(delay, proxies['http'] if proxies else None)
                try:
                    if data is not None:
                        request_response = session.post(url, headers=headers, data=data, verify=ssl_verify, proxies=proxies, timeout=self.timeout)
                    else:
                        request_response = session.get(url, headers=headers, verify=ssl_verify, proxies=proxies, timeout=self.timeout)
                except Exception as e:
                    print('Download error:', e)
                    response = Response('', 500, str(e))
                else:
                    print('Download:', url, request_response.status_code)
                    content = request_response.content if not request_response.encoding or not auto_encoding else request_response.text
                    response = Response(content, request_response.status_code, request_response.reason)
                    if not self._should_retry(response, num_failures):
                        break
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


    def geocode(self, address, api_key):
        gm = services.GoogleMaps(self, api_key)
        return gm.geocode(address)


    def threaded(self, requests, max_workers=4, max_queue=1000):
        def process_callback(request, response):
            if request.callback:
                for next_request in request.callback(request, response) or []:
                    if isinstance(next_request, Request):
                        requests.append(next_request)
                    else:
                        yield next_request

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while requests:
                # avoid loading too many requests into memory at once
                cur_requests = []
                for _ in range(max_queue):
                    if requests:
                        cur_requests.append(requests.pop())
                    else:
                        break

                future_to_request = {}
                for request in cur_requests:
                    try:
                        response = self.cache[request.get_key()]
                        if self._should_retry(response):
                            raise KeyError()
                    except KeyError:
                        future = executor.submit(self.get, url=request.url, headers=request.headers, data=request.data, read_cache=False, write_cache=False)
                        future_to_request[future] = request
                    else:
                        yield from process_callback(request, response)

                # process the completed callbacks
                for future in concurrent.futures.as_completed(future_to_request):
                    request = future_to_request[future]
                    try:
                        response = future.result()
                    except Exception as e:
                        print('{} generated an exception: {}'.format(request.url, e))
                    else:
                        self.cache[request.get_key()] = response
                        yield from process_callback(request, response)
                    del future_to_request[future]
