# -*- coding: utf-8 -*-

import os, re, signal, sys, time, urllib, zipfile
from http.cookiejar import Cookie, CookieJar
from . import download, xpath
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By 
from selenium.webdriver.common.proxy import Proxy, ProxyType


class CacheBrowser:
    def __init__(self, executable_path='~/bin/chromedriver', headless=True, cache=None, cookie_jar=None, cookie_key=None, proxy=None, init_callback=None, timeout=None):
        self.chrome_service = Service(executable_path=os.path.expanduser(executable_path))
        self.chrome_options = Options()
        self.chrome_options.add_argument("--disable-dev-shm-usage") # https://stackoverflow.com/a/50725918/1689770
        self.chrome_options.add_argument("--disable-gpu") #https://stackoverflow.com/questions/51959986/how-to-solve-selenium-chromedriver-timed-out-receiving-message-from-renderer-exc
        if headless:
            self.chrome_options.add_argument('--headless')

        self.set_proxy(proxy)
        self.init_callback = init_callback

        self.browser = None
        self.timeout = timeout
        #self.capabilities = webdriver.DesiredCapabilities.CHROME
        self.cache = download.Download().cache if cache is None else cache
        self.cookie_key = cookie_key
        if cookie_key:
            try:
                self.cookies = self.cache[cookie_key]
                print('loading:', self.cookies)
            except KeyError:
                self.cookies = []
        else:
            self.cookies = self.format_cookies(cookie_jar)
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def init(self):
        if self.browser is None:
            self.browser = Chrome(service=self.chrome_service, options=self.chrome_options)#, desired_capabilities=self.capabilities)
            if self.init_callback is not None:
                self.init_callback()

    def exit_gracefully(self, signum, frame):
        self.close()
        sys.exit(1)

    def close(self):
        self.save_cookies()
        if self.browser is not None:
            self.browser.quit()

    def format_cookies(self, cookie_jar):
        cookies = []
        for cookie in cookie_jar or []:
            cookies.append({'name': cookie.name, 'value': cookie.value, 'path': cookie.path, 'domain': cookie.domain, 'secure': cookie.secure, 'expiry': cookie.expiry})
        return cookies

    def load_cookies(self, url):
        cookies = []
        loaded_cookies = False
        for cookie in self.cookies:
            if cookie['domain'] in url:
                # can only load cookies when at the domain
                self.browser.add_cookie(cookie)
                loaded_cookies = True
            else:
                cookies.append(cookie)
        if loaded_cookies:
            # need to reload page with cookies
            print('reload cookies')
            self.browser.get(url)
            self.cookies = cookies

    def get_cookies(self):
        cj = CookieJar()
        for c in self.browser.get_cookies():
            cj.set_cookie(Cookie(0, c['name'], c['value'], None, False, c['domain'], c['domain'].startswith('.'), c['domain'].startswith('.'), c['path'], True, c['secure'], c.get('expiry', 2147483647), False, None, None, {}))
        return cj

    def save_cookies(self):
        if self.cookie_key is not None and self.browser is not None:
            print('saving:', self.browser.get_cookies())
            self.cache[self.cookie_key] = self.browser.get_cookies()

    def parse_proxy(self, http_proxy):
        match = re.search('//(.*?):(.*?)@(.*?):(\d+)', http_proxy)
        if match:
            proxy_user, proxy_pass, proxy_host, proxy_port = match.groups()
            print(match.groups())
        else:
            match = re.match('([\d\.]+):(\d+)', http_proxy)
            proxy_host, proxy_port = match.groups()
            proxy_user = proxy_pass = ''
        return proxy_user, proxy_pass, proxy_host, proxy_port

    def set_proxy(self, http_proxy):
        self._proxy = http_proxy
        if not http_proxy:
            return
        proxy_user, proxy_pass, proxy_host, proxy_port = self.parse_proxy(http_proxy)
        manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
        """

        background_js = """
            var config = {
                    mode: "fixed_servers",
                    rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                    }
                };

            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }

            chrome.webRequest.onAuthRequired.addListener(
                        callbackFn,
                        {urls: ["<all_urls>"]},
                        ['blocking']
            );
        """ % (proxy_host, proxy_port, proxy_user, proxy_pass)
        pluginfile = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(pluginfile, 'w') as zp:
            zp.writestr('manifest.json', manifest_json)
            zp.writestr('background.js', background_js)
        self.chrome_options.add_extension(pluginfile)


    def wait(self, xpath):
        self.browser.implicitly_wait(20)
        return self.browser.find_element(By.XPATH, xpath)


    def get(self, url, read_cache=True, write_cache=True, retry=True, delay=5, wait_xpath=None):
        try:
            if not read_cache:
                raise KeyError()
            response = self.cache[url]
            if not response and retry:
                raise KeyError()
        except KeyError:
            self.init()
            print('Rendering:', url)
            if self.timeout:
                self.browser.set_page_load_timeout(self.timeout)
            try:
                self.browser.get(url)
            except TimeoutException:
                print('Request timed out')
                response = download.Response('', 408, 'Request timed out')
            else: 
                time.sleep(delay)
                if wait_xpath:
                    self.wait(wait_xpath)
                self.load_cookies(url)
                html = self.browser.page_source
                # chrome will wrap JSON in pre - how to solve this properly?
                if '<body><pre>{' in html:
                    html = xpath.get(html, '/html/body/pre')
                response = download.Response(html, 200, '')
                if write_cache:
                    self.cache[url] = response
                self.save_cookies()
        return response
