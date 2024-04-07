import itertools, re, sys, urllib, urllib.parse
from optparse import OptionParser
from . import common
import lxml.html
import lxml.etree


class Tree:
    def __init__(self, doc, **kwargs):
        if doc is None:
            self.doc = None
        elif isinstance(doc, lxml.html.HtmlElement):
            # input is already a passed lxml tree
            self.doc = doc
        else:
            try:
                try:
                    self.doc = lxml.html.fromstring(doc)
                except ValueError:
                    # For error: Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without declaration
                    self.doc = lxml.html.fromstring(doc.encode('utf-8'))
            except lxml.etree.LxmlError as e:
                print('Error parsing doc:', e)
                self.doc = None

    def search(self, path):
        if self.doc is None:
            return []
        else:
            return [Tree(e) for e in self.doc.xpath(path)]

    def get(self, path):
        es = self.search(path)
        if es:
            return es[0]
        else:
            return Tree(None)

    def __str__(self):
        if self.doc is None:
            return ''
        else:
            try:
                parts = [self.doc.text] + [c if isinstance(c, str) else lxml.etree.tostring(c).decode() for c in self.doc] + [self.doc.tail]
                return ''.join(filter(None, parts)) #or str(self.doc)
            except AttributeError as e:
                print('Error parsing node:', e)
                return ''


def get(html, xpath, remove=None):
    """Return first element from XPath search of HTML
    """
    return str(Tree(html, remove=remove).get(xpath))

def search(html, xpath, remove=None):
    """Return all elements from XPath search of HTML
    """
    return [str(e) for e in Tree(html, remove=remove).search(xpath)]


class Form:
    """Helper class for filling and submitting forms
    """
    def __init__(self, form):
        self.data = {}
        for input_name, input_value in zip(search(form, '//input/@name'), search(form, '//input/@value')):
            self.data[input_name] = input_value
        for text_name, text_value in zip(search(form, '//textarea/@name'), search(form, '//textarea')):
            self.data[text_name] = text_value
        for select_name, select_contents in zip(search(form, '//select/@name'), search(form, '//select')):
            self.data[select_name] = get(select_contents, '/option[@selected]/@value')
        if '' in self.data:
            del self.data['']

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __str__(self):
        return urllib.urlencode(self.data)

    def submit(self, D, action, **argv):
        return D.get(url=action, data=self.data, **argv)



js_re = re.compile('location.href ?= ?[\'"](.*?)[\'"]')
def get_links(html, url=None, local=True, external=True):
    """Return all links from html and convert relative to absolute if source url is provided

    html:
        HTML to parse
    url:
        optional URL for determining path of relative links
    local:
        whether to include links from same domain
    external:
        whether to include linkes from other domains
    """
    def normalize_link(link):
        if urllib.parse.urlsplit(link).scheme in ('http', 'https', ''):
            if '#' in link:
                link = link[:link.index('#')]
            if url:
                link = urllib.parse.urljoin(url, link)
                if not local and common.same_domain(url, link):
                    # local links not included
                    link = None
                if not external and not common.same_domain(url, link):
                    # external links not included
                    link = None
        else:
            link = None # ignore mailto, etc
        return link
    a_links = search(html, '//a/@href')
    i_links = search(html, '//iframe/@src')
    js_links = js_re.findall(html)
    links = []
    for link in a_links + i_links + js_links:
        try:
            link = normalize_link(link)
        except UnicodeError:
            pass
        else:
            if link and link not in links:
                links.append(link)
    return links
