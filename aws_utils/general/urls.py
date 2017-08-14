import re
from urllib import unquote_plus
from urlparse import urlparse

from aws_utils.general.strings import decode_str_to_unicode

HTTP_CHECK = r'^http.?:\/\/'

def fully_unquote_plus(s):
    """replaces the plus signs with spaces in urls
    Examples: 'test+1+1' -> 'test 1 1'
    Args:
        s (str):  url

    Returns:
        s/fully_unquote_plus (str): unquoted url
    """
    new_s = unquote_plus(s)
    if s == new_s:
        return s

    return fully_unquote_plus(new_s)


def decode_to_unicode(uri_component):
    """Attempt to decode a uri of potentially unknown encoding. Currently a very simplistic implementation.
    First try unicode-8 and try windows alternatively
    Args:
        uri_component (str) : string of unknown encoding

    Returns:
        str : string
    """
    # TODO: evaluate feasibility of using idna decodeing instead
    return decode_str_to_unicode(uri_component)


def _url_parser(url):
    """wrapper for urlparse to add missing http://. Assumes all urls are http
    Args:
        url (str): url

    Returns:
        (tuple(str)): the parsed url attributes
    """
    if not re.match(HTTP_CHECK, url):
        url = 'http://' + url.lstrip('/')
    return urlparse(url)


def domain_extract(url):
    """extract netloc from url. This is the domain of a url
    Args:
        url (str): url

    Returns:
        netloc (str): domain of the url
    """
    _, netloc, _, _, _, _ = _url_parser(url)
    return netloc

def parse_query_url(url):
    """extract path and query substrings from the url
    Args:
        url (str): the url

    Returns:
        path (str): the path component of the url (excluding the domain)
        query (str): the substring the query
    """
    url = url.encode('utf8') if isinstance(url, unicode) else url
    _, _, path, _, query, _ = _url_parser(url)
    return path, query
