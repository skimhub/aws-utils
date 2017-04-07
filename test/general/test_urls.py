# -*- coding: utf-8 -*-

import pytest

from aws_utils.general import urls


@pytest.mark.parametrize(('input', 'expected'), [
    # normal
    (u'test1', u'test1'),
    # with multiple plus
    (u'test+1+1', u'test 1 1'),
     # string input
    ('test+1+1', 'test 1 1'),
    # other special charactors representing +
    (u'hello%2520dave%2520yo', u'hello dave yo'),
    (u'hello%252520dave%252520yo', u'hello dave yo'),
    (u'hello%25252520dave%25252520yo', u'hello dave yo'),
    (u'hello%2525252520dave%2525252520yo', u'hello dave yo'),
    (u"'h'ello'+dave's'+", u"'h'ello' dave's' "),
])
def test_fully_unquote_plus(input, expected):
    """Ensure URL params are parsed as expected. No filtering."""
    terms = urls.fully_unquote_plus(input)
    assert terms == expected


@pytest.mark.parametrize(('url', 'expected'), [
    (u'http://www.cool.com/search?q=lovely+horses', ('/search', 'q=lovely+horses')),
    (u'//www.bacon.com/search?q=lovely+horses', ('/search', 'q=lovely+horses')),
    (u'www.cool.com/search?q=lovely-horses', ('/search', 'q=lovely-horses')),
])
def test_parse_query_url(url, expected):
    actual = urls.parse_query_url(url)
    assert actual == expected

@pytest.mark.parametrize(('url', 'expected'), [
    (u'http://www.cool.com/search?q=lovely+horses', 'www.cool.com'),
    (u'//www.bacon.com/search?q=lovely+horses', 'www.bacon.com'),
    (u'www.cool.com/search?q=lovely+horseslajksbdljkabnsdkl', 'www.cool.com'),
    (u'////www.horseworld.com/search?q=lovely+horses', 'www.horseworld.com'),
])
def test_domain_extract(url, expected):
    out = urls.domain_extract(url)
    assert out == expected

@pytest.mark.parametrize(('input', 'expected'), [
    ('', u''),
    ('bacon', u'bacon'),
    ('b\xc3\xa5con', u'båcon'),  # utf8
    ('b\xe5con', u'båcon'),  # windows-1252
    (u'煎餅'.encode('GBK'), u'¼åïž')
])
def test_decode_to_unicode(input, expected):
    """Ensure default decoding (utf8 and windows-1252) works as expected."""
    actual = urls.decode_to_unicode(input)

    assert actual == expected
    assert isinstance(actual, unicode)
