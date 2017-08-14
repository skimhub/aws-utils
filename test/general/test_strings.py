# -*- coding: utf-8 -*-

import pytest

from aws_utils.general import strings as string

@pytest.mark.parametrize(('input', 'expected'), [
    ('', []),
    ('1234', ['1234']),
    ('abcd', ['abcd']),
    ('abcd efgh', ['abcd', 'efgh']),
    ('25 tests', ['25', 'tests']),
    # preserves order of words in phrase
    ('efgh 25 abcd ', ['efgh', '25', 'abcd']),
    ('abcd/abcd', ['abcd', 'abcd']),
    (u'ãbacon_abcd $123g', [u'ãbacon', u'abcd', u'123g']),
])
def test_shallow_word_segment(input, expected):
    actual = string.shallow_word_segment(input)
    assert actual == expected


@pytest.mark.parametrize(('input', 'expected'), [
    ('', u''),
    ('bacon', u'bacon'),
    ('b\xc3\xa5con', u'båcon'),  # utf8
    ('b\xe5con', u'båcon'),  # windows-1252
    (u'煎餅'.encode('GBK'), u'¼åïž')
])
def test_decode_to_unicode(input, expected):
    """Ensure default decoding (utf8 and windows-1252) works as expected."""
    actual = string.decode_str_to_unicode(input)

    assert actual == expected
    assert isinstance(actual, unicode)
