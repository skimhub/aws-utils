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
