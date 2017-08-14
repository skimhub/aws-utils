# -*- coding: utf-8 -*-

import re

def shallow_word_segment(phrase):
    """segments the words and numbers from a string phrase. Does a shallow word segmentation method using regex
    Examples: u'ãbacon_abcd $123g' -> [u'ãbacon', u'abcd', u'123g']
    Args:
        phrase (str): phrase that has multiple words

    Returns:
        ([str]): list of words segmented
    """
    return re.findall(r'[^_\W]+', phrase, flags=re.UNICODE)


def decode_str_to_unicode(string):
    """Attempt to decode a string of potentially unknown encoding. Currently a very simplistic implementation.
        First try unicode-8 and try windows alternatively
        Args:
            string (str) : string of unknown encoding

        Returns:
            str : string after decoding
    """
    try:
        return string.decode('utf-8')
    except UnicodeDecodeError:
        #  try to encode in windows encoding
        return string.decode('windows-1252', errors='replace')
