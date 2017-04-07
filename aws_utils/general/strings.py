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