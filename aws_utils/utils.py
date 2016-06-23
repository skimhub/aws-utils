import logging
import sys
import time

import boto


try:
    import configparser
except ImportError:
    import ConfigParser as configparser

logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def set_boto_retry_attemps_config_option(num_of_attempts):
    try:
        boto.config.add_section("Boto")
    except configparser.DuplicateSectionError:
        pass
    boto.config.set(
        "Boto", "metadata_service_num_attempts", str(num_of_attempts))


def poller(func_callable, callback_end_polling, retry_interval_seconds, max_retry_count=sys.maxsize):
    """
    Call `func_callable` and retry until `callback_end_polling` is True or exception.

    Args:
        func_callable: function to call multiple times
        callback_end_polling: boolean function that accepts the value returned by `func_callable`. When True the pooling terminates.  
        retry_interval_seconds:
        max_retry_count:
    Returns:
        True when `callback_end_polling` is true, and otherwise if false every time of the retry.
    """
    try:
        retry_count = 0
        while retry_count <= max_retry_count:
            value = func_callable()
            if callback_end_polling(value):
                return True
            retry_count += 1
            logger.debug(
                'Waiting for %s seconds for retry attempt number %d', retry_interval_seconds, retry_count)
            time.sleep(retry_interval_seconds)
        return False
    except Exception as e:
        logger.exception('Unable to poll with exception: %s', e)
