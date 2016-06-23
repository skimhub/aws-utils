import logging
import time


logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class NoSuchActivityError(Exception):
    pass


def poller(status_getter, interval, callback):
    """
    Call `callback` until exception or return False.
    """
    try:
        while True:
            status = status_getter()

            if callback(status):
                pass
            else:
                return False
            time.sleep(interval)
    except Exception as e:
        logger.exception('Unable to poll for status : ', e)