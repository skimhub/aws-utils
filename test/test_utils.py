import functools

from aws_utils.utils import poller
from mock.mock import Mock


def test_poller():
    def func_callable():
        pass

    mock = Mock()
    mock.callback_end_polling.return_value = True

    assert poller(func_callable, mock.callback_end_polling, 0, 0) == True

    mock.callback_end_polling.return_value = False
    assert poller(func_callable, mock.callback_end_polling, 0, 0) == False


def test_poller_retry():
    TEST_RETRY_COUNT = 10

    class counter:
        count = 0

    def increment(counter):
        counter.count += 1
        return counter.count

    def callback_end_polling(count):
        return count == TEST_RETRY_COUNT

    func_callable = functools.partial(increment, counter)
    assert poller(func_callable, callback_end_polling, 0, 20) == True
