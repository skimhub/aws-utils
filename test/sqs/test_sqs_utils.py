import moto
import pytest

from test.utils import create_test_sqs_queue, put_messages_in_sqs_queue

from aws_utils.sqs import sqs_utils as sqs

TEST_SQS_QUEUE_NAME = "test-queue"

@moto.mock_sqs
@pytest.mark.parametrize(('input'), [
    (''),
    ('1234'),
    ('123456789101112131415'),
])
def test_delete_messages_from_queue(input):
    data = list(input)
    queue = create_test_sqs_queue(TEST_SQS_QUEUE_NAME)
    put_messages_in_sqs_queue(queue, data)

    sqs.delete_all_messages_from_queue(queue)

    assert queue.count() == 0

