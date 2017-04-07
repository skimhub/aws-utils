import boto
from boto.sqs.queue import Queue

QUEUE_BATCH_SIZE = 10

def _get_queue_object(queue):
    # if the queue name is given
    if isinstance(queue, str):
        sqs_conn = boto.connect_sqs()
        return sqs_conn.get_queue(queue)
    elif not isinstance(queue, Queue):
        raise TypeError("SQS queue is not a Queue object")
    return queue


def delete_all_messages_from_queue(queue):
    """deletes the messages available in a sqs queue. Ideal to clear a queue and reset it.  messages are
        deleted in batches to make it memory safe. Therefore, take into account that new messages might arrive while the
        deletion is taking place.
    Args:
        queue (str / Queue): sqs queue name / boto queue object
    """
    queue = _get_queue_object(queue)

    # pull first batch of messages
    messages = queue.get_messages(QUEUE_BATCH_SIZE)

    # until queue is empty
    while len(messages) != 0:
        for message in messages:
            queue.delete_message(message)
        # pull the next batch
        messages = queue.get_messages(QUEUE_BATCH_SIZE)
