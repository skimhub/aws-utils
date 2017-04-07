import boto

# sqs
from boto.sqs.message import Message


def create_test_sqs_queue(queue_name):
    sqs_conn = boto.connect_sqs()
    sqs_conn.create_queue(queue_name)
    return sqs_conn.get_queue(queue_name)


def put_messages_in_sqs_queue(queue, messages):
    for message in messages:
        queue.write(Message(body=message))