import boto
from boto.s3.key import Key

from moto import mock_s3

from aws_utils.s3 import paths


@mock_s3
def test_delete_path():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('mybucket')

    k = Key(bucket)
    k.key = 'some-path/file1'
    k.set_contents_from_string('Some content')

    paths.delete_path(bucket, 'some-path')
