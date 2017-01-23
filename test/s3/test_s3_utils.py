import uuid
from collections import namedtuple

import boto

try:
    import cPickle as pickle
except ImportError:
    import pickle

import moto
import os
import pytest
from pytest import raises

from aws_utils.s3.paths import save_to_s3
from aws_utils.s3.s3_utils import merge_part_files, get_from_s3, partition_list, load_pickle_from_s3, file_size, path_contains_data, \
    setup_bucket, delete_contents_of_s3_directory, get_contents_of_directory

TEST_BUCKET = 'audience-data-store-qa'
TEST_INP_PREFIX = 'integration-tests/s3_utils_input'
TEST_OUT_PREFIX = 'integration-tests/s3_utils_output'

HEADER_STRING = 'HEADER' * 10
LONG_STRING = 'Hello' * (2 ** 20) + 'Extra'
SMALL_STRING = 'Small' * 100
FILES_CONTENT = {
    'header.gz': HEADER_STRING,
    'part1.gz': LONG_STRING,
    'part2.gz': LONG_STRING,
    'part3.gz': SMALL_STRING,
    'other': LONG_STRING,
}
OUTPUT = '%s/%s' % (TEST_OUT_PREFIX, 'merged.gz')

Key = namedtuple('Key', ['name', 'size'])


@pytest.fixture(scope='function')
def bucket(request):
    conn = boto.connect_s3(host='s3.amazonaws.com')
    bucket = conn.get_bucket(TEST_BUCKET)

    for fn, content in FILES_CONTENT.items():
        key_path = '%s/%s' % (TEST_INP_PREFIX, fn)
        save_to_s3(bucket, key_path, content)

    def teardown():
        for fn in FILES_CONTENT:
            bucket.delete_key('%s/%s' % (TEST_INP_PREFIX, fn))

        bucket.delete_key(OUTPUT)

    request.addfinalizer(teardown)
    return bucket


@pytest.mark.slow
def test_merge_files_ordering(bucket):
    desired_content = (FILES_CONTENT['header.gz'] + FILES_CONTENT['part1.gz'] + \
                       FILES_CONTENT['part2.gz'] + FILES_CONTENT['part3.gz'])

    merge_part_files(bucket, TEST_INP_PREFIX,
                     bucket, OUTPUT,
                     sort_key=lambda obj: 'header.gz' in obj.key)

    merged = get_from_s3(bucket, OUTPUT)
    assert len(merged) == len(desired_content), "Content length not matching"
    assert merged == desired_content, "Content not matching"


@pytest.mark.parametrize(('input', 'expected'), [
    ([Key('a', 10), Key('b', 20), Key('c', 50), Key('d', 60)],
     [[Key('a', 10), Key('b', 20), Key('c', 50)], Key('d', 60)]),
    ([Key('a', 40), Key('b', 40), Key('c', 20)],
     # in this case the second partition is not greater than threshold
     # but there is no alternative without a full look-ahead
     [[Key('a', 40), Key('b', 40)], [Key('c', 20)]]),
])
def test_partitioned_list(input, expected):
    assert list(partition_list(input, threshold=50)) == expected


@moto.mock_s3()
@pytest.mark.skip(reason="requires local file, in the future bring this file into the project")
def test_success_load_pickle_from_s3():
    conn = boto.connect_s3()
    conn.create_bucket(TEST_BUCKET)

    if os.path.exists('/app/data'):
        prefix = 'file:///app/'
    else:
        prefix = ''

    bucket = conn.get_bucket(TEST_BUCKET)
    local_pkl_path = prefix + 'test/data/pickles/brands_classifier_merc/index_to_word.pkl'
    k = boto.s3.key.Key(bucket)
    k.key = 'index_to_word.pkl'
    k.set_contents_from_filename(local_pkl_path)

    with open(local_pkl_path, 'r') as pkl:
        expected = pickle.loads(pkl.read())
        assert load_pickle_from_s3(bucket, 'index_to_word.pkl') == expected


def _create_file(file_path, bucket_name=TEST_BUCKET):
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    key = bucket.new_key(file_path)
    key.set_contents_from_string('a')

    return bucket, key


@pytest.mark.parametrize(('file_content', 'expected'), [
    ('', 0),
    ('1', 1)])
def test_file_size(file_content, expected):
    file_path = str(uuid.uuid1())
    key = bucket = None

    try:
        # given
        bucket, key = _create_file(file_path)
        key.set_contents_from_string(file_content)

        # when
        size = file_size(bucket, file_path)

        # then
        assert size == expected
    finally:
        if bucket and key:
            bucket.delete_key(key)


def test_file_size_throws_exception_for_non_existant_file():
    conn = boto.connect_s3()
    bucket = conn.get_bucket(TEST_BUCKET)

    # when
    with raises(IOError, message="Expecting Exception"):
        file_size(bucket, 'file_that_does_not_exist')


@pytest.mark.parametrize(('file_content', 'expected', 'minimum_file_size'), [
    ('', False, 0),
    ('1', True, 0),
    ('1', False, 20)])
def test_path_contains_data(file_content, expected, minimum_file_size):
    file_path = str(uuid.uuid1())
    key = bucket = None

    try:
        # given
        bucket, key = _create_file(file_path)
        key.set_contents_from_string(file_content)

        # when
        contains_data = path_contains_data(bucket, file_path, min_file_size=minimum_file_size)

        # then
        assert contains_data == expected
    finally:
        if bucket and key:
            bucket.delete_key(key)


def test_path_contains_data_with_extension_filter():
    file_path = str(uuid.uuid1()) + '.exc'
    key = bucket = None

    try:
        # given
        bucket, key = _create_file(file_path)
        key.set_contents_from_string('some data')

        # when
        contains_data = path_contains_data(bucket, file_path, file_extension='.filter')

        # then
        assert not contains_data
    finally:
        if bucket and key:
            bucket.delete_key(key)


def test_setup_bucket_with_period_in_name():
    """test for issue AS-426 opening a bucket with a '.' in the path was throwing exception"""
    setup_bucket('data.api.qa.test')


@moto.mock_s3
def test_delete_contents_of_s3_directory_should_fail_on_root():
    with pytest.raises(Exception):
        delete_contents_of_s3_directory('', bucket_name='test_bucket')


@moto.mock_s3
def test_delete_contents_of_s3_directory_should_fail_on_root():
    test_bucket = 'test_bucket'
    root_path = 'my/test/root/path/is/here'

    boto.connect_s3().create_bucket(test_bucket)

    for i in range(2000):
        _create_file(root_path + '/' + str(uuid.uuid4()) + '/' + str(uuid.uuid4()), bucket_name=test_bucket)

    assert len(get_contents_of_directory(root_path, bucket=test_bucket)) == 2000

    delete_contents_of_s3_directory(root_path, bucket_name=test_bucket)

    assert len(get_contents_of_directory(root_path, bucket=test_bucket)) == 0
