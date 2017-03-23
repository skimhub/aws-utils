import uuid, os
from collections import namedtuple
import boto, boto3, moto
import pytest
import re
from pytest import raises

try:
    import cPickle as pickle
except ImportError:
    import pickle

from aws_utils.s3.s3_utils import merge_part_files, get_from_s3, partition_list, load_pickle_from_s3, file_size, \
    path_contains_data, save_to_s3, \
    setup_bucket, delete_contents_of_s3_directory, get_contents_of_directory, rename_keys_on_s3, rename_s3_key, \
    fetch_s3_filepaths_to_local, get_s3_filename, get_s3_keys_by_regex

TEST_BUCKET = 'audience-data-store-qa'
TEST_INP_PREFIX = 'integration-tests/s3_utils_input'
TEST_OUT_PREFIX = 'integration-tests/s3_utils_output'
TEST_REGION = 'us-east-1'
TEST_S3_KEYS = {TEST_INP_PREFIX + '/app_nexus/',
                TEST_INP_PREFIX + '/app_nexus/guid_map',
                TEST_INP_PREFIX + '/app_nexus/guid_map/compressed_file.gz',
                TEST_INP_PREFIX + '/app_nexus/2016-01-10T12:22/guid_map/',
                TEST_INP_PREFIX + '/app_nexus/2016-01-10T12:22/guid_map/compressed_file.gz'}

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
TEST_FILE_CONTENT = 'a'

Key = namedtuple('Key', ['name', 'size'])


@pytest.fixture(scope='function')
def bucket(request):
    conn = boto.connect_s3(host='s3.amazonaws.com')
    conn.create_bucket(TEST_BUCKET)
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


@pytest.mark.skipif('.tox' in os.environ['PATH'], reason='tox run and smart-open incompatibility')
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
def test_success_load_pickle_from_s3():
    conn = boto.connect_s3()
    conn.create_bucket(TEST_BUCKET)
    bucket = conn.get_bucket(TEST_BUCKET)

    string_to_assert = 'test pickle content'
    mocked_pkl = pickle.dumps(string_to_assert)

    k = bucket.new_key('index_to_word.pkl')
    k.set_contents_from_string(mocked_pkl)

    assert load_pickle_from_s3(bucket, 'index_to_word.pkl') == string_to_assert


def _create_file(file_path, bucket_name=TEST_BUCKET):
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    key = bucket.new_key(file_path)
    key.set_contents_from_string(TEST_FILE_CONTENT)

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


@moto.mock_s3()
def test_boto3_rename_keys_on_s3(boto3_client):
    mock_bucket = TEST_BUCKET + 'test'

    # given the filter
    def mock_filter(string): return string.endswith(('guid_map', 'guid_map/'))

    # given the function that modifies the prefix names
    def mock_modifier(string): return string.replace(':', '')

    # generate mocked keys on s3
    conn = boto3.resource('s3', region_name=TEST_REGION)
    conn.create_bucket(Bucket=mock_bucket)
    for key in TEST_S3_KEYS:
        conn.Object(mock_bucket, key).put('test string\n ala-bala')
    assert len([conn.Object(mock_bucket, key) for key in TEST_S3_KEYS]) == len(TEST_S3_KEYS)

    # when
    rename_keys_on_s3(mock_bucket, TEST_REGION, TEST_INP_PREFIX,
                      prefix_modification_func=mock_modifier,
                      filter_keys_func=mock_filter)
    excpected_keys = (TEST_S3_KEYS | {TEST_INP_PREFIX + '/app_nexus/2016-01-10T1222/guid_map/compressed_file.gz'}) \
                     - {TEST_INP_PREFIX + '/app_nexus/2016-01-10T12:22/guid_map/compressed_file.gz'}
    new_keys_on_s3 = {key['Key'] for key in
                      boto3_client('s3', TEST_REGION).list_objects_v2(Bucket=mock_bucket,
                                                                      Prefix=TEST_INP_PREFIX)['Contents']}
    assert new_keys_on_s3 == excpected_keys

    # when no filter
    rename_keys_on_s3(mock_bucket, TEST_REGION, TEST_INP_PREFIX,
                      prefix_modification_func=mock_modifier,
                      filter_keys_func=None)
    new_keys_on_s3 = {key['Key'] for key in
                      boto3_client('s3', TEST_REGION).list_objects_v2(Bucket=mock_bucket,
                                                                      Prefix=TEST_INP_PREFIX)['Contents']}
    assert len(new_keys_on_s3) == len(TEST_S3_KEYS)

    # when no modifier:
    with pytest.raises(Exception):
        assert rename_keys_on_s3(mock_bucket, TEST_REGION, TEST_INP_PREFIX,
                                 prefix_modification_func=None,
                                 filter_keys_func=mock_filter)


@moto.mock_s3()
def test_fetch_s3_filepaths_to_local(tmpdir):
    test_bucket = TEST_BUCKET + 'test'
    boto.connect_s3().create_bucket(test_bucket)
    s3_directory = 'my/test/root/directory/'
    local_directory = tmpdir.dirname + '/'

    s3_keys = []
    for i in range(10):
        _, key = _create_file(s3_directory + str(uuid.uuid4()), bucket_name=test_bucket)
        s3_keys.append(key)

    local_filepaths = fetch_s3_filepaths_to_local(s3_keys, local_directory)
    assert len(local_filepaths) == 10
    print tmpdir.dirname
    for local_file in local_filepaths:
        with open(local_file) as f:
            assert f.read() == TEST_FILE_CONTENT


@moto.mock_s3()
@pytest.mark.parametrize(('s3_file_path', 'expected'), [
    ('my/test/file.txt', 'file.txt'),
    ('my/test', 'test'),
    ('my', 'my')])
def test_get_s3_filename(s3_file_path, expected):
    test_bucket = TEST_BUCKET + 'test'
    boto.connect_s3().create_bucket(test_bucket)
    _, key = _create_file(s3_file_path, bucket_name=test_bucket)
    s3_filename = get_s3_filename(s3_file_path)
    assert s3_filename == expected


@moto.mock_s3()
@pytest.mark.parametrize(('s3_file_path'), [
    ('my/test/'),
    ('my/')])
def test_get_s3_filename_error(s3_file_path):
    test_bucket = TEST_BUCKET + 'test'
    boto.connect_s3().create_bucket(test_bucket)
    _, key = _create_file(s3_file_path, bucket_name=test_bucket)
    with pytest.raises(ValueError):
        get_s3_filename(s3_file_path)


@moto.mock_s3()
def test_get_s3_keys_by_regex():
    test_bucket = TEST_BUCKET + 'test'
    conn = boto.connect_s3()
    conn.create_bucket(test_bucket)
    bucket = conn.get_bucket(test_bucket)

    s3_directory = 'my/test/root/directory/'
    for i in range(5):
        _, key = _create_file(s3_directory + 'segment_{}'.format(str(i)), bucket_name=test_bucket)
    _create_file(s3_directory + '_SUCCESS_', bucket_name=test_bucket)

    pattern = re.compile('segment_\d+')
    x = get_s3_keys_by_regex(bucket, s3_directory, pattern)
    assert len(x) == 5


@moto.mock_s3()
def test_get_s3_keys_by_regex_no_files():
    test_bucket = TEST_BUCKET + 'test'
    conn = boto.connect_s3()
    conn.create_bucket(test_bucket)
    bucket = conn.get_bucket(test_bucket)

    s3_directory = 'my/test/root/directory/'
    _create_file(s3_directory + '_SUCCESS_', bucket_name=test_bucket)

    pattern = re.compile('segment_\d+')
    with pytest.raises(ValueError):
        get_s3_keys_by_regex(bucket, s3_directory, pattern)



