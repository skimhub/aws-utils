import gzip
import logging
import boto
import boto3
import smart_open

try:
    from ConfigParser import DuplicateSectionError
except ImportError:
    from configparser import DuplicateSectionError
try:
    import cPickle as pickle
except ImportError:  # for python 3
    import pickle

from io import StringIO
from boto.s3.bucket import Bucket
from dateutil import rrule

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from retrying import retry
from urlparse import urlparse

logger = logging.getLogger(__name__)

CHUNK_SIZE = 5 * (1024 ** 2)
STD_DATE_PREFIX = 'year={:04}/month={:02}/day={:02}/'


class PklError(Exception):
    pass


def get_date_prefix(date, prefix_tmpl=STD_DATE_PREFIX):
    """Construct a standard S3 date prefix."""
    return prefix_tmpl.format(date.year, date.month, date.day)


def get_date_paths(from_date, to_date, prefix_tmpl=STD_DATE_PREFIX):
    """Return a list of date prefixes for each day between from and to.

    Inclusive of each bounding date.
    """
    return [get_date_prefix(d, prefix_tmpl)
            for d in _iterate_days(from_date, to_date)]


def get_filesize(bucket, path):
    return bucket.lookup(path).size


def save_to_s3(bucket, path, data, compress=False):
    """Takes a data string and saves it to provided path in the provided bucket

    Args:
        compress (bool): If True the data is gzip compressed before saving to s3.
        data (str): String of data we wish to pass
        path (str): Path within the bucket to save the file to, should not contain the bucket name
        bucket (str or Bucket): Bucket to add the file to, if a string is provided, we try and open an amazon bucket with that name.
    """
    if isinstance(bucket, str):
        bucket = boto.connect_s3(host='s3.amazonaws.com').get_bucket(bucket)

    key = Key(bucket)
    key.key = path
    logger.debug("Uploading to %s", key.key)

    if compress:
        mock_file = StringIO()
        gzip_obj = gzip.GzipFile(filename='gzipped_file', mode='wb', fileobj=mock_file)
        gzip_obj.write(data)
        gzip_obj.close()
        data = mock_file.getvalue()

    key.set_contents_from_string(data)


def get_from_s3(bucket, path):
    k = Key(bucket)
    k.key = path
    return k.get_contents_as_string()


def delete_path(bucket, path):
    """
    Attempts to delete all keys under a given path
    Will also try to remove spark meta data keys for the path
    """
    for k in bucket.list(path):
        k.delete()
    k = Key(bucket)
    k.key = path.strip("/") + "_$folder$"
    k.delete()


def verify_s3_pkl(bucket, path, data):
    """
    Grabs a pkl file from S3 and comparse to local pkl.
    Raises a PklError if it fails
    """
    pkl = get_from_s3(bucket, path)

    if pickle.loads(pkl) != data:
        raise PklError


def file_is_empty(bucket, path):
    key = bucket.lookup(path)
    if key.size == 0:
        return True
    else:
        return False


def setup_boto():
    try:
        boto.config.add_section("Boto")
    except DuplicateSectionError:
        pass
    boto.config.set("Boto", "metadata_service_num_attempts", "20")


def setup_bucket(bucket_name):
    setup_boto()
    s3_conn = boto.connect_s3(host='s3.amazonaws.com', calling_format=OrdinaryCallingFormat())
    bucket = s3_conn.get_bucket(bucket_name)
    return bucket


def get_bucket(bucket):
    if isinstance(bucket, Bucket):
        return bucket
    if isinstance(bucket, str):
        return setup_bucket(bucket)
    else:
        raise TypeError("Expected bucket to be Bucket or str was %s " % type(bucket))


def _iterate_days(from_date, to_date):
    """Yield datetime instances for the start of each day between from and to.

    Inclusive of both bounding days.

    For example, this will yield all days in April (1 to 30)
    >> _iterate_days(dt(2013, 4, 1), dt(2013, 4, 30))
    """
    if from_date > to_date:
        raise ValueError('from_date %s is > to_date %s', from_date, to_date)
    return rrule.rrule(rrule.DAILY, dtstart=from_date, until=to_date)


# FIXME: this is a bit too lax, and will regularly return $folder$
# and _SUCCESS files which we do not want, refactor to use key.size
# to filter these out
def retrieve_segments_list(bucket, segment_type_path):
    keys = bucket.list(segment_type_path)
    folders = []
    for key in keys:
        raw_key = key.name.split(segment_type_path, 1)[1]
        if raw_key.split("/")[0] != "":
            folders.append(raw_key.split("/")[0])
    return set(folders)


def get_md5(bucket, path):
    """ Returns the md5 of a key using boto bucket and path """
    return bucket.get_key(path).etag[1:-1]


def get_segment_filepaths(segment_dest_dir, s3_bucket_conn,
                          output_bucket):
    """ Returns a list of all segment files within a S3 directory
        These are boto paths, so they are the paths relative to a bucket
        e.g production/output/segments/bluekai_segments/part-00001
    """
    segment_dir = (segment_dest_dir
                   .split('s3n://{}/'.format(output_bucket))[-1])
    segment_dir_contents = list(retrieve_segments_list(
        s3_bucket_conn, segment_dir))

    # Remove 0 byte non-segment files
    segment_filenames = [fp for fp
                         in segment_dir_contents
                         if not ((fp.endswith('$folder$')) or (fp == '_SUCCESS'))]

    return ['{}{}'.format(segment_dir, filename)
            for filename
            in segment_filenames]


def merge_part_files(input_bucket, input_prefix,
                     output_bucket, output_key, list_key=None,
                     sort_key=None):
    """

    Args:
        input_bucket: a boto bucket object
        input_prefix: a string representing the prefix to scan for keys to
                         merge
        output_bucket: a boto bucket object
        output_key: the key to store the merged file
        sort_key: function to sort the keys with
                     If for example you want have header as first file sort_fn would be
                     lambda x: 'header' in x.key
    """
    if list_key is None:
        list_key = lambda k: k.key.endswith('.gz')

    key_list = [k for k in input_bucket.list(input_prefix) if list_key(k)]
    if sort_key is not None:
        key_list = sorted(key_list, key=sort_key, reverse=True)

    out_path = 's3://{}/{}'.format(output_bucket.name, output_key)
    chunk = StringIO()

    def _inc(out, size):
        out.total_size += size
        out.parts += 1

    with smart_open.smart_open(out_path, mode='wb', min_part_size=CHUNK_SIZE) as out:
        for parts in partition_list(key_list, threshold=CHUNK_SIZE):
            if isinstance(parts, list):
                # In this case it means that more parts were grouped
                # together because to reach the desired minumum size
                chunk = StringIO()
                for part in parts:
                    chunk.write(part.get_contents_as_string())
                    _inc(out, part.size)

                chunk.seek(0)
                out.mp.upload_part_from_file(chunk, out.parts)
            else:
                # in this case the part is big enough to be uploaded
                # straight away
                _inc(out, parts.size)
                out.mp.copy_part_from_key(input_bucket.name, parts.key, out.parts)


def partition_list(lis, threshold):
    """Partition a list in sublists, clustering together elements and
    maintaining the order, making sure that the total size for each
    cluster is always bigger than the given threshold.

    If a small element ends up last it should go by itself
    """
    chunk, partial = [], 0
    idx = 0
    while idx < len(lis):
        if lis[idx].size < threshold:
            while partial < threshold and idx < len(lis):
                chunk.append(lis[idx])
                partial += lis[idx].size
                idx += 1

            yield chunk
            chunk, partial = [], 0
        else:
            yield lis[idx]
            idx += 1


def is_pickle_load_exception(exception):
    return isinstance(exception, (EOFError, ValueError))


@retry(retry_on_exception=is_pickle_load_exception,
       stop_max_attempt_number=3,
       wait_fixed=10000)
def load_pickle_from_s3(bucket, path):
    """Loads pickle files from S3, retries on the provided exception.

    We retry on EOF and Value errors as these occasionally occur in production
    and disappear upon re-run, leading us to believe they are S3 related
    """
    pkl = get_from_s3(bucket, path)
    return pickle.loads(pkl)


def get_bucket_and_path_from_uri(path):
    """Returns a list containing S3 bucket and path from a URI.

    :param path: (str) a S3 uri -> 's3n://bucket/path-to/something'
    :return: A tuple containing the S3 bucket and path -> (bucket, path-to/something)
    """
    parsed_url = urlparse(path)
    return parsed_url.netloc, parsed_url.path.lstrip('/')


def path_exists(bucket, path):
    """Check if a given path exists

    Args:

    """
    return bool(bucket.get_key(path, validate=True))


def file_size(bucket, file_path):
    """Get the size of a file. Raises an exception if the file is not found

    Args:
        bucket (boto.s3.bucket.Bucket):
        file_path (str):

    Returns:
        int: file size in bytes
    """
    key = bucket.get_key(file_path)
    if not key:
        raise IOError('file %s does not exist in bucket %s' % (file_path, bucket))

    return key.size


def path_contains_data(bucket, root_path, min_file_size=0, file_extension=None):
    """Checks if there are any files under this path that contain files of size greater than 0

    Args:
        bucket (boto.s3.bucket.Bucket): bucket within which to check.
        root_path (str): Should be the path relative to the bucket, does not support wildcards.
        file_extension (str): optional filter for file type, e.g. setting this to '.gz' will only be True if there
            are .gz files with in the path.
        min_file_size (int): sometimes we may have empty gz files so set a minimum file size for returning True.
            Files of exactly this size will be excluded.
    Returns:
        bool
    """
    for key in bucket.list(root_path):
        if file_extension and not key.name.endswith(file_extension):
            continue
        if key.size > min_file_size:
            return True

    return False


def _delete_1000_s3_files(bucket, directory):
    conn = boto3.resource('s3')
    objects_to_delete = conn.meta.client.list_objects(Bucket=bucket, Prefix=directory)
    delete_keys = {'Objects': [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]}

    if delete_keys['Objects']:
        conn.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)
        return True
    return False


def delete_contents_of_s3_directory(directory, bucket_name=None):
    """Be very careful, this method will delete everything under a given path, use with caution

    Args:
        directory (str): If bucket is not set then this path must contain the bucket directory, otherwise the bucket can be
        bucket_name (str): If set this is the bucket we delete from and the directory is the path within that
    """
    if bucket_name is None:
        bucket_name, directory = get_bucket_and_path_from_uri(directory)

    if isinstance(bucket_name, Bucket):
        bucket_name = bucket_name.name

    logger.info("deleting contents of s3 bucket %s directory %s", bucket_name, directory)

    assert len(directory) > 10, "just in case don't want to delete the root of the bucket..."

    # Deleting keys from s3 appears to max out at 1000 entries so we need to do this multiple times until the directory is clear.
    while _delete_1000_s3_files(bucket_name, directory):
        pass


def get_contents_of_directory(directory, bucket=None):
    """List all the files in a given s3 directory

    Args:
        directory (str): If bucket is not set then this path must contain the bucket directory, otherwise the bucket can be
        bucket (str or Bucket): If set this is the bucket we delete from and the directory is the path within that

    Returns:
        list of str - the names of all the files
    """
    if bucket is None:
        bucket, directory = get_bucket_and_path_from_uri(directory)
    bucket = get_bucket(bucket)

    return [x.key for x in bucket.list(prefix=directory)]
