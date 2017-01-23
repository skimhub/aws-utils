import gzip
import logging
import warnings
from io import StringIO

import boto
from boto.exception import S3ResponseError

from boto.s3.key import Key

from aws_utils.s3 import s3_utils
from aws_utils.s3.s3_utils import get_bucket

logger = logging.getLogger(__name__)


def delete_path(bucket, path):
    """
    Deprecated Please use same method in s3_utils instead
    """
    warnings.warn(
        "Please use same method in s3_utils instead",
        DeprecationWarning
    )
    return s3_utils.delete_path(bucket, path)


def path_exists(bucket, path):
    """
    Deprecated Please use same method in s3_utils instead
    """
    warnings.warn(
        "Please use same method in s3_utils instead",
        DeprecationWarning
    )
    return s3_utils.path_exists(bucket, path)


def save_to_s3(bucket, path, data, compress=False):
    """
    Deprecated Please use same method in s3_utils instead
    """
    warnings.warn(
        "Please use same method in s3_utils instead",
        DeprecationWarning
    )

    return s3_utils.save_to_s3(bucket, path, data, compress=compress)
