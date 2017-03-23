import logging
import warnings

from aws_utils.s3 import s3_utils

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
