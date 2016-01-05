import logging

from boto.exception import S3ResponseError

from boto.s3.key import Key

logger = logging.getLogger(__name__)


def delete_path(bucket, path):
    """
    Attempts to delete all keys under a given path
    Will also try to remove spark meta data keys for the path
    """
    # TODO: see if using smart_open would be faster
    for k in bucket.list(path):
        k.delete()

    k = Key(bucket)
    k.key = path.strip("/") + "_$folder$"
    try:
        k.delete()
    except S3ResponseError:
        logger.debug("Could not delete metadata")
