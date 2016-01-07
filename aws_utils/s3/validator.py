import re
import json

from os import path, makedirs


def validate_bucket_name(name):
    protocol = "s3n://"
    valid_name = re.sub(protocol, '', name)
    return valid_name.strip("/")


def validate_file_path(file_path):
    return file_path.strip("/")


def validate_directory_path(directory_path):
    return directory_path.strip("/") + "/"


def create_path(args, protocol='s3n'):
    valid_args = []
    for arg in args:
        valid_args.append(validate_directory_path(arg))
    my_path = ''.join(valid_args)
    s3_path = '{}://{}'.format(protocol, my_path)
    s3_valid_path = validate_file_path(s3_path)
    return s3_valid_path


def path_to_parquet_glob(path):
    """Spark 1.4 read.parquet selects all files in a dir so if we have a
    dir, add a parquet glob

    """
    if path.endswith("/"):
        path += "*.parquet"

    return path


def validate_local_save_directory(directory_path):
    return path.normpath(path.expanduser(path.expandvars(directory_path)))


def ensure_directories(directory_path):
    if not path.isdir(directory_path):
        makedirs(directory_path)

def load_json_or_drop(row):
    """Safely load row json, dropping unparseable rows.

    For use with .flatMap()
    """
    try:
        yield json.loads(row)
    except Exception as e:
        pass
