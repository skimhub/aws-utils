import validator
import logging

logger = logging.getLogger(__name__)


def get_events_path(bucket, path, *args):
    return validator.create_path([bucket, path] + list(args), protocol='s3n')


def save_parquet_data_to_s3(bucket, path, date_path, data):
    output_path = "s3n://{}/{}{}profiles.parquet".format(
        bucket, path, date_path)
    logger.info("Saving to %s", output_path)
    data.write.parquet(output_path)
    logger.info("Finished: %s", output_path)
