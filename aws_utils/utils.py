try:
    import configparser 
except ImportError:
    import ConfigParser as configparser
import boto

def set_boto_retry_attemps_config_option(num_of_attempts):
    try:
        boto.config.add_section("Boto")
    except configparser.DuplicateSectionError:
        pass
    boto.config.set("Boto", "metadata_service_num_attempts", str(num_of_attempts))