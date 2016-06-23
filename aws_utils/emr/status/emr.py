from aws_utils.emr.status import NoSuchActivityError, poller
import boto3
import functools

import logging
logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def get_emr_state(emr_name, region, emrs=None):
    """
    Connects to AWS and gets a status of a EMR cluster
    :param emr_name: (str) name of the cluster
    :param region: (str) AWS region
    :return: current status of a EMR
    Possible return values : STARTING, BOOTSTRAPPING, RUNNING, WAITING, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
    """
    # create a client connection
    if not emrs:
        # boto3client_emr = boto_connect('emr', region)
        boto3client_emr = boto3.client('emr', region)
        emrs = boto3client_emr.list_clusters()

    current_status_sample_emr = ''
    # get the status of the cluster
    for emr in emrs['Clusters']:
        if emr['Name'] == emr_name:
            current_status_sample_emr = emr['Status']['State']
    # for testing purposes :
    # from random import choice, sample
    # current_status_sample_emr = sample(set([1, 2, 3, 4, 5]), 1)[0]
    # emr.poll_for_status("emrname", REGION, 3, 10)
    if current_status_sample_emr is not '':
        return current_status_sample_emr
    else:
        raise NoSuchActivityError('The cluster status is empty')


def poll_for_status(emr_name, region, terminating_status,
                    interval, callback=None):

    logger.info('Terminating status : %s', terminating_status)

    #combine callback and terminating_status
    def check_status(status):
        logger.info('Actual status : %s', status)
        if str(terminating_status) == str(status):
            pass
        else:
            return check_status

    # Construct the status getter for the cluster
    status_getter = functools.partial(get_emr_state, emr_name, region)
    return poller(status_getter, interval, callback=check_status)
