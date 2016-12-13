import functools
import logging

from aws_utils import NoSuchActivityError
import aws_utils
import boto3


logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def get_emr_cluster_state(emr_cluster_name, emr_list_clusters):
    """
    Looks for the state of the EMR cluster and if present returns it. Otherwise raises NoSuchActivityError exception.

    Args:
        cluster_name (str): name of the cluster

    Returns:
        str. The state of the cluster in the EMR::
            Possible return values: STARTING, BOOTSTRAPPING, RUNNING, WAITING, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
    """
    current_state = ''
    # get the status of the cluster
    for cluster in emr_list_clusters['Clusters']:
        if cluster['Name'] == emr_cluster_name:
            current_state = cluster['Status']['State']

    if current_state is not '':
        return current_state
    else:
        raise NoSuchActivityError('The cluster status is empty')


def get_boto3_emr_list_clusters(region):
    """
    Connects to AWS and gets a list of a EMR clusters on the region

        Args:
            region (str): AWS region
        Returns:
            list of clusters of a EMR
    """
    boto3client_emr = boto3.client('emr', region)
    return boto3client_emr.list_clusters()


def poll_for_cluster_state(emr_cluster_name, region, terminating_state, retry_interval_seconds,
                           max_retry_count):

    logger.info(
        'Terminating Cluster [%s] state : %s', emr_cluster_name, terminating_state)

    # combine callback and terminating_status
    def compare_state_from_list_clusters(emr_list_clusters):
        state = get_emr_cluster_state(emr_cluster_name, emr_list_clusters)
        logger.info('Cluster [%s] state: %s', emr_cluster_name, state)
        return str(terminating_state) == str(state)

    # Construct the status getter for the cluster
    func_callable = functools.partial(get_boto3_emr_list_clusters, region)
    result = aws_utils.utils.poller(
        func_callable, compare_state_from_list_clusters, retry_interval_seconds, max_retry_count)
    return result
