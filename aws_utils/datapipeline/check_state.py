import functools
import logging

from aws_utils import NoSuchActivityError
from aws_utils.utils import poller
import boto3


logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

EXIT_STATUS = 'ERROR'

class HealthStatusError(Exception):
    pass


def get_boto3_emr_pipeline_settings(pipeline_name, region):
    """
    Connects to AWS and gets a list of a EMR clusters on the region

        Args:
            region (str): AWS region
        Returns:
            list of clusters of a EMR
    """
    boto3client_pipelines = boto3.client('datapipeline', region)  # 'us-east-1')
    pipelines = boto3client_pipelines.list_pipelines()

    for pipeline in pipelines['pipelineIdList']:
        if pipeline['name'] == pipeline_name:
            return boto3client_pipelines.describe_pipelines(pipelineIds=[pipeline['id']])


def get_pipeline_state(pipeline_settings):
    """
    Connects to AWS and gets a status of a pipeline
    :param pipeline_name: (str) name of the Pipeline
    :param region: (str) AWS region
    :return: A dictionary containing both status and state of the pipeline : e.g {'status': 7, 'health': 2}
    Possible return STATUSES : ACTIVATING, CANCELED, CASCADE_FAILED, DEACTIVATING, FAILED, FINISHED, INACTIVE, PAUSED,
                             PENDING, RUNNING, SHUTTING_DOWN, SKIPPED, TIMEDOUT, VALIDATING, WAITING_FOR_RUNNER, WAITING_ON_DEPENDENCIES
    Possible return STATES : HEALTHY, ERROR
    """
    pipeline_fields = pipeline_settings[
        'pipelineDescriptionList'][0]['fields']
    current_pipeline_state = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] ==
                                      '@pipelineState'])
    current_health_status = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] == '@healthStatus'])

    if current_pipeline_state is not '':
        return {'pipelineState': current_pipeline_state, 'healthStatus': current_health_status}
    else:
        raise NoSuchActivityError('The Pipeline state is empty')


def poll_pipeline_for_state(pipeline_name, region, terminating_state, interval, max_retry):

    logger.info('Terminating state: %s', terminating_state)

    def compare_state_from_list_pipelines(pipeline_settings):
        status = get_pipeline_state(pipeline_settings)
        logger.info('Cluster [%s] state: %s', pipeline_name, status)
        return str(terminating_state['pipelineState']) == str(status['pipelineState']) \
               or str(status['healthStatus']) == "ERROR"

    func_callable = functools.partial(get_boto3_emr_pipeline_settings, pipeline_name, region)
    return poller(func_callable, compare_state_from_list_pipelines, interval, max_retry)