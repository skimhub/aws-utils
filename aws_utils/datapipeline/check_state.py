import functools
import logging

from aws_utils import NoSuchActivityError
from aws_utils.utils import poller
import boto3


logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthStatusError(Exception):
    pass


def get_pipeline_state(pipeline_name, region, pipelines=None):
    """
    Connects to AWS and gets a status of a pipeline
    :param pipeline_name: (str) name of the Pipeline
    :param region: (str) AWS region
    :return: A dictionary containing both status and state of the pipeline : e.g {'status': 7, 'health': 2}
    Possible return STATUSES : ACTIVATING, CANCELED, CASCADE_FAILED, DEACTIVATING, FAILED, FINISHED, INACTIVE, PAUSED,
                             PENDING, RUNNING, SHUTTING_DOWN, SKIPPED, TIMEDOUT, VALIDATING, WAITING_FOR_RUNNER, WAITING_ON_DEPENDENCIES
    Possible return STATES : HEALTHY, ERROR
    """
    # create a client connection
    if not pipelines:
        boto3client_pipelines = boto3.client(
            'datapipeline', region)  # 'us-east-1')
        pipelines = boto3client_pipelines.list_pipelines()

    # get the status of the pipeline
    current_pipeline_state = ''
    # get the status of the pipeline
    for pipeline in pipelines['pipelineIdList']:
        if pipeline['name'] == pipeline_name:
            pipeline_settings = boto3client_pipelines.describe_pipelines(
                pipelineIds=[pipeline['id']])
            pipeline_fields = pipeline_settings[
                'pipelineDescriptionList'][0]['fields']
            current_pipeline_state = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] ==
                                              '@pipelineState'])
            current_health_status = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] ==
                                             '@healthStatus'])

    # for testing purposes
    # from random import sample
    # current_status_pipeline = sample(set(['1', '2', '3']), 1)[0]
    # current_health_pipeline = 'HEALTHY' #sample(set(['HEALTHY', 'ERROR']),
    # 1)[0]
    if current_pipeline_state is not '':
        return {'pipelineState': current_pipeline_state, 'healthStatus': current_health_status}
    else:
        raise NoSuchActivityError('The Pipeline state is empty')


def poll_for_state(pipeline_name, region, terminating_state, interval, max_retry):

    logger.info('Terminating state: %s', terminating_state)

    def check_state(state_and_health):
        logger.info('Actual state and health: %s', state_and_health)
        return str(terminating_state) == str(state_and_health['pipelineState'])

    status_getter = functools.partial(
        get_pipeline_state, pipeline_name, region)
    return poller(status_getter, check_state, interval, max_retry)
