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
        boto3client_pipelines = boto3.client('datapipeline', region)  # 'us-east-1')
        pipelines = boto3client_pipelines.list_pipelines()

    # get the status of the pipeline
    current_status_pipeline = ''
    # get the status of the pipeline
    for pipeline in pipelines['pipelineIdList']:
        if pipeline['name'] == pipeline_name:
            pipeline_settings = boto3client_pipelines.describe_pipelines(pipelineIds=[pipeline['id']])
            pipeline_fields = pipeline_settings['pipelineDescriptionList'][0]['fields']
            current_status_pipeline = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] == 
                               '@pipelineState'])
            current_health_pipeline = ''.join([item['stringValue'] for item in pipeline_fields if item['key'] == 
                               '@healthStatus'])

    # for testing purposes
    # from random import sample
    # current_status_pipeline = sample(set(['1', '2', '3']), 1)[0]
    # current_health_pipeline = 'HEALTHY' #sample(set(['HEALTHY', 'ERROR']), 1)[0]
    if current_status_pipeline is not '':
        return {'status':current_status_pipeline, 'health':current_health_pipeline }
    else:
        raise NoSuchActivityError('The Pipeline status is empty')


def poll_for_status(emr_name, region, terminating_status,
                    interval, callback=None):

    logger.info('Terminating status : %s', terminating_status)

    # combine callback and terminating_status
    def check_status(status):
        logger.info('Actual status : %s', status)
        if str(terminating_status['health']) == str(status['health']):
            raise HealthStatusError('The health status of the pipeline is : %s' % status['health'])
        else:
            if str(terminating_status['status']) == str(status['status']):
                pass
            if str(terminating_status['status']) != str(status['status']):
                # status = status['status']
                return check_status

    # Construct the status getter for the cluster
    status_getter = functools.partial(get_pipeline_state, emr_name, region)
    return poller(status_getter, interval, callback=check_status)
