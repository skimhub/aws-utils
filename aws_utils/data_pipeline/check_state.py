import functools
import logging

from aws_utils import NoSuchActivityError
from aws_utils.utils import poller
import boto3


logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def get_pipeline_state(pipeline_name, region, pipelines=None):
    """
    Connects to AWS and gets a status of a pipeline
    :param pipeline_name: (str) name of the Pipeline
    :param region: (str) AWS region
    :return: current status of a Pipeline
    Possible return values : ACTIVATING, CANCELED, CASCADE_FAILED, DEACTIVATING, FAILED, FINISHED, INACTIVE, PAUSED,
                             PENDING, RUNNING, SHUTTING_DOWN, SKIPPED, TIMEDOUT, VALIDATING, WAITING_FOR_RUNNER, WAITING_ON_DEPENDENCIES
    """
    # create a client connection
    if not pipelines:
        boto3client_pipelines = boto3.client('datapipeline', region)  # 'us-east-1')
        pipelines = boto3client_pipelines.list_pipelines()

    current_status_pipeline = ''
    # get the status of the pipeline
    for pipeline in pipelines['pipelineIdList']:
        if pipeline['name'] == pipeline_name:
            pipeline_settings = boto3client_pipelines.describe_pipelines(pipelineIds=[pipeline['id']])
            pipeline_fields = pipeline_settings['pipelineDescriptionList'][0]['fields']
            pipeline_status = [item['stringValue'] for item in pipeline_fields if item['key'] == '@pipelineState']
            current_status_pipeline = ''.join(pipeline_status)
    if current_status_pipeline is not '':
        return current_status_pipeline
    else:
        raise NoSuchActivityError('The Pipeline state is empty')

def poll_for_state(emr_name, region, terminating_state,
                    interval, callback=None):

    logger.info('Terminating state : %s', terminating_state)

    # combine callback and terminating_status
    def check_state(state):
        logger.info('Actual state : %s', state)
        if str(terminating_state) == str(state):
            pass
        else:
            return check_state

    # Construct the status getter for the cluster
    status_getter = functools.partial(get_pipeline_state, emr_name, region)
    return poller(status_getter, callback_end_polling=check_state, interval)
