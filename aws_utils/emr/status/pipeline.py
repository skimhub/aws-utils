import boto3
import logging
import functools
from aws_utils.emr.status import poller

logging.basicConfig(format='%(levelname)s:\t%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class NoSuchActivityError(Exception):
    """
    A custom exception
    """


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
        boto3client_pipelines = boto3.client('datapipeline', region) #'us-east-1')
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
        raise NoSuchActivityError('The Pipeline status is empty')

def poll_for_status(emr_name, region, terminating_status,
                    interval, callback=None):

    logger.info('Terminating status : %s', terminating_status)

    # combine callback and terminating_status
    def check_status(status):
        logger.info('Actual status : %s', status)
        if str(terminating_status) == str(status):
            pass
        else:
            return check_status

    # Construct the status getter for the cluster
    status_getter = functools.partial(get_pipeline_state, emr_name, region)
    return poller(status_getter, interval, callback=check_status)