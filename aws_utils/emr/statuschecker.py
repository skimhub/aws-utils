import time
import boto3
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NoSuchActivityError(Exception):
    """
    A custom exception
    """


def get_pipeline_state(pipeline_name, region):
    """
    Connects to AWS and gets a status of a pipeline
    :param pipeline_name: (str) name of the Pipeline
    :param region: (str) AWS region
    :return: current status of a Pipeline
    Possible return values : ACTIVATING, CANCELED, CASCADE_FAILED, DEACTIVATING, FAILED, FINISHED, INACTIVE, PAUSED,
                             PENDING, RUNNING, SHUTTING_DOWN, SKIPPED, TIMEDOUT, VALIDATING, WAITING_FOR_RUNNER, WAITING_ON_DEPENDENCIES
    """
    # create a client connection
    boto3client_pipelines = boto3.client('datapipeline', region) #'us-east-1')
    pipelines = boto3client_pipelines.list_pipelines()

    current_status_pipeline = ''
    # get the status of the pipeline
    for pipeline in pipelines['pipelineIdList']:
        if pipeline['name'] == pipeline_name:
            pipeline_settings = boto3client_pipelines.describe_pipelines(pipelineIds=[pipeline['id']])
            pipeline_fields = pipeline_settings['pipelineDescriptionList'][0]['fields']
            pipeline_status = [item['stringValue'] for item in pipeline_fields if item['key'] == '@pipelineState']
            if pipeline_status is not '':
                return ''.join(pipeline_status)
            else:
                raise NoSuchActivityError('\tThe EMR status is empty')


def get_emr_state(emr_name, region):
    """
    Connects to AWS and gets a status of a EMR cluster
    :param emr_name: (str) name of the cluster
    :param region: (str) AWS region
    :return: current status of a EMR
    Possible return values : STARTING, BOOTSTRAPPING, RUNNING, WAITING, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
    """
    # create a client connection
    boto3client_emr = boto3.client('emr', region)
    emrs = boto3client_emr.list_clusters()

    current_status_sample_emr = ''
    # get the status of the cluster
    for emr in emrs['Clusters']:
        if emr['Name'] == emr_name:
            current_status_sample_emr = emr['Status']['State']

    if current_status_sample_emr is not '':
        return current_status_sample_emr
    else:
        raise NoSuchActivityError('\tThe cluster status is empty')


def statuschecker(get_activity_state, activity_name, region, sleep, status):
    """
    A method that runs any of the "singleton" get_state methods every X seconds
    e.g. cluster_status = statuschecker.statuschecker(statuschecker.get_emr_state, 'SampleData-123', 'us-east-1', 10, 'TERMINATED')
    :param get_activity_state: (func) a method that retreives the status - e.g get_pipeline_state
    :param activity_name: (str) name of the cluster or the pipeline
    :param region: (str) AWS region
    :param sleep: (int) number of seconds between each check
    :param status: (str) AWS status that breaks the loop
    :return: The status returned by the cluster/pipeline once it became == to the <status> param
    """
    check_status = True
    cluster_status = ''
    logger.info('\tPing status of the %s every %s seconds ...', activity_name, sleep)
    while check_status:
        try:
            cluster_status = get_activity_state(activity_name, region)
            time.sleep(sleep)
            logger.info('\tCurrent status : %s', cluster_status)
            if cluster_status == status:
                logger.info('\tStatus is now %s',  cluster_status)
                # reset the status to exit the loop
                check_status = False
                logger.info('\tStop the checker')
        except NoSuchActivityError as error:
            logger.error(error)
            break
    return cluster_status