from random import sample

from aws_utils import NoSuchActivityError
from mock import patch
from mock.mock import Mock
import pytest

from aws_utils.datapipeline import check_state


RANDOM_STATUS = current_status_sample_emr = sample(set([1, 2, 3, 4, 5]), 1)[0]
MATCH_PIPELINE_STATE = 'MATCH'
MATCH_HEALTH_STATUS = 'HEALTH'
NOMATCH_STATE = 'NOMATCH'
NOTMATCH_HEALTH_STATUS = 'EXIT'
REGION = 'us-reg'
MATCH_RESULT = {'pipelineState': MATCH_PIPELINE_STATE,
                'healthStatus': MATCH_HEALTH_STATUS}

mocked_pipelineIdList = {
    'pipelineIdList': [
        {'id': 'id1', 'name': 'name1'},
        {'id': 'id2', 'name': 'name2'},
        {'id': 'id3', 'name': 'name3'}
    ]
}

mocked_pipelinesMetadata = {
        'ResponseMetadata': {'HTTPStatusCode': 200,
        'RequestId': 'abc'},
        'pipelineDescriptionList': [
        {
            'pipelineId': 'id1',
            'name': 'name1',
            'fields': [
                {
                    'key': '@pipelineState',
                    'stringValue': MATCH_PIPELINE_STATE,
                    'refValue': 'string'
                },
                {
                    'key': '@healthStatus',
                    'stringValue': MATCH_HEALTH_STATUS,
                    'refValue': 'string'
                },
            ],
            'description': 'string',
            'tags': [
                {
                    'key': 'string',
                    'value': 'string'
                },
            ]
        },
    ]
}

mocked_NonExistingPipelineMetadata = {'pipelineDescriptionList': [{u'fields': []}]}


@patch('boto3.client')
def test_get_pipeline_state(mocked_client):
    boto3client_pipelines = Mock()
    mocked_client.return_value = boto3client_pipelines

    boto3client_pipelines.list_pipelines = Mock()
    boto3client_pipelines.list_pipelines.return_value = mocked_pipelineIdList

    boto3client_pipelines.describe_pipelines = Mock()
    boto3client_pipelines.describe_pipelines.return_value = mocked_pipelinesMetadata

    assert check_state.get_pipeline_state(mocked_pipelinesMetadata) == MATCH_RESULT

    with pytest.raises(NoSuchActivityError):
        check_state.get_pipeline_state(mocked_NonExistingPipelineMetadata)


@patch('aws_utils.datapipeline.check_state.get_pipeline_state')
@patch('aws_utils.datapipeline.check_state.get_boto3_emr_pipeline_settings')
def test_poll_for_cluster_state_retry(mocked_get_boto3_emr_pipeline_settings, mocked_get_pipeline_state):
    mocked_get_boto3_emr_pipeline_settings.return_value = mocked_pipelinesMetadata
    mocked_get_pipeline_state.return_value = MATCH_RESULT

    assert check_state.poll_pipeline_for_state('name1', REGION, {'pipelineState': MATCH_PIPELINE_STATE,
                                                                'healthStatus': MATCH_HEALTH_STATUS}, 0, 0) == True
    mocked_get_pipeline_state.assert_called_with(mocked_pipelinesMetadata)


    assert check_state.poll_pipeline_for_state('name1', REGION, {'pipelineState': NOMATCH_STATE,
                                                                 'healthStatus': MATCH_HEALTH_STATUS}, 0, 0) == False
    mocked_get_pipeline_state.assert_called_with(mocked_pipelinesMetadata)


    assert check_state.poll_pipeline_for_state('name1', REGION, {'pipelineState': MATCH_PIPELINE_STATE,
                                                                 'healthStatus': NOTMATCH_HEALTH_STATUS}, 0, 0) == True
    mocked_get_pipeline_state.assert_called_with(mocked_pipelinesMetadata)

    # replaces the healthStatus in the mock with "ERROR" so we can test if it terminates on ERROR
    mocked_get_boto3_emr_pipeline_settings.return_value['pipelineDescriptionList'][0]['fields'][1]['stringValue'] = NOTMATCH_HEALTH_STATUS
    assert check_state.poll_pipeline_for_state('name1', REGION, {'pipelineState': MATCH_PIPELINE_STATE,
                                                                 'healthStatus': MATCH_HEALTH_STATUS}, 0, 0) == True
    mocked_get_pipeline_state.assert_called_with(mocked_pipelinesMetadata)