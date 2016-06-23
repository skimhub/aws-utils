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


@patch('boto3.client')
def test_get_pipeline_state(mocked_client):
    boto3client_pipelines = Mock()
    mocked_client.return_value = boto3client_pipelines

    boto3client_pipelines.list_pipelines = Mock()
    boto3client_pipelines.list_pipelines.return_value = mocked_pipelineIdList

    boto3client_pipelines.describe_pipelines = Mock()
    boto3client_pipelines.describe_pipelines.return_value = mocked_pipelinesMetadata

    assert check_state.get_pipeline_state('name1', REGION) == MATCH_RESULT

    with pytest.raises(NoSuchActivityError):
        check_state.get_pipeline_state('name not exist', REGION)


@patch('aws_utils.datapipeline.check_state.get_pipeline_state')
def test_poll_for_cluster_state_retry(mocked_get_pipeline_state):
    mocked_get_pipeline_state.return_value = MATCH_RESULT

    assert check_state.poll_for_state(
        'name1', REGION, MATCH_PIPELINE_STATE, 0, 0) == True
    mocked_get_pipeline_state.assert_called_with('name1', REGION)

    assert check_state.poll_for_state(
        'name not exist', REGION, NOMATCH_STATE, 0, 0) == False
    mocked_get_pipeline_state.assert_called_with('name not exist', REGION)
