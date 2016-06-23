from random import sample

from aws_utils import NoSuchActivityError
from aws_utils.emr import check_state
from mock import patch
import pytest


RANDOM_STATUS = current_status_sample_emr = sample(set([1, 2, 3, 4, 5]), 1)[0]
MATCH_STATE = 'MATCH'
NOMATCH_STATE = 'NOMATCH'
REGION = 'us-reg'

mocked_response = {
                   'Clusters': [
                                {
                                 'Status': {
                                            'Timeline': {},
                                            'State': MATCH_STATE,
                                            'StateChangeReason': {
                                                                  'Message': '',
                                                                  'Code': '1'
                                                                  }
                                            },
                                 'NormalizedInstanceHours': 32,
                                 'Id': 'j-3L8',
                                 'Name': 'Cluster-Match'
                                 },
                                {
                                 'Status': {
                                            'Timeline': {},
                                            'State': NOMATCH_STATE,
                                            'StateChangeReason': {
                                                                  'Message': '',
                                                                  'Code': '1'
                                                                  }
                                            },
                                 'NormalizedInstanceHours': 64,
                                 'Id': 'j-L7R',
                                 'Name': 'Cluster-Nomatch'
                                 },
                                {
                                 'Status': {
                                            'Timeline': {},
                                            'State': RANDOM_STATUS,
                                            'StateChangeReason': {
                                                                  'Message': '',
                                                                  'Code': '1'
                                                                  }
                                            },
                                 'NormalizedInstanceHours': 0,
                                 'Id': 'j-3D2',
                                 'Name': 'Cluster-Random'
                                 }
                                ]
                   }


def test_get_emr_cluster_state():
    assert check_state.get_emr_cluster_state('Cluster-Match', emr_list_clusters=mocked_response) == MATCH_STATE


def test_get_emr_cluster_state_exception():
    with pytest.raises(NoSuchActivityError):
        check_state.get_emr_cluster_state('Not Exist', emr_list_clusters=mocked_response)


@patch('aws_utils.utils.poller')
def test_poll_for_cluster_state(mocked_poller):
    mocked_poller.return_value = True
    assert check_state.poll_for_cluster_state('Cluster-Match', REGION, MATCH_STATE, 0, 0) == True

    mocked_poller.return_value = False
    assert check_state.poll_for_cluster_state('Cluster-Match', REGION, NOMATCH_STATE, 0, 0) == False


@patch('aws_utils.emr.check_state.get_emr_cluster_state')
@patch('aws_utils.emr.check_state.get_boto3_emr_list_clusters')
def test_poll_for_cluster_state_retry(mocked_get_boto3_emr_list_clusters, mocked_get_emr_cluster_state):
    mocked_get_boto3_emr_list_clusters.return_value = mocked_response
    mocked_get_emr_cluster_state.return_value = MATCH_STATE

    assert check_state.poll_for_cluster_state('Cluster-Match', REGION, MATCH_STATE, 0, 10) == True
    mocked_get_boto3_emr_list_clusters.assert_called_with(REGION)
    mocked_get_emr_cluster_state.assert_called_with('Cluster-Match', mocked_response)

    assert check_state.poll_for_cluster_state('Cluster-Match', REGION, NOMATCH_STATE, 0, 10) == False
    mocked_get_boto3_emr_list_clusters.assert_called_with(REGION)
    mocked_get_emr_cluster_state.assert_called_with('Cluster-Match', mocked_response)

