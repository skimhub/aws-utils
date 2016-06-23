import pytest
from random import sample
from mock import Mock, patch, mock
from aws_utils.emr.status import emr

RANDOM_STATUS = current_status_sample_emr = sample(set([1, 2, 3, 4, 5]), 1)[0]
MATCH_STATUS = 'MATCH'
NOMATCH_STATUS = 'NOMATCH'
REGION = 'us-reg'

mocked_response = {u'Clusters': [
    {u'Status': {u'Timeline': {}, u'State': u'MATCH', u'StateChangeReason': {u'Message': u'', u'Code': u'1'}},
     u'NormalizedInstanceHours': 32, u'Id': u'j-3L8', u'Name': u'Cluster-Match'},
    {u'Status': {u'Timeline': {}, u'State': u'NOMATCH', u'StateChangeReason': {u'Message': u'', u'Code': u'1'}},
     u'NormalizedInstanceHours': 64, u'Id': u'j-L7R', u'Name': u'Cluster-Nomatch'},
    {u'Status': {u'Timeline': {}, u'State': RANDOM_STATUS, u'StateChangeReason': {u'Message': u'', u'Code': u'1'}},
     u'NormalizedInstanceHours': 0, u'Id': u'j-3D2', u'Name': u'Cluster-Random'}
]}


def test_get_emr_state():
    from aws_utils.emr.status import emr
    assert emr.get_emr_state('Cluster-Match', REGION, emrs=mocked_response) == 'MATCH'
    #statuses_response = boto3_get_response('list_clusters', client.return_value, mocked_response)
    #print emr.get_emr_state('Cluster-Match', REGION)  # == statuses_response['Clusters']['Status'][
    # 'State']
    # print response
    assert 1 == 1


@patch('aws_utils.emr.status.emr.get_emr_state')
@patch('aws_utils.emr.status.simple_poller')
def test_poll_for_status(mocked_get_emr_state, mocked_poller):
    from aws_utils.emr.status.emr import get_emr_state
    mocked_get_emr_state.return_value = 'MATCH'

    from aws_utils.emr.status import simple_poller
    mocked_poller.return_value = 'MATCH'

    emr.poll_for_status('Cluster-Match', REGION, 'MATCH', 10) == mocked_get_emr_state.return_value[0]


@patch('aws_utils.emr.status.emr.get_emr_state')
@patch('aws_utils.emr.status.simple_poller')
def test_poll_for_status_2(mocked_get_emr_state, mocked_simple_poller):
    from aws_utils.emr.status.emr import get_emr_state
    mocked_get_emr_state.return_value = 'MATCH'

    from aws_utils.emr.status import simple_poller
    # #mocked_poller_list = 'NOMATCH'
    # mocked_get_emr_state.mock_call_count = 2

    mocked_simple_poller.return_value = 'NOMATCH'#True
    #print emr.poll_for_status.status
    #mocked_get_emr_state.assert_called_with(mocked_simple_poller)#emr.poll_for_status('Cluster-Match', REGION,
    # 'MATCH', 10))



    # emr.poll_for_status('Cluster-Nomatch', REGION, 'NOMATCH', 10)


# # @mock.patch('aws_utils.emr.status.botoclient.boto_connect')
# # @mock.patch()
# def test_get_emr_state(boto3_client, boto3_get_response):
#     # from aws_utils.emr.status.botoclient import boto_connect
#     from aws_utils.emr.status import emr
#     #client = Mock(return_value=boto3_client('emr', REGION))
#     #mock_boto_connect = client
#     # mock_boto_connect.return_value = Mock(return_value=boto3_get_response('list_clusters', boto3_client('emr', REGION),
#     #                                                                       mocked_response))#boto3_client('emr', REGION))
#     # print "CL : ", mock_boto_connect.return_value
#     # from aws_utils.emr.status import emr
#     # print emr.boto_connect.__dict__
#     print emr.get_emr_state('Cluster-Match', REGION, emrs=mocked_response)
#     #statuses_response = boto3_get_response('list_clusters', client.return_value, mocked_response)
#     #print emr.get_emr_state('Cluster-Match', REGION)  # == statuses_response['Clusters']['Status'][
#     # 'State']
#     # print response
#     assert 1 == 1
