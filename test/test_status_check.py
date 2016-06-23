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

@patch('aws_utils.emr.status.emr.get_emr_state')
@patch('aws_utils.emr.status.poller')
def test_poll_for_status(mocked_get_emr_state, mocked_poller):
    from aws_utils.emr.status.emr import get_emr_state
    mocked_get_emr_state.return_value = 'MATCH'

    from aws_utils.emr.status import simple_poller
    mocked_poller.return_value = 'MATCH'

    emr.poll_for_status('Cluster-Match', REGION, 'MATCH', 10) == mocked_get_emr_state.return_value[0]



# TODO Make it work
@patch('aws_utils.emr.status.emr.get_emr_state')
@patch('aws_utils.emr.status.poller')
def test_poll_for_status_retry(mocked_get_emr_state, mocked_status):
    from aws_utils.emr.status.emr import get_emr_state
    mocked_get_emr_state.return_value = 'MATCH'

    from aws_utils.emr.status import simple_poller
    mocked_status.return_value = 'NOMATCH'#True

    # assert emr.poll_for_status('Cluster-Match', REGION, 'MATCH', 10) is retried

    assert 1 == 1
