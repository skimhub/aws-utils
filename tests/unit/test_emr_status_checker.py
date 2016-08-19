import datetime

from dateutil.tz import tzlocal
from mock import MagicMock
from pytest import raises

from aws_utils.emr.emr_status_checker import (has_cluster_successfully_finished,
                                              EMRPollingException)

TEST_CLUSTER_ID = 'A Cluster ID'

RUNNING_CLUSTER_STATUS_RESPONSE = {
    u'Cluster': {
        u'Id': u'A Cluster ID',
        u'Status': {
            u'State': u'RUNNING',
            u'StateChangeReason': {
                u'Message': u'Running step'
            },
            u'Timeline': {
                u'CreationDateTime': datetime.datetime(2000, 7, 13, 11, 34, 21, 919000, tzinfo=tzlocal()),
                u'ReadyDateTime': datetime.datetime(2016, 7, 13, 11, 44, 48, 973000, tzinfo=tzlocal())
            }
        }
    }
}


TERMINATED_CLUSTER_STATUS_RESPONSE = {
    u'Cluster': {
        u'Id': u'A Cluster ID',
        u'Status': {
            u'State': u'TERMINATED',
            u'StateChangeReason': {
                u'Code': u'ALL_STEPS_COMPLETED',
                u'Message': u'Steps completed'
            },
            u'Timeline': {
                u'CreationDateTime': datetime.datetime(2016, 7, 13, 10, 0, 0, 576000, tzinfo=tzlocal()),
                u'EndDateTime': datetime.datetime(2016, 7, 13, 10, 45, 48, 205000, tzinfo=tzlocal()),
                u'ReadyDateTime': datetime.datetime(2016, 7, 13, 10, 4, 6, 25000, tzinfo=tzlocal())
            }
        }
    }
}



TERMINATED_WITH_ERRORS_CLUSTER_STATUS_RESPONSE = {
    u'Cluster': {
        u'Id': u'A Cluster ID',
        u'Status': {
            u'State': u'TERMINATED_WITH_ERRORS',
            u'StateChangeReason': {
                u'Code': u'STEP_FAILURE',
                u'Message': u'Shut down as step failed'
            },
            u'Timeline': {
                u'CreationDateTime': datetime.datetime(2016, 7, 13, 5, 52, 5, 368000, tzinfo=tzlocal()),
                u'EndDateTime': datetime.datetime(2016, 7, 13, 7, 43, 59, 890000, tzinfo=tzlocal()),
                u'ReadyDateTime': datetime.datetime(2016, 7, 13, 5, 56, 11, 219000, tzinfo=tzlocal())
            }
        }
    }
}


def test_has_cluster_successfully_finished():
    emr_mock = MagicMock()
    attrs = {'describe_cluster':
                 (lambda ClusterId: TERMINATED_CLUSTER_STATUS_RESPONSE)}
    emr_mock.configure_mock(**attrs)

    assert has_cluster_successfully_finished(emr_mock, TEST_CLUSTER_ID) == True


def test_has_cluster_successfully_finished_not():
    emr_mock = MagicMock()
    attrs = {'describe_cluster':
                 (lambda ClusterId: TERMINATED_WITH_ERRORS_CLUSTER_STATUS_RESPONSE)}
    emr_mock.configure_mock(**attrs)

    with raises(EMRPollingException):
        has_cluster_successfully_finished(emr_mock, TEST_CLUSTER_ID)


def test_has_reached_wait_limit():
    emr_mock = MagicMock()
    attrs = {'describe_cluster':
                 (lambda ClusterId: RUNNING_CLUSTER_STATUS_RESPONSE)}
    emr_mock.configure_mock(**attrs)

    with raises(EMRPollingException):
         has_cluster_successfully_finished(emr_mock, TEST_CLUSTER_ID)

