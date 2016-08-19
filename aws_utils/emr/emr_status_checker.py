# -*- coding: utf-8 -*-
import logging
import time
from collections import namedtuple
from datetime import datetime, timedelta

from dateutil.tz import tzlocal

logging.basicConfig()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

States = namedtuple('States', ['running', 'success', 'failure'])

CLUSTER_STATES = States(
    ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING'],
    ['TERMINATED'],
    ['TERMINATED_WITH_ERRORS'])


class EMRPollingException(Exception):
    pass

def on_cluster_status_change(logger, status, prev_state):
    """Handle EMR cluster status change."""
    logger.info('Cluster status changed: %s -> %s', prev_state, status)


def on_cluster_success_status(logger, status, reason):
    """Handle EMR cluster success status."""
    logger.info('Cluster success: %s %s', status, reason)


def on_cluster_failure_status(logger, status, reason):
    """Handle EMR cluster failure status."""
    msg = 'Cluster failure: {} {}'.format(status, reason)
    logger.exception(msg)
    raise EMRPollingException(msg)


def has_reached_wait_limit(logger, status, wait_limit):
    '''
    :param status: dict containing `Timeline` key with cluster's `CreationDateTime`
    :param wait_limit: limit to wait for the cluster to finish its job
    :return: a tuple boolean to consider whether the polling for the cluster
    status should be ended or not.
    '''
    stop = False
    start = status['Timeline']['CreationDateTime']
    now = datetime.now(tzlocal())
    elapsed_time = now - start

    if elapsed_time.total_seconds() > wait_limit:
        msg = ('This job has been running for over {}. '
               'Considering it a failure since the wait limit is {}'
               ''.format(timedelta(seconds=elapsed_time.total_seconds()),
                         timedelta(seconds=wait_limit)))
        stop = True
        logger.warning(msg)

    return stop


def has_cluster_successfully_finished(emr, cluster_id, logger=LOGGER,
                                      on_status_change=on_cluster_status_change,
                                      on_success=on_cluster_success_status,
                                      on_failure=on_cluster_failure_status,
                                      wait_duration=30, wait_limit=86400):
    '''
    :param emr: botocore.client.EMR object
    :param logger: logger to log
    :param cluster_id: cluter ID on AWS
    :param on_status_change: action to perform on status change
    :param on_success: action to perform on cluster success
    :param on_failure: action to perform on cluster failure
    :param wait_duration: delay in between checks
    :param wait_limit: total limit to wait for cluster termination. If
    overflow, then raises an EMRPollingException
    :return: True if success, False if any kind of failure.
    '''

    prev_state = None
    while True:
        try:
            description = emr.describe_cluster(ClusterId=cluster_id)

            status = description['Cluster']['Status']

            if status['State'] != prev_state:
                on_status_change(logger, status['State'], prev_state)
                prev_state = status['State']

            if status['State'] in CLUSTER_STATES.success:
                on_success(logger, status['State'], status['StateChangeReason'])
                return True

            elif status['State'] in CLUSTER_STATES.failure:
                on_failure(logger, status['State'], status['StateChangeReason'])
                return False

            if has_reached_wait_limit(logger, status, wait_limit):
                raise EMRPollingException('Cluster is taking too much time')
            else:
                time.sleep(wait_duration)
        except Exception as exc:
            logger.exception('Unable to get cluster status')
            raise EMRPollingException(exc)
