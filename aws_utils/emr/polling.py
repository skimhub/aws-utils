# -*- coding: utf-8 -*-

import time
from collections import namedtuple

import boto.emr.connection


States = namedtuple('States', ['running', 'success', 'failure'])

CLUSTER_STATES = States(
    ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING'],
    ['TERMINATED'],
    ['TERMINATED_WITH_ERRORS'])

JOBFLOW_STATES = States(
    ['PENDING', 'RUNNING'],
    ['COMPLETED'],
    ['CANCELLED', 'FAILED'])


class EMRFailure(Exception):
    pass


def on_cluster_status_change(conn, logger, status, prev_state):
    """Handle EMR cluster status change."""
    msg = 'Jobflow status changed to {} from {}'.format(
        status.state, prev_state)
    logger.debug(msg)


def on_cluster_success_status(conn, logger, status):
    """Handle EMR cluster success status."""
    logger.debug('Jobflow success: %s %s',
                 status.state, status.laststatechangereason)


def on_cluster_failure_status(conn, logger, status):
    """Handle EMR cluster failure status."""
    msg = 'Jobflow failure: {} {}'.format(
        status.state, status.laststatechangereason)
    logger.debug(msg)
    raise EMRFailure(msg)


def poll_for_status(conn, logger, jobflow_id,
                    on_status_change=on_cluster_status_change,
                    on_success=on_cluster_success_status,
                    on_failure=on_cluster_failure_status,
                    wait_duration=30):
    """Poll the cluster until success or failure of jobflow."""
    prev_state = None

    while True:
        try:
            status = conn.describe_jobflow(jobflow_id)
        except boto.exception.EmrResponseError:
            # TODO: What do to about this? Retry?
            logger.exception('Unable to get jobflow status')

        if status.state != prev_state:
            on_status_change(conn, logger, status, prev_state)
            prev_state = status.state

        if status.state in JOBFLOW_STATES.success:
            on_success(conn, logger, status)
        elif status.state in JOBFLOW_STATES.failure:
            on_failure(conn, logger, status)

        time.sleep(wait_duration)
