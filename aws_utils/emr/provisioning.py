# -*- coding: utf-8 -*-

import json

import boto3
from smart_open import smart_open


DEFAULT_CLUSTER_CONFIG = {
    'VisibleToAllUsers': True,
    'ReleaseLabel': 'emr-4.3.0',
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': 's3://data.api.qa.us/logs/',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Tags': []}


DEFAULT_INSTANCE_CONFIG = {
    'InstanceGroups': [
        {
            'Name': 'Master',
            'Market': 'SPOT',
            'InstanceRole': 'MASTER',
            'BidPrice': '0.2',
            'InstanceType': 'm2.xlarge',
            'InstanceCount': 1
        },
        {
            'Name': 'Executors',
            'Market': 'SPOT',
            'InstanceRole': 'CORE',
            'BidPrice': '0.2',
            'InstanceType': 'm2.xlarge',
            'InstanceCount': 1
        },
    ],
    'Ec2KeyName': 'EMR',
    'Placement': {
        'AvailabilityZone': 'us-east-1e'
    },
    'KeepJobFlowAliveWhenNoSteps': True,
    'TerminationProtected': False}


# Call another function to generate the instance config
# instances = instance_config(
#     master={'type': '', 'count': 1, 'bid_price': None},
#     executors={'type': '', 'count': 10, 'bid_price': 1.2},
#     availability_zone='')

# provision_emr('',
#     instances,
#     get_configurations_file(''),
#     default_bootstrap_steps())

# EMR('name', **kargs).instances().configurations().with_default_bootstrap().steps()

# with EMR('name') as cluster:
#     cluster.instances().configurations().with_default_bootstrap().steps()
#     cluster.start()
#     cluster.submit_step()
#     cluster.terminate()
#     cluster.status()
#     cluster.wait_until_complete()


class EMR(object):
    def __init__(self, name, **cluster_config):
        self.name = name
        self.steps = []
        self.bootstrap_steps = []
        self.cluster_config = cluster_config
        self.client = boto3.client('emr')
        self.jobflow_id = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.terminate()

    def start(self):
        self.client.run_job_flow(
            Name=self.name,
            Steps=self.steps,
            BootstrapActions=self.bootstrap_steps)

    def terminate(self):
        if self.is_active():
            self.client.terminate_job_flows(JobFlowIds=[self.jobflow_id])

    def instances(self):
        return self

    def configurations(self, configurations_dict):
        return self

    def configurations_from_uri(self, uri):
        """Fetch and load a json configurations file.

        The aws cli allows a uri to a json configurations file. Unfortunately,
        boto does not. It expects a list of dicts.
        So, we load the external file here.
        """
        with smart_open.smart_open(uri) as config:
            return self.configurations(json.load(config))

    def steps(self):
        return self

    def with_default_bootstrap(self, **kargs):
        self.bootstrap_steps = default_bootstrap_steps(**kargs)
        return self

    def is_active(self):
        return self.jobflow_id and self.status() in {'RUNNING', 'WAITING'}


def provision_emr(name, instances, configurations, bootstrap_steps, steps=[], **kargs):
    """Start an EMR cluster.

    * `configurations` must be either a uri to a json file or a dict
    * `instances` must be a dict defining InstanceGroups
    """
    config = DEFAULT_CLUSTER_CONFIG.copy()
    config.update(kargs)
    config['Tags'] == format_tags(config['Tags'])

    emr_client = boto3.client('emr')
    # TODO: Allow configurations url or dict

    return emr_client.run_job_flow(
        Name=name,
        Steps=steps,
        BootstrapActions=bootstrap_steps,
        **config)


def run_steps_on_cluster(cluster_id, steps):
    """Run the steps on an existing cluster."""
    raise NotImplementedError()


def default_instance_config(isntance_type, count):
    return DEFAULT_CLUSTER_CONFIG


def get_configurations_file(uri):
    """Fetch and load a json configurations file.

    The aws cli allows a uri to a json configurations file. Unfortunately,
    boto does not. It expects a list of dicts.
    So, we load the external file here.
    """
    with smart_open.smart_open(uri) as config:
        return json.load(config)


def default_bootstrap_steps(ipython=False):
    """Construct default EMR bootstrap steps."""
    steps = []

    if ipython:
        steps.append({
            'Name': 'Install ipython',
            'ScriptBootstrapAction': {
                'Path': 's3://elasticmapreduce/bootstrap-actions/run-if',
                'Args': ['instance.isMaster=true',
                         'sudo pip install ipython']}
            })

    return steps


def format_tags(tags):
    """Format tag tuples as dicts required by the AWS API.

    >>> [('creator', 'me')]
    ... [{'Key': 'creator', 'Value': 'me'}]
    """
    return [{'Key': k, 'Value': v} for k, v in tags]
