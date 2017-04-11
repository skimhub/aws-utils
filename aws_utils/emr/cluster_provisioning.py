import copy

DEFAULT_EMR = 'emr-4.6.0'

INSTANCE = {
    'InstanceGroups': [
        {
            'Name': 'Master',
            'Market': 'SPOT',  # SPOT|ON_DEMAND
            'InstanceRole': 'MASTER',
            'BidPrice': '',  # bid price for instance
            'InstanceType': '',  # instance type
            'InstanceCount': 1,
        },
        {
            'Name': 'Executors',
            'Market': 'SPOT',  # SPOT|ON_DEMAND
            'InstanceRole': 'CORE',
            'BidPrice': '',  # bid price for instance
            'InstanceType': '',  # instance type
            'InstanceCount': 1,
        },
    ],
    'Ec2KeyName': 'EMR',
    'Ec2SubnetId': '',
    'KeepJobFlowAliveWhenNoSteps': False,  # keep alive if no steps submitted
    'TerminationProtected': False}

CONFIGURATIONS = [
    {
        'Classification': 'spark',
        'Properties': {
            'maximizeResourceAllocation': 'true'
        }
    },
    {
        'Classification': 'spark-defaults',
        'Properties': {
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.speculation': 'false'
        }
    }
]

BOOTSTRAP_ACTION = {

    'Name': '',  # name of bootstrap action
    'ScriptBootstrapAction': {
        'Path': '',  # path to script runner that executes bootstrap action
        'Args': [
            '',  # package version
        ]
    }
}


def _get_cluster_config(name, billing, billing_secondary, billing_tag, log_uri, configurations_config,
                        instance_template, emr_image=DEFAULT_EMR):
    return {
        'Name': name,  # name of the EMR cluster
        'VisibleToAllUsers': True,
        'ReleaseLabel': emr_image,
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'LogUri': log_uri,  # EMR log uri
        'Applications': [{'Name': 'Spark'}, {'Name': 'Ganglia'}],
        'Tags': [
            {'Key': 'billing', 'Value': billing},
            {'Key': 'billing_secondary', 'Value': billing_secondary},
            {'Key': 'tier', 'Value': billing_tag}
        ],
        'Instances': instance_template,  # instance config
        'Configurations': configurations_config,  # configuration config
        'BootstrapActions': '',  # bootstrap config
        'Steps': '',  # steps config
    }


STEP = {
    'Name': '',
    'ActionOnFailure': 'TERMINATE_CLUSTER',
    'HadoopJarStep': {

        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
        'MainClass': '',
        'Args': [
            '',  # path to executor
            '',  # command that will be executed
        ]
    }
}


def construct_base_cluster(instances, instance_type, bid_price, vpc_subnet, cluster_name, billing_tag, cluster_log_uri,
                           billing, billing_secondary,
                           emr_image=DEFAULT_EMR,
                           on_demand=None,
                           keep_alive=None):
    """Returns a completed boto3 cluster configuration without steps.

    Args:
        billing (str):
        billing_secondary (str):
        instances (int):
            number of core instances the cluster should have.
        instance_type (str):
            ec2 instance type master and core instances.
        bid_price (str):
            bid price for the EC2 instances within the cluster, core and
            master.
        vpc_subnet (str):
            VPC subnet the EMR cluster should be hosted in.
        cluster_name (str):
            Name of the cluster.
        billing_tag (str):
            AWS billing tag to be associated with the cluster, 'live' or 'dev'.
        cluster_log_uri (str):
            S3 uri to where the EMR logs will be stored.
        on_demand (boolean):
            whether we should use on demand instances rather than spot.
        keep_alive (boolean):
            whether the cluster should remain alive without steps/upon
            failure of a step.
    Returns:
        dict :
            boto3 config ready to be submitted to AWS.
            Does not include bootstrap or steps.
    """
    # Load the templates
    instance_template = copy.deepcopy(INSTANCE)
    configurations_config = copy.deepcopy(CONFIGURATIONS)

    # Update master and core instance values
    master_instance_config, core_instance_config = instance_template['InstanceGroups']
    core_instance_config['InstanceCount'] = instances
    master_instance_config['BidPrice'] = bid_price
    core_instance_config['BidPrice'] = bid_price
    master_instance_config['InstanceType'] = instance_type
    core_instance_config['InstanceType'] = instance_type

    if on_demand:
        core_instance_config['Market'] = 'ON_DEMAND'
        master_instance_config['Market'] = 'ON_DEMAND'

    instance_template['InstanceGroups'] = [master_instance_config, core_instance_config]

    # Update general instance settings
    instance_template['Ec2SubnetId'] = vpc_subnet
    if keep_alive:
        instance_template['KeepJobFlowAliveWhenNoSteps'] = True

    # Update general cluster settings
    cluster_config = _get_cluster_config(cluster_name, billing, billing_secondary, billing_tag, cluster_log_uri, configurations_config,
                                         instance_template, emr_image)

    return cluster_config


def construct_bootstrap_action(bootstrap_name, bootstrap_path, bootstrap_args):
    """Constructs a bootstrap action for a AWS EMR cluster.

    Args:
        bootstrap_name, str:
            descriptive name of the bootstrap action.
        bootstrap_path, str:
            S3 uri to a bootstrap shell script.
        bootstrap_args, list:
            list of str arguments for the bootstrap_path.
    Returns:
        bootstrap_action, list:
            a boto3 bootstrap action for a AWS EMR cluster.
    """
    # Load the template
    bootstrap_action_template = copy.deepcopy(BOOTSTRAP_ACTION)

    # Update required values
    bootstrap_action_template['Name'] = bootstrap_name
    bootstrap_action_template['ScriptBootstrapAction']['Args'] = bootstrap_args
    bootstrap_action_template['ScriptBootstrapAction']['Path'] = bootstrap_path

    return bootstrap_action_template


def construct_spark_step(spark_args, script_args, script_path, exec_path, step_name, keep_alive):
    """Returns a spark step for a cluster.

    Spark steps are constructed of 4 things:
        spark arguments, script location script arguments and a path
        to the executable that runs the step itself.

    Args:
        spark_args, list:
            comma separated spark arguments.
        script_args, list:
            comma separated script arguments
        script_path, str:
            path to the spark script to be run, relative to the top
            of the repo.
        exec_path, str:
            S3 path to the script that runs the step.
        step_name, str:
            descriptive name of the step.
        keep_alive, boolean:
            whether we should keep the cluster alive in the event of
            the step failing.
    Returns:
        step, dict:
            a boto3 spark step for a AWS EMR cluster
    """
    # Load the template
    step_template = copy.deepcopy(STEP)

    # Construct the spark and script argument
    step_args = spark_args + [script_path] + script_args
    step = ' '.join(str(arg) for arg in step_args)

    # Update required values
    step_template['Name'] = step_name
    step_template['HadoopJarStep']['Args'] = [exec_path, step]
    if keep_alive:
        step_template['ActionOnFailure'] = 'CANCEL_AND_WAIT'

    return step_template
