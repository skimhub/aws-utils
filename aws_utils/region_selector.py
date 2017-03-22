import datetime

import boto3

US_EAST_REGION = {'us-east-1'}

INSTANCE_VERSION = 'Linux/UNIX (Amazon VPC)'


def fetch_spot_prices(region, start_time, end_time, instance_type, instance_version=INSTANCE_VERSION):
    """Fetches prices of EC2 spot instances from AWS.

    Args:
        region (str): region to look for instances in
        start_time (datetime.datetime):
        end_time (datetime.datetime):
        instance_type (str):
        instance_version (str): the types of instances that we wish to return prices for.

    Returns:
        yield str, float: yields tuple of avialability_zone and price over the period

    Raises: ValueError,
        raised in the event that the boto3 response is empty.
    """
    conn = boto3.client('ec2', region_name=region)

    res = conn.describe_spot_price_history(StartTime=start_time,
                                           EndTime=end_time,
                                           InstanceTypes=[instance_type],
                                           ProductDescriptions=[instance_version])
    for item in res['SpotPriceHistory']:
        yield item['AvailabilityZone'], float(item['SpotPrice'])

    token = res['NextToken']

    while token:
        res = conn.describe_spot_price_history(StartTime=start_time,
                                               EndTime=end_time,
                                               InstanceTypes=[instance_type],
                                               ProductDescriptions=[instance_version],
                                               NextToken=token)
        for item in res['SpotPriceHistory']:
            yield item['AvailabilityZone'], float(item['SpotPrice'])
        token = res['NextToken']


def fetch_price_stats_per_availability_zone(region, start_time, end_time, instance_type, instance_version=INSTANCE_VERSION):
    """Groups raw prices by region, returns min, max and avg price.

    Args:
        region (str): region to look for instances in
        start_time (datetime.datetime):
        end_time (datetime.datetime):
        instance_type (str):
        instance_version (str): the types of instances that we wish to return prices for.

    Returns: dict,
        {'us-east-1b': {'min': 2.01, 'max': 3.53,'avg':2.8, 'latest':3.0}}
    """
    by_zone = {}
    for zone, price in fetch_spot_prices(region, start_time, end_time, instance_type, instance_version):
        by_zone.setdefault(zone, []).append(price)

    prices_per_region = {}
    for zone, prices in by_zone.iteritems():
        region_prices = {'min': min(prices),
                         'max': max(prices),
                         'avg': sum(prices) / float(len(prices)),
                         'latest': prices[0]}
        prices_per_region[zone] = region_prices

    return prices_per_region


def get_cheapest_availability_zone(instance_type, search_regions=US_EAST_REGION, expected_job_length=datetime.timedelta(days=1)):
    """Get the cheapest availability zone from a set of regions. Cheapest is deterened by 'latest price + average price'
    over the duration that the job is expected to run for

    Args:
        instance_type (str): Type of aws instance e.g. "m2.4xlarge"
        search_regions ({str}): Set of regions we want to look for availability zones in.
        expected_job_length (datetime.timedelta): The period we expect the job to run this is used as the amount of time to look back over
            for the average

    Returns:
        (str, {}) : e.g. ('us-east-1b': {'min': 2.01, 'max': 3.53,'avg':2.8, 'latest':3.0})
    """
    if isinstance(search_regions, str):
        search_regions = {search_regions}

    aggregated_prices = {}
    for region in search_regions:
        result_stats = fetch_price_stats_per_availability_zone(region,
                                                               datetime.datetime.utcnow() - expected_job_length,
                                                               datetime.datetime.utcnow(),
                                                               instance_type)

        if not len(result_stats):
            raise Exception("No valid avialability zones found for region %s" % (region,))

        aggregated_prices.update(result_stats)

    cheapest_availability_zone, stats = min(aggregated_prices.iteritems(), key=lambda x: x[1]['avg'] + x[1]['latest'])

    return cheapest_availability_zone, stats
