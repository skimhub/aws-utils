from aws_utils.region_selector import get_cheapest_availability_zone


def test_get_cheapest_region():
    best_region, stats = get_cheapest_availability_zone('m2.4xlarge')
    assert isinstance(best_region, str)