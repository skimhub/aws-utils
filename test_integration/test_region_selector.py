import pytest

from aws_utils.region_selector import get_cheapest_availability_zone


def test_get_cheapest_availability_zone():
    best_region, stats = get_cheapest_availability_zone('m2.4xlarge')
    assert isinstance(best_region, str)
    assert stats['min'] <= stats['avg']
    assert stats['max'] >= stats['avg']


def test_get_cheapest_availability_zone_fail_if_bad_region():
    with pytest.raises(Exception):
        get_cheapest_availability_zone('m2.4xlarge', search_regions='made up region')
