import pytest
import boto3
from botocore.stub import Stubber


@pytest.fixture(scope='module')
def boto3_client():
    def pass_response(type, region):
        client = boto3.client(type, region)
        return client
    return pass_response


@pytest.fixture(scope='module')
def boto3_get_response():
    def pass_response(method, stubbed_client, mocked_response):
        stubber = Stubber(stubbed_client)
        stubber.add_response(method, mocked_response)#, expected_params)
        with stubber:
            response = stubbed_client.list_buckets()
        return response
    return pass_response