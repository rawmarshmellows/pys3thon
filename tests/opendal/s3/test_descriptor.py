import json

from pys3thon.opendal.s3.descriptor import S3JSONStorageDescriptor, S3StorageDescriptor
from pys3thon.opendal.shared import StorageDescriptor


def test_create_s3_json_descriptor_with_all_parameters():
    s3_json_descriptor = json.load(
        open("tests/opendal/test_data/s3_json_descriptor_with_all_parameters.json")
    )
    descriptor = S3JSONStorageDescriptor.create_from_json_descriptor(s3_json_descriptor)
    assert descriptor.bucket == "test-bucket"
    assert descriptor.key == "test-key"
    assert descriptor.aws_access_key_id == "test-access"
    assert descriptor.encrypted_aws_secret_access_key == "test-secret"
    assert descriptor.region == "test-region"
    assert descriptor.endpoint == "test-endpoint"
    assert isinstance(descriptor, S3JSONStorageDescriptor)


def test_create_s3_json_descriptor_with_only_required_parameters():
    s3_json_descriptor = json.load(
        open(
            "tests/opendal/test_data/s3_json_descriptor_with_only_required_parameters.json"
        )
    )
    descriptor = S3JSONStorageDescriptor.create_from_json_descriptor(s3_json_descriptor)
    assert descriptor.bucket == "test-bucket"
    assert descriptor.key == "test-key"
    assert descriptor.aws_access_key_id == "test-access"
    assert descriptor.encrypted_aws_secret_access_key == "test-secret"
    assert descriptor.region == "us-west-2"
    assert descriptor.endpoint is None
    assert isinstance(descriptor, S3JSONStorageDescriptor)


def test_create_s3_descriptor_from_s3_json_descriptor_class():
    json_descriptor = S3JSONStorageDescriptor(
        bucket="test-bucket",
        key="test-key",
        aws_access_key_id="test-access",
        encrypted_aws_secret_access_key="test-secret",
        region="test-region",
        endpoint="test-endpoint",
    )
    descriptor = (
        StorageDescriptor.create_from_json_storage_descriptor(
            json_descriptor
        )
    )
    assert isinstance(descriptor, S3StorageDescriptor)
    assert descriptor.bucket == "test-bucket"
    assert descriptor.path == "test-key"
    assert descriptor.aws_access_key_id == "test-access"
    assert descriptor.encrypted_aws_secret_access_key == "test-secret"
    assert descriptor.region == "test-region"
    assert descriptor.endpoint == "test-endpoint"
