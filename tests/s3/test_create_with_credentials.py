import boto3
from moto import mock_aws

from pys3thon.s3.client import S3Client


@mock_aws
def test_s3_client_with_credentials():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    credentials = {
        "aws_access_key_id": "fake_access",
        "aws_secret_access_key": "fake_secret",
    }
    s3_client = S3Client(credentials=credentials)
    assert s3_client.client._request_signer._credentials.access_key == "fake_access"
    assert s3_client.client._request_signer._credentials.secret_key == "fake_secret"


@mock_aws
def test_s3_client_without_credentials():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()

    # See if the default credentials from moto are used
    assert s3_client.client._request_signer._credentials.access_key == "FOOBARKEY"
    assert s3_client.client._request_signer._credentials.secret_key == "FOOBARSECRET"
