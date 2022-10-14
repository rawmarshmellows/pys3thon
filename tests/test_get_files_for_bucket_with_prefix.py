import io

import boto3
from moto import mock_s3

from pys3thon.client import S3Client


@mock_s3
def test_get_files_for_bucket_with_no_files_in_root_prefix():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-1/subdirectory-1/test_fileobj-1.txt",
    )

    file_contents = s3_client.get_files_for_bucket_with_prefix(
        bucket="test-bucket", prefix="test-directory-1/"
    )

    assert len(file_contents) == 0


@mock_s3
def test_get_files_for_bucket_with_prefix_for_less_than_999_files():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    for i in range(10):
        s3_client.upload_fileobj(
            io.BytesIO(b"my data stored as file object in RAM"),
            bucket="test-bucket",
            key=f"test-directory-1/test_fileobj-{i}.txt",
        )

    file_contents = s3_client.get_files_for_bucket_with_prefix(
        bucket="test-bucket", prefix="test-directory-1/"
    )

    assert len(file_contents) == 10
    assert sorted([file_content["Key"] for file_content in file_contents]) == [
        f"test-directory-1/test_fileobj-{i}.txt" for i in range(10)
    ]


@mock_s3
def test_get_files_for_bucket_with_prefix_for_more_than_999_files():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    for i in range(1001):
        s3_client.upload_fileobj(
            io.BytesIO(b"my data stored as file object in RAM"),
            bucket="test-bucket",
            key=f"test-directory-1/test_fileobj-{i}.txt",
        )

    file_contents = s3_client.get_files_for_bucket_with_prefix(
        bucket="test-bucket", prefix="test-directory-1/"
    )

    assert len(file_contents) == 1001
    assert sorted(
        [file_content["Key"] for file_content in file_contents]
    ) == sorted(
        [f"test-directory-1/test_fileobj-{i}.txt" for i in range(1001)]
    )
