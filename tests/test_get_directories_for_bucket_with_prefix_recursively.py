import io

import boto3
from moto import mock_s3

from pys3thon.client import S3Client


@mock_s3
# flake8: noqa
def test_get_directories_for_bucket_with_prefix_recursively_works_on_bucket_root_with_one_directory():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    test_fileobj = io.BytesIO(b"my data stored as file object in RAM")
    s3_client.upload_fileobj(
        test_fileobj,
        bucket="test-bucket",
        key="test-directory/test_fileobj.txt",
    )

    directories = s3_client.get_directories_for_bucket_with_prefix_recursively(
        bucket="test-bucket", prefix=None
    )
    assert directories == ["test-directory/"]


@mock_s3
# flake8: noqa
def test_get_directories_for_bucket_with_prefix_recursively_works_on_bucket_root_with_three_directories():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-1/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-2/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-3/test_fileobj.txt",
    )

    directories = s3_client.get_directories_for_bucket_with_prefix_recursively(
        bucket="test-bucket", prefix=None
    )

    assert sorted(directories) == [
        "test-directory-1/",
        "test-directory-2/",
        "test-directory-3/",
    ]


@mock_s3
# flake8: noqa
def test_get_directories_for_bucket_with_prefix_recursively_works_on_bucket_root_directory_three_levels_deep():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    test_fileobj = io.BytesIO(b"my data stored as file object in RAM")
    s3_client.upload_fileobj(
        test_fileobj,
        bucket="test-bucket",
        key="test-directory/a/b/c/test_fileobj.txt",
    )

    directories = s3_client.get_directories_for_bucket_with_prefix_recursively(
        bucket="test-bucket", prefix=None
    )
    assert sorted(directories) == [
        "test-directory/",
        "test-directory/a/",
        "test-directory/a/b/",
        "test-directory/a/b/c/",
    ]


@mock_s3
# flake8: noqa
def test_get_directories_for_bucket_with_prefix_recursively_works_on_bucket_root_with_three_directories_three_levels_deep():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()

    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-1/a/b/c/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-2/a/b/c/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-3/a/b/c/test_fileobj.txt",
    )

    directories = s3_client.get_directories_for_bucket_with_prefix_recursively(
        bucket="test-bucket", prefix=None
    )

    assert sorted(directories) == [
        "test-directory-1/",
        "test-directory-1/a/",
        "test-directory-1/a/b/",
        "test-directory-1/a/b/c/",
        "test-directory-2/",
        "test-directory-2/a/",
        "test-directory-2/a/b/",
        "test-directory-2/a/b/c/",
        "test-directory-3/",
        "test-directory-3/a/",
        "test-directory-3/a/b/",
        "test-directory-3/a/b/c/",
    ]


@mock_s3
# flake8: noqa
def test_get_directories_for_bucket_with_prefix_recursively_works_on_bucket_nested_directory_with_three_directories_in_root():
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()

    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-1/a/b/c/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-2/a/b/c/test_fileobj.txt",
    )
    s3_client.upload_fileobj(
        io.BytesIO(b"my data stored as file object in RAM"),
        bucket="test-bucket",
        key="test-directory-3/a/b/c/test_fileobj.txt",
    )

    directories = s3_client.get_directories_for_bucket_with_prefix_recursively(
        bucket="test-bucket", prefix="test-directory-3/"
    )

    assert sorted(directories) == [
        "test-directory-3/",
        "test-directory-3/a/",
        "test-directory-3/a/b/",
        "test-directory-3/a/b/c/",
    ]
