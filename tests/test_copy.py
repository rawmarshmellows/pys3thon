from pathlib import Path
from uuid import uuid4

import boto3
from moto import mock_s3

from pys3thon.client import S3Client


@mock_s3
def test_persist_file(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean.pdf"
    destination_bucket = "test-bucket"
    destination_key = f"{str(uuid4())}/Nepean.pdf"

    tmp_file_path = str(tmpdir / "Nepean.pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    s3_client.copy(
        source_bucket, source_key, destination_bucket, destination_key
    )
    assert s3_client.check_if_exists_in_s3(destination_bucket, destination_key)


@mock_s3
def test_persist_file_for_uri_encoded_file(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean%20Legend%20(2).pdf"
    destination_bucket = "test-bucket"
    destination_key = f"{str(uuid4())}/Nepean%20Legend%20(2).pdf"

    tmp_file_path = str(tmpdir / "Nepean%20Legend%20(2).pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    s3_client.copy(
        source_bucket, source_key, destination_bucket, destination_key
    )
    assert s3_client.check_if_exists_in_s3(destination_bucket, destination_key)
