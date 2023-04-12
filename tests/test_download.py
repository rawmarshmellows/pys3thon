import os
from pathlib import Path
from uuid import uuid4

import boto3
from moto import mock_s3

from pys3thon.client import S3Client


@mock_s3
def test_download_file(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean.pdf"

    tmp_file_path = str(tmpdir / "Nepean.pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    try:
        file_name = source_key.split("/")[-1]
        s3_client.download(source_bucket, source_key, tmpdir / file_name)
        assert Path(tmpdir / file_name).exists()
    except Exception as e:
        assert False, str(e)


@mock_s3
def test_download_file_with_key_that_is_uri_encoded(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean%20Legend(2).pdf"

    tmp_file_path = str(tmpdir / "Nepean.pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    try:
        file_name = source_key.split("/")[-1]
        s3_client.download(source_bucket, source_key, tmpdir / file_name)
        assert Path(tmpdir / file_name).exists()
    except Exception as e:
        assert False, str(e)


@mock_s3
def test_download_to_temporary_file(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean.pdf"

    tmp_file_path = str(tmpdir / "Nepean.pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    object_size = s3_client.get_object_size(source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    try:
        with s3_client.download_to_temporary_file(
            source_bucket, source_key
        ) as temp_file_path:
            assert Path(temp_file_path).name == source_key.split("/")[-1]
            assert Path(temp_file_path).exists()
            assert os.stat(temp_file_path).st_size == object_size
        assert Path(temp_file_path).exists() is False
    except Exception as e:
        assert False, str(e)


@mock_s3
def test_download_to_temporary_file_with_specified_file_name(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()
    source_bucket = "test-bucket"
    source_key = f"{str(uuid4())}/Nepean.pdf"

    tmp_file_path = str(tmpdir / "Nepean.pdf")
    with open(tmp_file_path, "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    s3_client.upload_file(tmp_file_path, source_bucket, source_key)
    object_size = s3_client.get_object_size(source_bucket, source_key)
    assert s3_client.check_if_exists_in_s3(source_bucket, source_key)

    try:
        with s3_client.download_to_temporary_file(
            source_bucket, source_key, file_name="some_file_name"
        ) as temp_file_path:
            assert Path(temp_file_path).name == "some_file_name"
            assert Path(temp_file_path).exists()
            assert os.stat(temp_file_path).st_size == object_size
        assert Path(temp_file_path).exists() is False
    except Exception as e:
        assert False, str(e)
