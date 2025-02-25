import os
import time

import pytest
import requests

from pys3thon.opendal.s3.client import OpenDALS3Client
from pys3thon.opendal.s3.descriptor import S3StorageDescriptor
from pys3thon.opendal.shared import OpenDALClient


def test_create_opendal_s3_client_from_s3_encrypted_descriptor():
    descriptor = S3StorageDescriptor(
        bucket="test-bucket",
        path="test-key",
        aws_access_key_id="test-access",
        encrypted_aws_secret_access_key="encrypted-test-secret",
        region="test-region",
        endpoint="http://www.test-endpoint.com",
    )
    descriptor.decrypt(lambda x: x)
    client = OpenDALClient.create_from_storage_descriptor(descriptor)
    assert isinstance(client, OpenDALS3Client)
    assert client.bucket == "test-bucket"
    assert client.region == "test-region"
    assert client.access_key_id == "test-access"
    assert client.secret_access_key == "encrypted-test-secret"


def test_create_opendal_s3_client_from_s3_unencrypted_descriptor():
    descriptor = S3StorageDescriptor(
        bucket="test-bucket",
        path="test-key",
        aws_access_key_id="test-access",
        aws_secret_access_key="test-secret",
        region="test-region",
        endpoint="http://www.test-endpoint.com",
    )
    descriptor.decrypt(lambda x: x)
    client = OpenDALClient.create_from_storage_descriptor(descriptor)
    assert isinstance(client, OpenDALS3Client)
    assert client.bucket == "test-bucket"
    assert client.region == "test-region"
    assert client.access_key_id == "test-access"
    assert client.secret_access_key == "test-secret"


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_write_and_read(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    upload_path = f"/{timestamp}_file.txt"

    descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=upload_path,
        region=opendal_remote_configs["s3"]["region"],
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    client = OpenDALClient.create_from_storage_descriptor(descriptor)

    tmp_file_path = str(tmpdir / "test.txt")
    with open(tmp_file_path, "w") as local_file:
        local_file.write("Hello, world!")

    try:
        with open(tmp_file_path, "rb") as local_file:
            client.write(descriptor.path, local_file.read())
        assert client.read(descriptor.path) == b"Hello, world!"
    finally:
        client.delete(descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_presign_read(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    upload_path = f"/{timestamp}_file.txt"

    descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=upload_path,
        region=opendal_remote_configs["s3"]["region"],
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    client = OpenDALClient.create_from_storage_descriptor(descriptor)

    tmp_file_path = str(tmpdir / "test.txt")
    with open(tmp_file_path, "w") as local_file:
        local_file.write("Hello, world!")

    try:
        with open(tmp_file_path, "rb") as local_file:
            client.write(descriptor.path, local_file.read())

        presigned_read = client.presign_read(descriptor.path, expiration=60)
        response = requests.get(presigned_read.url)
        assert response.text == "Hello, world!"
    finally:
        client.delete(descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_delete(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    upload_path = f"/{timestamp}_file.txt"

    descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=upload_path,
        region=opendal_remote_configs["s3"]["region"],
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    client = OpenDALClient.create_from_storage_descriptor(descriptor)

    # Create a test file
    tmp_file_path = str(tmpdir / "test.txt")
    with open(tmp_file_path, "w") as local_file:
        local_file.write("Hello, world!")

    # Upload the file
    with open(tmp_file_path, "rb") as local_file:
        client.write(descriptor.path, local_file.read())

    # Verify file exists
    assert client.read(descriptor.path) == b"Hello, world!"

    # Delete the file
    client.delete(descriptor.path)

    # Verify file no longer exists by checking if read raises an exception
    with pytest.raises(Exception):
        client.read(descriptor.path)
