import os
import time

import pytest
import opendal.exceptions as opendal_exceptions

from pys3thon.opendal.s3.descriptor import S3StorageDescriptor
from pys3thon.opendal.service import OpenDALService
from pys3thon.opendal.shared import OpenDALClient


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_copy(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    opendal_operators["s3"]
    source_upload_path = f"{timestamp}_source_file.txt"
    destination_upload_path = f"{timestamp}_destination_file.txt"

    source_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=source_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    source_client = OpenDALClient.create_from_storage_descriptor(
        source_descriptor
    )

    destination_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=destination_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    destination_client = OpenDALClient.create_from_storage_descriptor(
        destination_descriptor
    )

    # write to source bucket
    tmp_file_path = str(tmpdir / "test.txt")
    with open(tmp_file_path, "w") as local_source_file:
        local_source_file.write("Hello, world!")

    try:
        with open(tmp_file_path, "rb") as local_source_file:
            source_client.write(source_descriptor.path, local_source_file.read())

        assert source_client.read(source_descriptor.path) == b"Hello, world!"

        # copy from source to destination
        service = OpenDALService()
        service.copy(
            source_client,
            source_descriptor.path,
            destination_client,
            destination_descriptor.path,
        )

        # read from destination bucket
        assert destination_client.read(destination_descriptor.path) == b"Hello, world!"
    finally:
        source_client.delete(source_descriptor.path)
        destination_client.delete(destination_descriptor.path)

@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_download(tmpdir, opendal_operators, opendal_remote_configs):
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
        service = OpenDALService()

        download_path = str(tmpdir / "downloaded_file.txt")
        service.download(
            client,
            descriptor.path,
            download_path
        )
        downloaded_file = open(download_path, "r")
        assert downloaded_file.read() == "Hello, world!"
    finally:
        client.delete(descriptor.path)

@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_download_to_temporary_file(tmpdir, opendal_operators, opendal_remote_configs):
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
        service = OpenDALService()
        with service.download_to_temporary_file(
            client,
            descriptor.path,
            file_name="temporarily_downloaded_file.txt"
        ) as download_path:
            downloaded_file = open(download_path, "r")
            assert downloaded_file.read() == "Hello, world!"
    finally:
        client.delete(descriptor.path)