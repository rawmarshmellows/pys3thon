import os
import time

import pytest

from pys3thon.opendal.s3.descriptor import S3StorageDescriptor
from pys3thon.opendal.service import OpenDALService
from pys3thon.opendal.shared import OpenDALClient


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_copy_s3_multiple_chunks(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    source_upload_path = f"{timestamp}_source_file.txt"
    destination_upload_path = f"{timestamp}_destination_file.txt"

    source_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=source_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    source_client = OpenDALClient.create_from_storage_descriptor(source_descriptor)

    destination_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=destination_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
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
            read_chunk_size=1,
        )

        # read from destination bucket
        assert destination_client.read(destination_descriptor.path) == b"Hello, world!"
    except Exception as e:
        print(f"Error: {e}")
        raise e
    finally:
        source_client.delete(source_descriptor.path)
        destination_client.delete(destination_descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_copy_multi_chunk_s3(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    source_upload_path = f"/{timestamp}_source_large_file.txt"
    destination_upload_path = f"/{timestamp}_destination_large_file.txt"

    source_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=source_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    source_client = OpenDALClient.create_from_storage_descriptor(source_descriptor)

    destination_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=destination_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    destination_client = OpenDALClient.create_from_storage_descriptor(
        destination_descriptor
    )

    # Create 12MB of random data
    content = os.urandom(12 * 1024 * 1024)  # 12MB of random bytes
    try:
        source_client.write(source_descriptor.path, content)
        assert source_client.read(source_descriptor.path) == content

        # copy from source to destination with 5MB chunk size
        service = OpenDALService()
        service.copy(
            source_client,
            source_descriptor.path,
            destination_client,
            destination_descriptor.path,
            read_chunk_size=5 * 1024 * 1024,  # 5MB chunks
        )

        # verify destination content
        assert destination_client.read(destination_descriptor.path) == content
    finally:
        source_client.delete(source_descriptor.path)
        destination_client.delete(destination_descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_copy_single_chunk_s3(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    source_upload_path = f"/{timestamp}_source_file.txt"
    destination_upload_path = f"/{timestamp}_destination_file.txt"

    source_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=source_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    source_client = OpenDALClient.create_from_storage_descriptor(source_descriptor)

    destination_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=destination_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    destination_client = OpenDALClient.create_from_storage_descriptor(
        destination_descriptor
    )

    # write small content that fits in a single chunk
    content = b"Small content for single chunk test"
    try:
        source_client.write(source_descriptor.path, content)
        assert source_client.read(source_descriptor.path) == content

        # copy from source to destination with chunk size larger than content
        service = OpenDALService()
        service.copy(
            source_client,
            source_descriptor.path,
            destination_client,
            destination_descriptor.path,
            read_chunk_size=len(content) + 1024,  # ensure single chunk
        )

        # verify destination content
        assert destination_client.read(destination_descriptor.path) == content
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
        service.download(client, descriptor.path, download_path)
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
            client, descriptor.path, file_name="temporarily_downloaded_file.txt"
        ) as download_path:
            downloaded_file = open(download_path, "r")
            assert downloaded_file.read() == "Hello, world!"
    finally:
        client.delete(descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_copy_with_invalid_source_path(
    tmpdir, opendal_operators, opendal_remote_configs
):
    timestamp = int(time.time())
    source_upload_path = f"/{timestamp}_nonexistent_source.txt"
    destination_upload_path = f"/{timestamp}_destination.txt"

    source_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=source_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    source_client = OpenDALClient.create_from_storage_descriptor(source_descriptor)

    destination_descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=destination_upload_path,
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
        region=opendal_remote_configs["s3"]["region"],
    )
    destination_client = OpenDALClient.create_from_storage_descriptor(
        destination_descriptor
    )

    service = OpenDALService()
    with pytest.raises(Exception):  # Replace with specific exception if known
        service.copy(
            source_client,
            source_descriptor.path,
            destination_client,
            destination_descriptor.path,
        )


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_download_large_file(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    upload_path = f"/{timestamp}_large_file.txt"

    descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=upload_path,
        region=opendal_remote_configs["s3"]["region"],
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    client = OpenDALClient.create_from_storage_descriptor(descriptor)

    # Create 12MB of random data
    content = os.urandom(12 * 1024 * 1024)

    try:
        client.write(descriptor.path, content)
        service = OpenDALService()

        download_path = str(tmpdir / "downloaded_large_file.txt")
        service.download(client, descriptor.path, download_path)

        with open(download_path, "rb") as downloaded_file:
            assert downloaded_file.read() == content
    finally:
        client.delete(descriptor.path)


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_download_nonexistent_file(tmpdir, opendal_operators, opendal_remote_configs):
    timestamp = int(time.time())
    nonexistent_path = f"/{timestamp}_nonexistent.txt"

    descriptor = S3StorageDescriptor(
        bucket=opendal_remote_configs["s3"]["bucket"],
        path=nonexistent_path,
        region=opendal_remote_configs["s3"]["region"],
        aws_access_key_id=opendal_remote_configs["s3"]["access_key_id"],
        aws_secret_access_key=opendal_remote_configs["s3"]["secret_access_key"],
    )
    client = OpenDALClient.create_from_storage_descriptor(descriptor)
    service = OpenDALService()

    download_path = str(tmpdir / "should_not_exist.txt")
    with pytest.raises(Exception):  # Replace with specific exception if known
        service.download(client, descriptor.path, download_path)
