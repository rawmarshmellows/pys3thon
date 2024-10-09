import time
import os
from pathlib import Path

import pytest

from pys3thon.opendal.service import OpenDALService
from tests.utils import cleanup

@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_s3_read_chunk_from_file_and_write_chunk_to_file_with_file_size_equal_read_chunk_size(
    tmpdir, opendal_operators
):
    # Test code for exact chunk size
    timestamp = int(time.time())
    file_name = f"{timestamp}_test_file_{1024}_bytes.txt"
    s3_file_path = f"/{file_name}"
    s3_copy_to_file_path = f"/{s3_file_path}_copy.txt"

    # Create a temporary file to upload
    local_file = Path(tmpdir) / file_name
    with open(local_file, "w") as f:
        f.write("0" * 1024)

    # Upload to S3
    s3_operator = opendal_operators["s3"]
    with open(local_file, "rb") as file_data:
        s3_operator.write(s3_file_path, file_data.read())

    # Read from S3 and write to S3 in chunks
    OpenDALService().copy(
        s3_operator,
        s3_file_path,
        s3_operator,
        s3_copy_to_file_path,
        read_chunk_size=1024,
    )

    content_length = s3_operator.stat(s3_copy_to_file_path).content_length
    assert content_length == 1024

    s3_downloaded_content = s3_operator.read(s3_copy_to_file_path)
    assert bytes(s3_downloaded_content) == bytes("0" * 1024, "utf-8")

    # Cleanup
    cleanup(s3_operator, s3_file_path)
    cleanup(s3_operator, s3_copy_to_file_path)

@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_s3_read_chunk_from_file_and_write_chunk_to_file_with_file_size_larger_than_read_chunk_size(
    tmpdir, opendal_operators
):
    # Test code for exact chunk size, we need to use 5MB read_chunk_size as this is the minimum s3 upload chunk size
    # We will test with 5MB file size and 5MB + 1 bytes file size, to test if the last chunk is read correctly here s3
    # allows the last chunk size to be less than the 5MB
    FIVE_MB = 5 * 1024**2
    timestamp = int(time.time())
    file_name = f"{timestamp}_test_file_{FIVE_MB + 1}_bytes.txt"
    s3_file_path = f"/{file_name}"
    s3_copy_to_file_path = f"/{s3_file_path}_copy.txt"

    # Create a temporary file to upload
    local_file = Path(tmpdir) / file_name
    with open(local_file, "w") as f:
        f.write("0" * (FIVE_MB + 1))

    # Upload to S3
    s3_operator = opendal_operators["s3"]
    with open(local_file, "rb") as file_data:
        s3_operator.write(s3_file_path, file_data.read())

    # Read from S3 and write to S3 in chunks
    OpenDALService().copy(
        s3_operator,
        s3_file_path,
        s3_operator,
        s3_copy_to_file_path,
        read_chunk_size=FIVE_MB**2,
    )

    content_length = s3_operator.stat(s3_copy_to_file_path).content_length
    assert content_length == FIVE_MB + 1

    s3_downloaded_content = s3_operator.read(s3_copy_to_file_path)
    assert bytes(s3_downloaded_content) == bytes("0" * (FIVE_MB + 1), "utf-8")

    # Cleanup
    cleanup(s3_operator, s3_file_path)
    cleanup(s3_operator, s3_copy_to_file_path)

@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_s3_read_chunk_from_file_and_write_chunk_to_file_with_file_size_smaller_than_read_chunk_size(
    tmpdir, opendal_operators
):
    # Test code for exact chunk size, we need to use 5MB read_chunk_size as this is the minimum s3 upload chunk size
    # We will test with 5MB file size and 5MB + 2000 bytes file size, to test if the last chunk is read correctly here
    # s3 allows the last chunk size to be less than the 5MB
    FIVE_MB = 5 * 1024**2
    timestamp = int(time.time())
    file_name = f"{timestamp}_test_file_{FIVE_MB - 1}_bytes.txt"
    s3_file_path = f"/{file_name}"
    s3_copy_to_file_path = f"/{s3_file_path}_copy.txt"

    # Create a temporary file to upload
    local_file = Path(tmpdir) / file_name
    with open(local_file, "w") as f:
        f.write("0" * (FIVE_MB - 1))

    # Upload to S3
    s3_operator = opendal_operators["s3"]
    with open(local_file, "rb") as file_data:
        s3_operator.write(s3_file_path, file_data.read())

    # Read from S3 and write to S3 in chunks
    OpenDALService().copy(
        s3_operator,
        s3_file_path,
        s3_operator,
        s3_copy_to_file_path,
        read_chunk_size=FIVE_MB,
    )

    content_length = s3_operator.stat(s3_copy_to_file_path).content_length
    assert content_length == FIVE_MB - 1

    s3_downloaded_content = s3_operator.read(s3_copy_to_file_path)
    assert bytes(s3_downloaded_content) == bytes("0" * (FIVE_MB - 1), "utf-8")

    # Cleanup
    cleanup(s3_operator, s3_file_path)
    cleanup(s3_operator, s3_copy_to_file_path)
