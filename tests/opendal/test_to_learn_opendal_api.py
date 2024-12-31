import os
from pathlib import Path

import pytest


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
@pytest.mark.skipif(
    os.path.exists("/.dockerenv"), reason="opendal doesn't work when it's in docker"
)
def test_opendal_s3_operator(tmpdir, opendal_operators):
    # Create a temporary file to upload
    local_file = Path(tmpdir) / "test_file.txt"
    with open(local_file, "w") as f:
        f.write("This is a test file for S3")

    # Upload to S3
    s3_operator = opendal_operators["s3"]
    with open(local_file, "rb") as file_data:
        s3_operator.write("/test_file_s3.txt", file_data.read())

    # S3 read verification
    s3_downloaded_content = s3_operator.read("/test_file_s3.txt")
    assert s3_downloaded_content == b"This is a test file for S3"


@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
@pytest.mark.skipif(
    os.path.exists("/.dockerenv"), reason="opendal doesn't work when it's in docker"
)
def test_opendal_azure_operator(tmpdir, opendal_operators):
    # Create a temporary file to upload
    local_file = Path(tmpdir) / "test_file.txt"
    with open(local_file, "w") as f:
        f.write("This is a test file for Azure")

    # Upload to Azure Blob
    azblob_operator = opendal_operators["azblob"]
    with open(local_file, "rb") as file_data:
        azblob_operator.write("/test_file_azure.txt", file_data.read())

    # Azure Blob read verification
    azblob_downloaded_content = azblob_operator.read("/test_file_azure.txt")
    assert azblob_downloaded_content == b"This is a test file for Azure"


@pytest.mark.skip(
    reason="Need to figure out how to integrate refresh token with dropbox"
)
@pytest.mark.skipif(
    os.environ.get("TEST_ENV") != "remote", reason="requires TEST_ENV=remote"
)
def test_opendal_dropbox_operator(tmpdir, opendal_operators):
    # Create a temporary file to upload
    local_file = Path(tmpdir) / "test_file.txt"
    with open(local_file, "w") as f:
        f.write("This is a test file for Dropbox")

    # Upload to Dropbox
    dropbox_operator = opendal_operators["dropbox"]
    with open(local_file, "rb") as file_data:
        dropbox_operator.write("/test_file_dropboxs.txt", file_data.read())

    # Dropbox read verification
    dropbox_downloaded_content = dropbox_operator.read("/test_file_dropboxs.txt")
    assert dropbox_downloaded_content == b"This is a test file for Dropbox."
