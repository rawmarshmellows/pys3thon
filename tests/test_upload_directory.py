from pathlib import Path

import boto3
from moto import mock_s3

from pys3thon.client import S3Client


@mock_s3
def test_upload_directory(tmpdir):
    tmpdir = Path(tmpdir)
    conn = boto3.resource("s3", region_name="ap-southeast-2")
    conn.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client = S3Client()

    output_dir = tmpdir / "output"
    (output_dir / "nested1" / "nested2").mkdir(parents=True, exist_ok=True)

    with open(output_dir / "file_1.txt", "w") as file1:
        # Write some content to the file
        file1.write("Hello, world!")

    with open(output_dir / "nested1" / "file_2.txt", "w") as file2:
        # Write some content to the file
        file2.write("Hello, world!")

    with open(output_dir / "nested1" / "nested2" / "file_3.txt", "w") as file3:
        # Write some content to the file
        file3.write("Hello, world!")

    s3_client.upload_directory(str(tmpdir), "test-bucket", "test-directory-1/")

    assert s3_client.check_if_exists_in_s3(
        bucket="test-bucket", key="test-directory-1/output/file_1.txt"
    )

    assert s3_client.check_if_exists_in_s3(
        bucket="test-bucket", key="test-directory-1/output/nested1/file_2.txt"
    )

    assert s3_client.check_if_exists_in_s3(
        bucket="test-bucket",
        key="test-directory-1/output/nested1/nested2/file_3.txt",
    )
