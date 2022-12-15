import logging
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import unquote

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

from .utils import run_shell_command


class S3Client:
    def __init__(self, profile_name=None, endpoint_url=None, region_name=None):
        session_kwargs = {}
        client_kwargs = {}
        if profile_name is not None:
            session_kwargs["profile_name"] = profile_name
        if endpoint_url is not None:
            client_kwargs["endpoint_url"] = endpoint_url
        if region_name is not None:
            client_kwargs["region_name"] = region_name

        client_kwargs["config"] = Config(signature_version="s3v4")

        self.client = boto3.session.Session(**session_kwargs).client(
            "s3", **client_kwargs
        )
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url

    @contextmanager
    def download_to_temporary_file(self, bucket, key, show_progress=False):
        try:
            temp_file = NamedTemporaryFile()
            self.download(bucket, key, temp_file.name, show_progress)
            yield temp_file.name
        finally:
            pass

    def download(self, bucket, key, save_prefix, show_progress=False):
        save_prefix = str(save_prefix)

        if show_progress:

            def hook(t):
                def inner(bytes_amount):
                    t.update(bytes_amount)

                return inner

            file_size = float(
                self.client.head_object(Bucket=bucket, Key=unquote(key))[
                    "ContentLength"
                ]
            )
            with tqdm(
                total=file_size, unit="B", unit_scale=True, desc=save_prefix
            ) as t:
                self.client.download_file(
                    bucket, unquote(key), save_prefix, Callback=hook(t)
                )
        else:
            self.client.download_file(bucket, unquote(key), save_prefix)

    def upload_file(self, path, bucket, key):
        self.client.upload_file(path, bucket, key)

    def upload_fileobj(self, fileobj, bucket, key):
        self.client.upload_fileobj(fileobj, bucket, key)

    def copy(self, source_bucket, source_key, dst_bucket, dst_key):
        self.client.copy(
            {"Bucket": source_bucket, "Key": source_key}, dst_bucket, dst_key
        )

    def check_if_exists_in_s3(self, bucket, key):
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def get_object_type(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key)["ContentType"]

    def get_object_size(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    def head_object(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key)

    def get_object_storage_class(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key).get(
            "StorageClass", "STANDARD"
        )

    def get_s3_keys(self, bucket, prefix=None, delimiter=None):
        keys = []
        try:
            for resp in self._construct_s3_paginator(
                bucket, prefix, delimiter
            ):
                keys.extend([c["Key"] for c in resp["Contents"]])
        except KeyError:
            pass
        return keys

    def get_top_level_bucket_keys(self, bucket):
        keys = []
        for prefix in self._construct_s3_paginator(
            bucket, delimiter="/"
        ).search("CommonPrefixes"):
            keys.append(prefix.get("Prefix"))
        return keys

    def get_directories_for_bucket_with_prefix_recursively(
        self, bucket, prefix=None, delimiter="/", log_every=100
    ):
        prefixes_parsed = 0
        stack = [prefix]
        all_prefixes = []
        while len(stack) > 0:
            prefix = stack.pop()
            if prefix is None:
                sub_prefixes = self.get_directories_for_bucket_with_prefix(
                    bucket, delimiter=delimiter
                )
            else:
                all_prefixes.append(prefix)
                sub_prefixes = self.get_directories_for_bucket_with_prefix(
                    bucket, prefix, delimiter
                )
            stack.extend(sub_prefixes)
            prefixes_parsed += 1
            if prefixes_parsed % log_every == 0:
                print(f"Parsed: {prefixes_parsed}")
        return all_prefixes

    def get_directories_for_bucket_with_prefix(
        self, bucket, prefix=None, delimiter="/"
    ):
        keys = []
        for prefix in self._construct_s3_paginator(
            bucket, prefix=prefix, delimiter=delimiter
        ).search("CommonPrefixes"):
            if prefix is None:
                continue
            keys.append(prefix.get("Prefix"))
        return keys

    def get_files_for_bucket_with_prefix(self, bucket, prefix, delimiter="/"):
        all_file_contents = []
        for contents in self._construct_s3_paginator(
            bucket, prefix=prefix, delimiter=delimiter
        ).search("Contents"):
            if contents is None:
                continue
            if contents["Key"] == prefix:
                continue
            all_file_contents.append(contents)
        return all_file_contents

    def generate_presigned_get_url(self, bucket, key, expiration=3600):
        """Generate a presigned URL to share an S3 object
        :param bucket_name: string
        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain
        valid
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 object
        try:
            response = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL
        return response

    def create_presigned_post_url(
        self, bucket, key, fields=None, conditions=None, expiration=3600
    ):
        """Generate a presigned URL S3 POST request to upload a file

        :param bucket_name: string
        :param object_name: string
        :param fields: Dictionary of prefilled form fields
        :param conditions: List of conditions to include in the policy
        :param expiration: Time in seconds for the presigned URL to remain
        valid
        :return: Dictionary with the following keys:
            url: URL to post to
            fields: Dictionary of form fields and values to submit with the
            POST
        :return: None if error.
        """

        # Generate a presigned S3 POST URL
        try:
            response = self.client.generate_presigned_post(
                bucket,
                key,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL and required fields
        return response

    def sync_folder(
        self,
        source_bucket,
        source_prefix,
        destination_bucket,
        destination_prefix,
    ):
        run_shell_command(
            " ".join(
                [
                    "aws",
                    "s3",
                    "sync",
                    f'"s3://{source_bucket}/{source_prefix}"',
                    f'"s3://{destination_bucket}/{destination_prefix}"',
                ]
            )
        )

    def sync_folder_from_local_to_s3(
        self,
        local_folder_path,
        destination_bucket,
        destination_prefix,
    ):
        run_shell_command(
            " ".join(
                [
                    "aws",
                    "s3",
                    "sync",
                    local_folder_path,
                    f'"s3://{destination_bucket}/{destination_prefix}"',
                ]
            )
        )

    def upload_directory(self, directory_path, bucket, prefix):
        # Convert the local path to a Path object
        directory_path = Path(directory_path)

        # Walk through the local directory and upload each file
        for child in directory_path.rglob("*"):
            # Check if the child is a file
            if child.is_file():
                # Compute the full S3 key of the file
                s3_key = str(Path(prefix) / child.relative_to(directory_path))

                # Upload the file to S3
                self.upload_file(str(child), bucket, s3_key)

    def _construct_s3_paginator(self, bucket, prefix=None, delimiter=None):
        kwargs = {"Bucket": bucket}
        s3_paginator = self.client.get_paginator("list_objects_v2")

        if prefix is not None:
            kwargs.update({"Prefix": prefix})

        if delimiter is not None:
            kwargs.update({"Delimiter": delimiter})
        return s3_paginator.paginate(**kwargs)
