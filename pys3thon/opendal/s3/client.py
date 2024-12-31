from io import BytesIO, IOBase

from smart_open import open

from ...s3.client import S3Client
from ..shared import OpenDALClient


class OpenDALS3Client(OpenDALClient):
    def __init__(
        self,
        bucket,
        region=None,
        endpoint=None,
        access_key_id=None,
        secret_access_key=None,
    ):
        """
        Initialize the OpenDALS3Client with AWS credentials and configuration.

        :param bucket: Name of the S3 bucket.
        :param region: AWS region name.
        :param endpoint: Custom S3 endpoint URL.
        :param access_key_id: AWS access key ID.
        :param secret_access_key: AWS secret access key.
        """
        self._bucket = bucket
        self._region = region
        self._endpoint = endpoint
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

        # Initialize S3 client with credentials
        credentials = None
        if access_key_id and secret_access_key:
            credentials = {
                "aws_access_key_id": access_key_id,
                "aws_secret_access_key": secret_access_key,
            }

        self.operator = S3Client(
            credentials=credentials,
            endpoint_url=endpoint,
            region_name=region,
        )

    def read(self, path: str) -> bytes:
        with self.operator.download_to_temporary_file(self._bucket, path) as temp_file:
            with open(temp_file, "rb") as f:
                return f.read()

    def presign_read(self, path: str, expiration: int) -> str:
        class PresignedUrl:
            def __init__(self, url):
                self.url = url

        return PresignedUrl(
            self.operator.generate_presigned_get_url(
                self._bucket, path, expiration=expiration
            )
        )

    def stat(self, path: str) -> dict:
        class Stat:
            def __init__(self, stat):
                self.stat = stat

            @property
            def content_length(self):
                return self.stat["ContentLength"]

        return Stat(self.operator.head_object(self._bucket, path))

    def open(self, path: str, mode: str = "rb") -> IOBase:
        return open(
            f"s3://{self._bucket}/{path}",  # noqa
            mode,
            transport_params={"client": self.operator.client},
        )

    def write(self, path: str, data: bytes):
        fileobj = BytesIO(data)
        self.operator.upload_fileobj(fileobj, self._bucket, path)

    def delete(self, path: str):
        self.operator.delete_object(self._bucket, path)

    @property
    def bucket(self):
        return self._bucket

    @property
    def region(self):
        return self._region

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def access_key_id(self):
        return self._access_key_id

    @property
    def secret_access_key(self):
        return self._secret_access_key
