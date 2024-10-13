from opendal import Operator

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
        :param access_key: AWS access key ID.
        :param secret_key: AWS secret access key.
        """
        s3_config = {
            "bucket": bucket,
        }

        if region:
            s3_config["region"] = region
        if endpoint:
            s3_config["endpoint"] = endpoint
        if access_key_id:
            s3_config["access_key_id"] = access_key_id
        if access_key_id:
            s3_config["secret_access_key"] = secret_access_key

        # Initialize opendal S3 operator
        self.operator = Operator("s3", **s3_config)
        self._bucket = bucket
        self._region = region
        self._endpoint = endpoint
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

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
