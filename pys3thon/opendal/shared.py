from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

from asgiref.sync import async_to_sync
import opendal.exceptions as opendal_exceptions


class StorageScheme(Enum):
    S3 = "S3"
    AZURE_BLOB = "AzureBlob"
    DROPBOX = "Dropbox"


@dataclass
class JSONStorageDescriptor:
    @staticmethod
    def create_from_json_descriptor(json_descriptor):
        from .s3.descriptor import S3JSONStorageDescriptor

        if json_descriptor["scheme"] == StorageScheme.S3.value:
            return S3JSONStorageDescriptor(
                bucket=json_descriptor["bucket"],
                key=json_descriptor["key"],
                aws_access_key_id=json_descriptor["awsAccessKeyId"],
                encrypted_aws_secret_access_key=json_descriptor[
                    "encryptedAwsSecretAccessKey"
                ],
                region=json_descriptor.get("region"),
                endpoint=json_descriptor.get("endpoint"),
            )
        else:
            raise ValueError(f"Unsupported storage scheme: {json_descriptor['scheme']}")


@dataclass
class StorageDescriptor(ABC):
    path: str

    @abstractmethod
    def decrypt(self):
        self.is_decrypted = True

    @staticmethod
    def create_from_json_storage_descriptor(
        json_descriptor: JSONStorageDescriptor,
    ):
        from .s3.descriptor import S3JSONStorageDescriptor, S3StorageDescriptor

        if isinstance(json_descriptor, S3JSONStorageDescriptor):
            return S3StorageDescriptor(
                bucket=json_descriptor.bucket,
                path=json_descriptor.key,
                aws_access_key_id=json_descriptor.aws_access_key_id,
                encrypted_aws_secret_access_key=json_descriptor.encrypted_aws_secret_access_key,
                region=json_descriptor.region,
                endpoint=json_descriptor.endpoint,
            )
        else:
            raise ValueError(f"Unsupported storage scheme: {json_descriptor.scheme}")


class OpenDALClient(ABC):
    @staticmethod
    def create_from_storage_descriptor(
        storage_descriptor: StorageDescriptor,
    ):
        from .s3.client import OpenDALS3Client
        from .s3.descriptor import S3StorageDescriptor

        if not storage_descriptor.is_decrypted:
            raise ValueError(
                "Storage descriptor must be decrypted before creating OpenDAL client."
            )
        if isinstance(storage_descriptor, S3StorageDescriptor):
            return OpenDALS3Client(
                bucket=storage_descriptor.bucket,
                region=storage_descriptor.region,
                endpoint=storage_descriptor.endpoint,
                access_key_id=storage_descriptor.aws_access_key_id,
                secret_access_key=storage_descriptor.aws_secret_access_key,
            )
        else:
            raise ValueError(f"Unsupported storage type: {storage_descriptor.scheme}")

    def read(self, path: str):
        return self.operator.read(path)
    
    def presign_read(self, path: str, expiration: int):
        async def get_presigned_url():
            presigned_read = await self.operator.to_async_operator().presign_read(path, expiration)
            return presigned_read
        return async_to_sync(get_presigned_url)()

    def stat(self, path: str):
        return self.operator.stat(path)

    def open(self, path: str, mode: str):
        return self.operator.open(path, mode)

    def write(self, path: str, data: bytes):
        self.operator.write(path, data)

    def delete(self, path: str):
        try:
            self.operator.delete(path)
            self.stat(path)
        except opendal_exceptions.NotFound:
            # need to explicity catch the failed read as OpenDAL doesn't error when delete fails
            pass
        except Exception as e:
            print(e)
            assert False, f"{path} not deleted"

