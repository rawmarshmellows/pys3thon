from dataclasses import dataclass
from typing import Optional

from ..shared import JSONStorageDescriptor, StorageDescriptor


@dataclass
class S3JSONStorageDescriptor(JSONStorageDescriptor):
    bucket: str
    key: str
    aws_access_key_id: str
    encrypted_aws_secret_access_key: str
    region: Optional[str] = None
    endpoint: Optional[str] = None


@dataclass
class S3StorageDescriptor(StorageDescriptor):
    bucket: str
    aws_access_key_id: str
    encrypted_aws_secret_access_key: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region: Optional[str] = None  # can be set via environment variable
    endpoint: Optional[str] = None

    is_decrypted: bool = False

    def __post_init__(self):
        assert (
            self.encrypted_aws_secret_access_key is not None
            or self.aws_secret_access_key is not None
        )

        if self.aws_secret_access_key is not None:
            self.is_decrypted = True

    def decrypt(self, decrypt_fn):
        if self.is_decrypted:
            return
        super().decrypt()
        self.aws_secret_access_key = decrypt_fn(self.encrypted_aws_secret_access_key)
