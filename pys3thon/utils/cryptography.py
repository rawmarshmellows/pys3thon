# aes_gcm.py
import base64
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


class AES256GCM:
    def __init__(self, key: bytes):
        """
        Initialize AES-256-GCM cipher with a key.

        Args:
            key: A 32-byte key required for AES-256 encryption/decryption

        Raises:
            ValueError: If key is not exactly 32 bytes
            AssertionError: If key is None
        """
        if key is not None and len(key) != 32:
            raise ValueError("Key must be 32 bytes")
        assert key is not None, "Key must be provided"
        self.key = key

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string using AES-256-GCM
        Args:
            plaintext: String to encrypt
        Returns:
            base64 strings of (ciphertext)_(iv)_(tag) joined by underscores
        """
        iv = os.urandom(12)
        cipher = Cipher(
            algorithms.AES(self.key), modes.GCM(iv), backend=default_backend()
        )
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext.encode()) + encryptor.finalize()

        return (
            f"{base64.b64encode(ciphertext).decode('utf-8')}_"
            f"{base64.b64encode(iv).decode('utf-8')}_"
            f"{base64.b64encode(encryptor.tag).decode('utf-8')}"
        )

    def decrypt(self, encrypted: str) -> str:
        """
        Decrypt a string using AES-256-GCM
        Args:
            encrypted: base64 strings of (ciphertext)_(iv)_(tag) joined by underscores
        Returns:
            Decrypted string
        """
        try:
            ciphertext_b64, iv_b64, tag_b64 = encrypted.split("_")
            ciphertext = base64.b64decode(ciphertext_b64)
            iv = base64.b64decode(iv_b64)
            tag = base64.b64decode(tag_b64)
        except (ValueError, base64.binascii.Error) as e:
            raise ValueError("Invalid encrypted format") from e

        cipher = Cipher(
            algorithms.AES(self.key), modes.GCM(iv, tag), backend=default_backend()
        )
        decryptor = cipher.decryptor()

        try:
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            return plaintext.decode("utf-8")
        except Exception as e:
            raise ValueError("Decryption failed") from e
