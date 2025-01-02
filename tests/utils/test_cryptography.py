import os

import pytest

from pys3thon.utils import AES256GCM


def test_known_key():
    key = b"0" * 32  # 32 bytes of zeros
    cipher = AES256GCM(key)
    plaintext = "Test message"
    encrypted = cipher.encrypt(plaintext)
    decrypted = cipher.decrypt(encrypted)
    assert decrypted == plaintext


def test_key_reuse():
    key = os.urandom(32)
    cipher1 = AES256GCM(key)
    cipher2 = AES256GCM(key)

    plaintext = "Secret message"
    encrypted = cipher1.encrypt(plaintext)
    decrypted = cipher2.decrypt(encrypted)
    assert decrypted == plaintext


def test_invalid_key_size():
    with pytest.raises(ValueError):
        AES256GCM(b"too-short")


def test_invalid_encrypted_format():
    key = os.urandom(32)
    cipher = AES256GCM(key)
    with pytest.raises(ValueError):
        cipher.decrypt("invalid_format")


def test_tampered_ciphertext():
    key = os.urandom(32)
    cipher = AES256GCM(key)
    plaintext = "Original message"
    encrypted = cipher.encrypt(plaintext)

    tampered = encrypted[:5] + "X" + encrypted[6:]  # Modify the 6th character

    with pytest.raises(ValueError):
        cipher.decrypt(tampered)


def test_different_key_fails():
    key1 = os.urandom(32)
    key2 = os.urandom(32)
    cipher1 = AES256GCM(key1)
    cipher2 = AES256GCM(key2)  # Different key

    plaintext = "Secret message"
    encrypted = cipher1.encrypt(plaintext)

    with pytest.raises(ValueError):
        cipher2.decrypt(encrypted)


def test_unicode_handling():
    key = os.urandom(32)
    cipher = AES256GCM(key)
    plaintext = "Hello, 世界! 🌍"
    encrypted = cipher.encrypt(plaintext)
    decrypted = cipher.decrypt(encrypted)
    assert decrypted == plaintext


def test_empty_string():
    key = os.urandom(32)
    cipher = AES256GCM(key)
    plaintext = ""
    encrypted = cipher.encrypt(plaintext)
    decrypted = cipher.decrypt(encrypted)
    assert decrypted == plaintext
