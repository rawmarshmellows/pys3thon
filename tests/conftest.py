import os

import pytest
from dotenv import find_dotenv, load_dotenv
from opendal import Operator


@pytest.fixture(scope="session", autouse=True)
def load_env():
    if os.environ.get("TEST_ENV") == "remote":
        env_file = find_dotenv(".env.test.remote")
        load_dotenv(env_file)


@pytest.fixture
def opendal_remote_configs():
    if os.environ.get("TEST_ENV") != "remote":
        return {}

    if os.environ.get("OPENDAL_S3_ENDPOINT") not in [None, ""]:
        s3_endpoint = os.environ["OPENDAL_S3_ENDPOINT"]
    else:
        s3_endpoint = None

    if os.environ.get("OPENDAL_AZBLOB_ENDPOINT") not in [None, ""]:
        azblob_endpoint = os.environ["OPENDAL_AZBLOB_ENDPOINT"]
    else:
        azblob_endpoint = None

    if os.environ.get("OPENDAL_AZBLOB_ROOT") not in [None, ""]:
        azblob_root = os.environ["OPENDAL_AZBLOB_ROOT"]
    else:
        azblob_root = None

    if os.environ.get("OPENDAL_DROPBOX_ROOT") not in [None, ""]:
        dropbox_root = os.environ["OPENDAL_DROPBOX_ROOT"]
    else:
        dropbox_root = None

    return {
        "s3": {
            "scheme": "s3",
            "root": "/",
            "bucket": os.environ["OPENDAL_S3_BUCKET"],
            "region": os.environ["OPENDAL_S3_REGION"],
            "endpoint": s3_endpoint,
            "access_key_id": os.environ["OPENDAL_S3_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["OPENDAL_S3_SECRET_ACCESS_KEY"],
        },
        "azblob": {
            "scheme": "azblob",
            "root": azblob_root,
            "container": os.environ["OPENDAL_AZBLOB_CONTAINER"],
            "endpoint": azblob_endpoint,
            "account_name": os.environ["OPENDAL_AZBLOB_ACCOUNT_NAME"],
            "account_key": os.environ["OPENDAL_AZBLOB_ACCOUNT_KEY"],
        },
        "dropbox": {
            "root": dropbox_root,
            "scheme": "dropbox",
            # "client_id": os.environ["OPENDAL_DROPBOX_CLIENT_ID"],
            # "client_secret": os.environ["OPENDAL_DROPBOX_CLIENT_SECRET"],
            "access_token": os.environ["OPENDAL_DROPBOX_ACCESS_TOKEN"],
            # "refresh_token": os.environ["OPENDAL_DROPBOX_REFRESH_TOKEN"],
        },
    }


@pytest.fixture
def opendal_operators(opendal_remote_configs):
    if os.environ.get("TEST_ENV", "local") == "local":
        return {
            "s3": Operator(
                scheme="s3",
                aws_access_key_id="fake_s3_access",
                aws_secret_access_key="fake_s3_secret",
                endpoint="http://localhost:9000",
                bucket="source-test-bucket",
                region="ap-southeast-2",
            ),
            "azblob": Operator(
                scheme="azblob",
                root="fake_azure_root",
                container="source-test-container",
                endpoint="https://fake_source_azure_account.blob.core.windows.net",
                account_name="fake_azure_account",
                account_key="fake_azure_key",
            ),
            "dropbox": Operator(
                scheme="dropbox",
                client_id="fake_dropbox_id",
                client_secret="fake_secret",
                access_token="fake_access_token",
            ),
        }
    elif os.environ["TEST_ENV"] == "remote":
        if os.environ.get("OPENDAL_S3_ENDPOINT") not in [None, ""]:
            os.environ["OPENDAL_S3_ENDPOINT"]
        else:
            pass

        if os.environ.get("OPENDAL_AZBLOB_ENDPOINT") not in [None, ""]:
            os.environ["OPENDAL_AZBLOB_ENDPOINT"]
        else:
            pass

        if os.environ.get("OPENDAL_AZBLOB_ROOT") not in [None, ""]:
            os.environ["OPENDAL_AZBLOB_ROOT"]
        else:
            pass

        if os.environ.get("OPENDAL_DROPBOX_ROOT") not in [None, ""]:
            os.environ["OPENDAL_DROPBOX_ROOT"]
        else:
            pass

        return {
            "s3": Operator(**opendal_remote_configs["s3"]),
            "azblob": Operator(**opendal_remote_configs["azblob"]),
            "dropbox": Operator(**opendal_remote_configs["dropbox"]),
        }
