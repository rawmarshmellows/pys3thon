import os
from functools import wraps

import opendal.exceptions as opendal_exceptions
from moto import mock_aws


def mock_aws_conditionally():
    if os.environ.get("TEST_ENV", "local") == "local":
        # Return the mocking decorator (e.g., from moto or your own implementation)
        return mock_aws
    else:
        # Return an identity decorator that does nothing
        def identity_decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return identity_decorator

