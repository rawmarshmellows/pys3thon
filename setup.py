from setuptools import find_packages, setup

import pys3thon

setup(
    name="pys3thon",
    version=pys3thon.__version__,
    author="Kevin Lu",
    author_email="kevinyihchyunlu@gmail.com",
    license="MIT",
    packages=find_packages(exclude=["tests", "tests/*"]),
    install_requires=[
        "boto3>=1.24.89",
        "tqdm==4.64.0",
        "pyOpenSSL==22.1.0",
        "opendal>=0.45.2",
        "smart_open==7.1.0",
        "asgiref==3.8.1",
    ],
    extras_require={
        "dev": [
            "pytest==7.1.3",
            "pytest-mock==3.10.0",
            "pytest-only",
            "python-semantic-release==7.32.1",
            "moto>=5.0",
            "flake8==5.0.4",
            "black==22.10.0",
            "isort==5.10.1",
            "pytest-watcher==0.4.3",
            "pre-commit==2.20.0",
            "click==8.1.3",
            "python-dotenv==1.0.1",
        ]
    },
)
