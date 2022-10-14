from setuptools import find_packages, setup

import pys3thon

setup(
    name="pys3thon",
    version=pys3thon.__version__,
    author="Kevin Lu",
    author_email="kevinyihchyunlu@gmail.com",
    license="MIT",
    packages=find_packages(exclude=["tests", "tests/*"]),
    install_requires=["boto3==1.24.89", "tqdm==4.64.0", "pyOpenSSL==22.1.0"],
    extras_require={
        "dev": [
            "pytest==7.1.3",
            "pytest-mock==3.10.0",
            "python-semantic-release==7.32.1",
            "moto==4.0.7",
            "flake8==5.0.4",
            "black==22.10.0",
            "isort==5.10.1",
            "pytest-watch==4.2.0",
            "pre-commit==2.20.0",
            "click==8.1.3",
        ]
    },
)
