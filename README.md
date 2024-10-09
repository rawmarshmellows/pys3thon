## pys3thon

A wrapper around boto3 S3 Client

### For development
1. Start by opening the development container
2. Install the dev dependencies
```
pip install '.[dev]'
pre-commit install
pre-commit install --hook-type commit-msg # this is for commitizen to work
pre-commit install --hook-type pre-push # this is for a pytest on push to work
```

### To run the tests
```
PYTHONPATH=. ptw --run 'pytest -sv tests'
```
