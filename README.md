## pys3thon

A wrapper around boto3 S3 Client

### For development

```
pip install '.[dev]'
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg # this is for commitizen to work
pre-commit install --hook-type pre-push # this is for a pytest on push to work
```
