# dynamodb-copy-table-python3

A simple Python scrip to copy data from one table to another in the same AWS Region
This was tested using `python 3.12` and `boto3@1.34.109`

---

### Requirements

This script assumes a table is already created in the destination account.

- Python 3.12
- boto3 (`pip install boto3`)

```
# Pyenv v3.12
brew install pyenv
pyenv install 3.12
echo "3.12" > ~/.pyenv/version
eval "$(pyenv init -)"
python --version
```

Setup `venv`:

```
# Setup venv
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Usage

A simple usage example:

```shell
$ python dynamodb-copy-table.py src_table dst_table
```

### Arguments:

`region`: the default region is `us-west-2`

```shell
export AWS_PROFILE=

python dynamodb-copy-table.py src_table dst_table --region=cn-north-1
```

### References

- [Import and Export DynamoDB Data using AWS Data Pipeline](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb.html)
