from copy import deepcopy
from math import ceil
import argparse
from uuid import uuid4
import boto3


def migrate(table, primaryKey, secondaryKey, quantity, region):
    print("Generating records for table %s in region %s" % (table, region))

    dynamo_client = boto3.client('dynamodb', region_name=region)

    dynamo_paginator = dynamo_client.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=table,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True,
        PaginationConfig={
            'max_items': 1,
        }
    )

    templateItem = {}
    for page in dynamo_response:
        templateItem = page['Items'][0]

    for x in range(ceil(quantity/25)):
        batch = []
        for y in range(25):
            generatedItem = deepcopy(templateItem)
            generatedItem[primaryKey]['S'] = str(uuid4())
            generatedItem[secondaryKey]['S'] = str(uuid4())
            batch.append({
                'PutRequest': {
                    'Item': generatedItem
                }
            })
            quantity -= 1
            if quantity == 0:
                break

        dynamo_client.batch_write_item(
            RequestItems={
                table: batch
            }
        )

    


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table record generation tool using boto3")
    parser.add_argument("table", help="The table to generate records in. This must have at least one existing record to use as a template", type=str)
    parser.add_argument("primaryKey", help="Primary key of the table", type=str)
    parser.add_argument("secondaryKey", help="Secondary key of the table", type=str)
    parser.add_argument("quantity", help="Number of records to generate", type=int)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    args = parser.parse_args()
    migrate(args.table, args.primaryKey, args.secondaryKey, args.quantity, args.region)