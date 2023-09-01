import sys
import argparse
import boto3


def migrate(source, target, region, fieldsToChange):
    print("Copying contents of table %s to %s in region %s" % (source, target, region))

    fieldsToChange = [field.split(',') for field in fieldsToChange]
    for field in fieldsToChange:
        print("Changing field %s to %s" % (field[0], field[1]))


    dynamo_client = boto3.client('dynamodb', region_name=region)
    dynamo_target_client = boto3.client('dynamodb', region_name=region)

    dynamo_paginator = dynamo_client.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=source,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )



    for page in dynamo_response:
        for item in page['Items']:
            for field in fieldsToChange:
                if field[0] in item:
                    item[field[1]] = item.pop(field[0])

            dynamo_target_client.put_item(
                TableName=target,
                Item=item
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table copier using boto3")
    parser.add_argument("sourceTable", help="Source table name", type=str)
    parser.add_argument("targetTable", help="Target table name", type=str)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    parser.add_argument("--fieldsToChange", help="Field name to change, and new name to use e.g. 'oldField,newField'", type=str, nargs=argparse.REMAINDER)
    args = parser.parse_args()
    migrate(args.sourceTable, args.targetTable, args.region, args.fieldsToChange)