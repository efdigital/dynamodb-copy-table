import sys
import argparse
import boto3


def migrate(source, target, region, fieldsToChange):
    print("Copying contents of table %s to %s in region %s" % (source, target, region))

    if len(fieldsToChange) > 0:
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
        ConsistentRead=True,
        PaginationConfig={
            'PageSize': 25,
        }
    )



    for page in dynamo_response:
        batch = []

        for item in page['Items']:
            for field in fieldsToChange:
                if field[0] in item:
                    item[field[1]] = item.pop(field[0])
            batch.append({
                'PutRequest': {
                    'Item': item
                }
            })

        dynamo_target_client.batch_write_item(
            RequestItems={
                target: batch
            }
        )

        # temporary hack to quickly add a bunch of items to the source table for testing
        # todo move this to a separate script
        # temp = page['Items'][0]

        # for x in range(200):
        #     temp['pk']['S'] = str(x)
        #     temp['sk']['S'] = str(x)
        #     temp['TimeToExist']['S'] = str(x)

        #     dynamo_client.put_item(
        #         TableName=source,
        #         Item=temp
        #     )

    


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table copier using boto3")
    parser.add_argument("sourceTable", help="Source table name", type=str)
    parser.add_argument("targetTable", help="Target table name", type=str)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    parser.add_argument("--fieldsToChange", help="Field name to change, and new name to use e.g. 'oldField,newField'", type=str, nargs='*', default=[])
    args = parser.parse_args()
    migrate(args.sourceTable, args.targetTable, args.region, args.fieldsToChange)