import argparse
import boto3

from progressbar import printProgressBar

ITEMS_PER_BATCH = 25


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
            'PageSize': ITEMS_PER_BATCH,
        }
    )

    dynamodb = boto3.resource('dynamodb', region_name=region)
    # Note that itemCount is an approximate value. The value in AWS is updated every 6 hours.
    itemCount = dynamodb.Table(source).item_count
    print("Source table has approximately %s items (as of AWS last update which could be up to 6 hours out of date)" % itemCount)
    i = 0
    printProgressBar(0, itemCount, prefix = 'Progress:', suffix = 'Complete', length = 50)
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

        # AWS has a minimum of 1 and a maximum of 25 items per batch
        if len(batch) == 0:
            continue
        # This seems to smooth out the throughput and avoid throttling
        # time.sleep(0)
        try:
            dynamo_target_client.batch_write_item(
                RequestItems={
                    target: batch
                }
            )
            i += ITEMS_PER_BATCH
            printProgressBar(i, itemCount, prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            print("Failed to write batch to target table: %s" % e)
            print("Batch was: %s" % batch)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table copier using boto3")
    parser.add_argument("sourceTable", help="Source table name", type=str)
    parser.add_argument("targetTable", help="Target table name", type=str)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    parser.add_argument("--fieldsToChange", help="Field name to change, and new name to use e.g. 'oldField,newField'", type=str, nargs='*', default=[])
    args = parser.parse_args()
    migrate(args.sourceTable, args.targetTable, args.region, args.fieldsToChange)