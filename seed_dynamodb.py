from __future__ import print_function
import boto3
import botocore
import time
import requests
import violations

TABLE_NAME = 'ev-violations'
BUSINESS_TABLE_NAME = 'ev-businesses'

def drop_table(dynamodb, table_name):
    """
    Drop the given dynamodb table, waiting until the table is confirmed deleted to return.
    """
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
        print("Table '{0}' deleted.".format(table_name))
    except botocore.exceptions.ClientError:
        print("Table '{0}' not found.".format(table_name))

def create_violations_table(dynamodb):
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {'AttributeName': 'business_id', 'KeyType': 'HASH'},
            {'AttributeName': 'id', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'business_id', 'AttributeType': 'S'},
            {'AttributeName': 'type', 'AttributeType': 'S'},
            {'AttributeName': 'id', 'AttributeType': 'N'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 25
        },
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'idx_violation_id',
                'KeySchema': [
                    {'AttributeName': 'type', 'KeyType': 'HASH'},
                    {'AttributeName': 'id', 'KeyType': 'RANGE'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 25
                },
            }
        ],
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
    print("Table '{0}' created.".format(TABLE_NAME))

def create_businesses_table(dynamodb):
    table = dynamodb.create_table(
        TableName=BUSINESS_TABLE_NAME,
        KeySchema=[
            {'AttributeName': 'business_id', 'KeyType': 'HASH'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'business_id', 'AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 25
        },
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=BUSINESS_TABLE_NAME)
    print("Table '{0}' created.".format(BUSINESS_TABLE_NAME))

def seed_table(dynamodb):
    all_violations = violations.download_all_violations()
    all_businesses = violations.download_all_businesses()
    all_violations = violations.merge(all_violations, all_businesses)

    count = 0

    with dynamodb.Table(BUSINESS_TABLE_NAME).batch_writer() as batch:
        for business in all_businesses:
            item = violations.get_business_item(business)
            batch.put_item(Item=item)

            count += 1
            if count % 50 == 0:
                print("Inserted {0} businesses".format(count))

    with dynamodb.Table(TABLE_NAME).batch_writer() as batch:
        for violation in all_violations:
            item = violations.get_item(violation)
            batch.put_item(Item=item)

            count += 1
            if count % 50 == 0:
                print("Inserted {0} records".format(count))

def reset_provisioned_throughput(client):
    client.update_table(
        TableName=TABLE_NAME,
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 5
        },
        GlobalSecondaryIndexUpdates=[
            {
                'Update': {
                    'IndexName': 'idx_violation_id',
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    },
                },
            },
        ],
    )
    client.update_table(
        TableName=BUSINESS_TABLE_NAME,
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 5
        },
    )
    print("Provisioned throughput successfully reset.")

def main():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    drop_table(dynamodb, TABLE_NAME)
    drop_table(dynamodb, BUSINESS_TABLE_NAME)
    create_violations_table(dynamodb)
    create_businesses_table(dynamodb)
    seed_table(dynamodb)
    reset_provisioned_throughput(dynamodb.meta.client)

if __name__ == '__main__':
    main()
