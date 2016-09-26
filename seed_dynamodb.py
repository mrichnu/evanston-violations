from __future__ import print_function
import boto3
import botocore
import csv
import time
import requests
import violations

TABLE_NAME = 'Violations'

def drop_table(dynamodb):
    try:
        table = dynamodb.Table(TABLE_NAME)
        resp = table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=TABLE_NAME)
        print("Table '{0}' deleted.".format(TABLE_NAME))
    except botocore.exceptions.ClientError:
        print("Table '{0}' not found.".format(TABLE_NAME))

def create_table(dynamodb):
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
                'ReadCapacityUnits': 2,
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
                        'ReadCapacityUnits': 2,
                        'WriteCapacityUnits': 25 
                    },
                }
            ],
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
    print("Table '{0}' created.".format(TABLE_NAME))

def seed_table(dynamodb):
    all_violations = violations.download_all_violations()
    all_businesses = violations.download_all_businesses()
    all_violations = violations.merge(all_violations, all_businesses)

    count = 0

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
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 5
        },
        GlobalSecondaryIndexUpdates=[
            {
                'Update': {
                    'IndexName': 'idx_violation_id',
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 2,
                        'WriteCapacityUnits': 5 
                    },
                },
            },
        ],
    )
    print("Provisioned throughput successfully reset.")

def main():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    drop_table(dynamodb)
    create_table(dynamodb)
    seed_table(dynamodb)
    reset_provisioned_throughput(dynamodb.meta.client)

if __name__ == '__main__':
    main()
