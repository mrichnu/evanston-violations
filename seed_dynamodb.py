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
        resp = dynamodb.delete_table(TableName=TABLE_NAME)
        print("Table '{0}' deleted.".format(TABLE_NAME))
        time.sleep(10)
    except botocore.exceptions.ClientError:
        print("Table '{0}' not found.".format(TABLE_NAME))

def create_table(dynamodb):
    resp = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': '_id', 'KeyType': 'HASH'},
                {'AttributeName': 'id', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': '_id', 'AttributeType': 'S'},
                {'AttributeName': 'id', 'AttributeType': 'N'},
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits': 10 
            }
    )
    print("Table '{0}' created.".format(TABLE_NAME))
    time.sleep(15)

def seed_table(dynamodb):
    all_violations = violations.download_all_violations()
    all_businesses = violations.download_all_businesses()
    all_violations = violations.merge(all_violations, all_businesses)

    count = 0
    for violation in all_violations:
        
        item = violations.get_item(violation)

        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item=item
        )
        count += 1
        if count % 10 == 0:
            print("Inserted {0} records".format(count))

def reset_provisioned_throughput(dynamodb):
    resp = dynamodb.update_table(
        TableName=TABLE_NAME,
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1 
        }
    )
    print("Reset provisioned throughput for table.")

def main():
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')

    drop_table(dynamodb)
    create_table(dynamodb)
    seed_table(dynamodb)
    reset_provisioned_throughput(dynamodb)

if __name__ == '__main__':
    main()
