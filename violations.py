#!/usr/bin/python
from __future__ import print_function
import boto3
import csv
import requests
import json

TABLE_NAME = 'Violations'
SNS_TOPIC = 'arn:aws:sns:us-east-1:149274529018:evanston-violations'

all_data_url = "http://data.cityofevanston.org/rest/datastreams/{0}/data.csv"
violations_data_id = 83966
business_data_id = 83968

recent_violations_url = \
        "http://www.civicdata.com/api/3/action/datastore_search?resource_id=f230b8d9-5605-422c-a2eb-dac203f62edb&sort=_id+DESC&limit=50"

business_info_url = \
        "http://www.civicdata.com/api/3/action/datastore_search?resource_id=1c15c579-989f-43cb-b513-67b6e3971990&q={0}&limit=1"

def download_all_violations():
    r = requests.get(all_data_url.format(violations_data_id))
    return list(csv.DictReader(r.text.encode('utf-8').split("\n")))

def download_all_businesses():
    r = requests.get(all_data_url.format(business_data_id))
    return list(csv.DictReader(r.text.encode('utf-8').split("\n")))

def merge(violations, businesses):
    # make a map of business id to name
    d = {}
    cols = ['name', 'address', 'city', 'state', 'postal_code', 'LAT', 'LON']
    for b in businesses:
        d[b['business_id']] = b

    for violation in violations:
        try:
            b = d[violation['business_id']]
            for c in cols:
                violation[c] = b[c]
        except KeyError:
            pass

    return violations

def get_current_max_violation_id(dynamodb):
    resp = dynamodb.query(
        TableName = TABLE_NAME,
        Limit = 1,
        KeyConditionExpression = "#k = :key",
        ExpressionAttributeNames = {
            "#k": "_id"
        },
        ExpressionAttributeValues = {
            ":key": {'S': 'violation'}
        },
        ScanIndexForward = False
    )
    return int(resp['Items'][0]['id']['N'])

def download_recent_violations():
    r = requests.get(recent_violations_url)
    data = r.json()
    return data['result']['records']

def merge_business_info(violation):
    url = business_info_url.format(violation['business_id'])
    r = requests.get(url)
    data = r.json()
    if data['result']['records']:
        b = data['result']['records'][0]
        cols = ['name', 'address', 'city', 'state', 'postal_code', 'LAT', 'LON']
        for c in cols:
            violation[c] = b[c]

def output(violation):
    message = json.dumps(violation)
    sns = boto3.client('sns')
    resp = sns.publish(TopicArn=SNS_TOPIC, Message=message)

def get_item(violation):
    item = {
        'business_id': violation['business_id'],
        'id': int(violation['_id']),
    }
    cols = ['date', 'code', 'description', 'name', 'address', 'postal_code',
            'lat', 'lon']
    for c in cols:
        if c in violation and violation[c]:
            item[c] = violation[c]
    return item

def save(dynamodb, violation):
    item = get_item(violation)
    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item=item
    )

def main():
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')

    current_max_id = get_current_max_violation_id(dynamodb)

    violations = download_recent_violations()

    for violation in violations:
        if violation['_id'] > current_max_id:
            merge_business_info(violation)
            output(violation)
            save(dynamodb, violation)
        else:
            break

if __name__ == '__main__':
    main()
