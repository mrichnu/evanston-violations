#!/usr/bin/python
from __future__ import print_function
import boto3
from boto3.dynamodb.conditions import Key, Attr
from collections import defaultdict
import csv
import requests
import json

REGION = 'us-east-1'
TABLE_NAME = 'ev-violations'
BUSINESS_TABLE_NAME = 'ev-businesses'
SNS_TOPIC = 'arn:aws:sns:us-east-1:149274529018:evanston-violations'

all_business_url = 'http://www.civicdata.com/api/3/action/datastore_search?resource_id=1c15c579-989f-43cb-b513-67b6e3971990&limit=1000'
all_violations_url = 'http://www.civicdata.com/api/3/action/datastore_search?resource_id=f230b8d9-5605-422c-a2eb-dac203f62edb&sort=_id+DESC&limit=50000'

recent_violations_url = \
        "http://www.civicdata.com/api/3/action/datastore_search?resource_id=f230b8d9-5605-422c-a2eb-dac203f62edb&sort=_id+DESC&limit=50"

business_info_url = \
        "http://www.civicdata.com/api/3/action/datastore_search?resource_id=1c15c579-989f-43cb-b513-67b6e3971990&q={0}&limit=1"

def download_all_violations():
    r = requests.get(all_violations_url, verify=False)
    return r.json()['result']['records']

def download_all_businesses():
    r = requests.get(all_business_url, verify=False)
    return r.json()['result']['records']

def merge(violations, businesses):
    # make a map of business id to name
    d = {}
    cols = ['name', 'address', 'city', 'state', 'postal_code', 'LON', 'LAT']
    for b in businesses:
        d[b['business_id']] = b

    for violation in violations:
        try:
            b = d[violation['business_id']]
            for c in cols:
                violation[c.lower()] = b[c]
        except KeyError:
            pass

    return violations

def get_current_max_violation_id(dynamodb):
    table = dynamodb.Table(TABLE_NAME)
    resp = table.query(
        IndexName='idx_violation_id',
        KeyConditionExpression=Key('type').eq('violation'),
        Limit=1,
        ScanIndexForward=False,
    )
    return int(resp['Items'][0]['id'])

def get_or_update_business(dynamodb, business_id):
    table = dynamodb.Table(BUSINESS_TABLE_NAME)
    resp = table.get_item(Key={'business_id': business_id})
    try:
        return resp['Item']
    except KeyError:
        # not found
        pass

    url = business_info_url.format(business_id)
    r = requests.get(url)
    data = r.json()
    if data['result']['records']:
        business = get_business_item(data['result']['records'][0])
        table.put_item(Item=business)

    return business

def download_recent_violations():
    r = requests.get(recent_violations_url)
    data = r.json()
    return data['result']['records']

def output(business_id, violations):
    message = json.dumps({'business_id': business_id, 'violations': violations})
    sns = boto3.client('sns')
    resp = sns.publish(TopicArn=SNS_TOPIC, Message=message)

def get_item(violation):
    item = {
        'business_id': violation['business_id'],
        'id': int(violation['_id']),
        'type': 'violation',
    }
    cols = ['date', 'code', 'description', 'name', 'address', 'postal_code',
            'lat', 'lon']
    for c in cols:
        if c in violation and violation[c]:
            item[c] = violation[c]

    return item

def get_business_item(business):
    item = {}

    cols = ['business_id', 'name', 'address', 'city', 'state', 'postal_code', 'LAT', 'LON']

    for c in cols:
        if c in business and business[c]:
            item[c.lower()] = business[c]
            if c == 'name':
                item['name_normalized'] = item['name'].lower().strip()

    return item

def save(dynamodb, violation):
    item = get_item(violation)
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)

def main():
    dynamodb = boto3.resource('dynamodb', region_name=REGION)

    current_max_id = get_current_max_violation_id(dynamodb)

    violations = download_recent_violations()

    unseen = defaultdict(list)

    for violation in violations:
        if violation['_id'] > current_max_id:
            business = get_or_update_business(dynamodb, violation['business_id'])
            violation.update(business)
            unseen[violation['business_id']].append(violation)
            save(dynamodb, violation)
        else:
            break
        
    for business_id, violations in unseen.iteritems():
        output(business_id, violations)

if __name__ == '__main__':
    main()
