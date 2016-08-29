#!/usr/bin/python
from __future__ import print_function
import boto3
import csv
import requests

url = "http://data.cityofevanston.org/rest/datastreams/{0}/data.csv" 
violations_data_id = 83966
business_data_id = 83968

def download_all_violations():
    r = requests.get(url.format(violations_data_id))
    return list(csv.DictReader(r.text.encode('utf-8').split("\n")))

def download_all_businesses():
    r = requests.get(url.format(business_data_id))
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
                d[c] = b[c]
        except KeyError:
            pass

    return violations

def in_db(dynamodb, violation):
    table = dynamodb.Table('Violations')
    resp = table.get_item(Key={'id': violation['_id']})
    return 'Item' in resp

def output(violation):
    if 'name' in violation:
        name = violation['name']
    else:
        name = violation['business_id']
    print("{0}: {1} on {2}".format(name, violation['code'], violation['date']))

def save(dynamodb, violation):
    table = dynamodb.Table('Violations')
    table.put_item(Item={
        'id': violation['_id'],
        'name': violation.get('name', None),
        'address': violation.get('address', None),
        'postal_code': violation.get('postal_code', None),
        'lat': violation.get('LAT', None),
        'lon': violation.get('LON', None),
        'business_id': violation['business_id'],
        'date': violation['date'],
        'code': violation['code'],
        'description': violation['description'],
        })

def main():
    violations = download_all_violations()
    businesses = download_all_businesses()
    violations = merge(violations, businesses)

    dynamodb = boto3.resource("dynamodb", region_name='us-east-1',
            endpoint_url="http://localhost:8000")

    count = 0
    for violation in violations:
        if not in_db(dynamodb, violation):
            output(violation)
            save(dynamodb, violation)
        count += 1
        if count >= 10:
            break

if __name__ == '__main__':
    main()
