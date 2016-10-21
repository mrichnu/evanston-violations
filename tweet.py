#!/bin/python
from __future__ import print_function
import json
import tweepy
from secrets import *

test_json = "{\"business_id\": \"09FOOD-0323\", \"violations\": [{\"city\": \"Evanston\", \"code\": \"(36) FLOORS, WALLS AND CEILINGS: Floors; constructed, drained, clean, good repair, coving installation; dustless cleaning methods.\", \"description\": \"Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. \", \"lon\": \"-87.68451978730991\", \"business_id\": \"09FOOD-0323\", \"lat\": \"42.047793994930885\", \"state\": \"IL\", \"postal_code\": \"60201\", \"address\": \"915 DAVIS ST\", \"date\": \"20160825\", \"_id\": 4983, \"name\": \"Chef's Station\"}]}"

def test():
    print(format_tweet(json.loads(test_json)))
    print(len(format_tweet(json.loads(test_json))))

def main(event, context):
    auth = tweepy.OAuthHandler(C_KEY, C_SECRET)
    auth.set_access_token(A_TOKEN, A_TOKEN_SECRET)
    api = tweepy.API(auth)

    message = json.loads(event['Records'][0]['Sns']['Message'])

    status = format_tweet(message)
    lat, lon = get_lat_lon(message)

    api.update_status(status=status, lat=lat, long=lon)

def format_tweet(message):
    violations = message['violations']
    if 'name' in violations[0]:
        name = violations[0]['name']
        address = violations[0]['address'].title()
        name_and_addr = "{0} ({1})".format(name, address)
    else:
        name = message['business_id']
        name_and_addr = "{0}".format(message['business_id'])

    return "{0} new {1} at {2} {3}".format(
        len(violations),
        pluralize(len(violations)),
        name_and_addr,
        get_business_url(message['business_id']))

def pluralize(count):
    if count > 1:
        return 'violations'
    return 'violation'

def get_business_url(business_id):
    return "https://evanstoninspector.com/business/{0}".format(business_id)

def get_lat_lon(message):
    if 'lat' in message:
        lat = float(message['lat'])
    else:
        lat = None

    if 'lon' in message:
        lon = float(message['lon'])
    else:
        lon = None

    return (lat, lon)

if __name__ == '__main__':
    test()
