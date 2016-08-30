#!/bin/python

import json
import tweepy
from secrets import *

test_json = "{\"city\": \"Evanston\", \"code\": \"(36) FLOORS, WALLS AND CEILINGS: Floors; constructed, drained, clean, good repair, coving installation; dustless cleaning methods.\", \"description\": \"Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. Walls and floor next to fryer and oven are soiled. Clean. \", \"LON\": \"-87.68451978730991\", \"business_id\": \"09FOOD-0323\", \"LAT\": \"42.047793994930885\", \"state\": \"IL\", \"postal_code\": \"60201\", \"address\": \"915 DAVIS ST\", \"date\": \"20160825\", \"_id\": 4983, \"name\": \"Chef's Station\"}"

def test():
    print format_tweet(json.loads(test_json))
    print len(format_tweet(json.loads(test_json)))

def main(event, context):
    auth = tweepy.OAuthHandler(C_KEY, C_SECRET)
    auth.set_access_token(A_TOKEN, A_TOKEN_SECRET)
    api = tweepy.API(auth)

    message = json.loads(event['Records'][0]['Sns']['Message'])

    status = format_tweet(message)
    lat, lon = get_lat_lon(message)

    api.update_status(status, None, lat, lon)

def format_tweet(message):
    if 'name' in message:
        name = message['name']
        address = message['address'].title()
        name_and_addr = "{0} at {1}".format(name, address)
    else:
        name = message['business_id']
        name_and_addr = "{0}".format(message['business_id'])

    maxlen = 140 - (len(name_and_addr) + 2)
    description = message['description']

    return "{0}: {1}".format(name_and_addr, description[:maxlen])

def get_lat_lon(message):
    if 'LAT' in message:
        lat = float(message['LAT'])
    else:
        lat = None

    if 'LON' in message:
        lon = float(message['LON'])
    else:
        lon = None

    return (lat, lon)


if __name__ == '__main__':
    test()
