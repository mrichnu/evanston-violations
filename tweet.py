#!/bin/python

import json
import tweepy
from secrets import *

def main(event, context):
    auth = tweepy.OAuthHandler(C_KEY, C_SECRET)
    auth.set_access_token(A_TOKEN, A_TOKEN_SECRET)
    api = tweepy.API(auth)

    message = json.loads(event['Records'][0]['Sns']['Message'])

    api.update_status(message, lat=message['LAT'], lon=message['LON']) 

def format_tweet(message):
    if 'name' in message:
        name = name
        address = message['address'].title()
        name_and_addr = "{0} at {1}".format(name, address)
    else:
        name = message['business_id']
        name_and_addr = "{0}".format(message['business_id'])

    maxlen = 140 - (len(name_and_addr) + 2)
    description = message['description']

    return "{0}: {1}".format(name_and_addr, description[:maxlen])
