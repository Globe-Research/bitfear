'''
Short script to test Twitter authentication and searching hashtags
using Tweepy
'''

import tweepy
import json

focus_hashtags = ['#bitcoin', '#crypto', '#cryptocurrency']
results = {}

# Load Twitter credentials
with open('./secrets/twitter.json', 'r') as f:
    twCreds = json.load(f)

consumer_key = twCreds['consumer_key']
consumer_secret = twCreds['consumer_secret']
access_token = twCreds['access_token']
access_token_secret = twCreds['access_token_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

tApi = tweepy.API(auth)

for hashtag in focus_hashtags:
    results[hashtag] = tApi.search(hashtag)

print(results)