'''
Short script to test Twitter authentication and searching hashtags
using Tweepy
'''

import tweepy
import json

focus_hashtags = ['#bitcoin', '#crypto', '#cryptocurrency']
results = {}

# Need a Twitter developer account & application!
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

tApi = tweepy.API(auth)

for hashtag in focus_hashtags:
    results[hashtag] = tApi.search(hashtag)

with open('hashtag_search.json', 'w') as out:
    json.dump(results, out)