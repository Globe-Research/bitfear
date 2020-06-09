import json
import os
import re
import time
import tweepy
import csv

import numpy as np

from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

twitter_secrets_path = './secrets/twitter.json'

focus_languages = ['en']
focus_hashtags = ['#bitcoin'] # '#crypto', '#cryptocurrency'

# Load Twitter credentials
try:
    with open(twitter_secrets_path, 'r') as f:
        twCreds = json.load(f)

    consumer_key = twCreds['consumer_key']
    consumer_secret = twCreds['consumer_secret']
    access_token = twCreds['access_token']
    access_token_secret = twCreds['access_token_secret']
except Exception as e:
    logger.error('Failed to load Twitter credentials!', exc_info=True)
    raise

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

def remove_pattern(input_txt, pattern):
    '''
    Removes substring from input matching given pattern.

    Parameters:
        input_txt (string): Text to remove pattern match from
        pattern (string): Pattern to match to remove

    Returns:
        input_txt (string): Input string with any pattern matches removed
    '''

    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt

def clean_tweets(lst):
    '''
    Removes Twitter handles, return handles, URLs and characters
    unsupported for sentiment analysis.

    Parameters:
        lst (list): List of tweet text strings to clean
    

    Returns:
        lst (string): List of cleaned tweet text strings
    '''

    # remove twitter Return handles (RT @xxx:)
    lst = np.vectorize(remove_pattern)(lst, "RT @[\w]*:")
    # remove twitter handles (@xxx)
    lst = np.vectorize(remove_pattern)(lst, "@[\w]*")
    # remove URL links (httpxxx)
    lst = np.vectorize(remove_pattern)(lst, "https?://[A-Za-z0-9./]*")
    # remove special characters, numbers, punctuations (except for #)
    lst = np.core.defchararray.replace(lst, "[^a-zA-Z#]", " ")
    return lst

def sentiment_scores(tweet):
    '''
    Performs VADER sentiment analysis on input string.

    Parameters:
        tweet (string): Cleaned tweet text string
    

    Returns:
        sent_dict (dict): A dictionary with compound, neg, neu, pos as the keys and floats as the values
    '''

    sent_dict = analyser.polarity_scores(tweet)
    return sent_dict

def sentiment_compound_score(tweet):
    '''
    Performs VADER sentiment analysis on input string and
    returns only an integer corresponding to positive, negative
    or neutral based on compound score.

    Parameters:
        tweet (string): Cleaned tweet text string
    

    Returns:
        (int): -1 = negative, 0 = neutral, 1 = positive
    '''
    
    score = analyser.polarity_scores(tweet)
    lb = score['compound']
    if lb >= 0.05:
        return 1
    elif (lb > -0.05) and (lb < 0.05):
        return 0
    else:
        return -1


tApi = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
analyser = SentimentIntensityAnalyzer()

# Open/Create a file to append data
csvFile = open('capi_twitter.csv', 'a')
#Use csv Writer
csvWriter = csv.writer(csvFile)

for tweet in tweepy.Cursor(tApi.search,q="#bitcoin",count=100,
                           lang="en",
                           since="2019-05-01",
                           until="2020-03-10").items():
    if not '…' in tweet.text and not 'RT' in tweet.text:
            out = dict()

            out['created'] = tweet.created_at
            out['tweet_id'] = tweet.id_str
            out['user_id'] = tweet.user.id_str

            try:
                out['text'] = tweet.extended_tweet['full_text']
            except (AttributeError, KeyError):
                out['text'] = tweet.text
            
            out['text_clean'] = clean_tweets([out['text']])[0]
            out['sentiment_scores'] = sentiment_scores(out['text_clean'])
            out['sentiment_rating'] = sentiment_compound_score(out['text_clean'])
            csvWriter.writerow([out['created'], out['tweet_id'], out['user_id'], out['sentiment_scores']['compound']])