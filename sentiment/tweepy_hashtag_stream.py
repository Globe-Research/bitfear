import google.cloud.exceptions
import json
import logging
import os
import re
import time
import tweepy

import numpy as np

from datetime import datetime
from google.cloud import firestore
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

twitter_secrets_path = './secrets/twitter.json'

focus_languages = ['en']
focus_hashtags = ['#bitcoin'] # '#crypto', '#cryptocurrency'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('twitter_stream.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

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

def firestore_write(collection, document, data):
    try:
        db.collection(collection).document(document).set(data)
        return True
    except Exception as e:
        logger.error('Firestore API error!', exc_info=True)
        return False

class StreamListener(tweepy.StreamListener):
    '''
    A listener to handle tweets that are received from the stream.
    Outputs select metadata from received tweets to Google Cloud Firestore.
    '''
    def on_status(self, status):

        if not '…' in status.text and not 'RT' in status.text:
            tweet = dict()

            tweet[u'created'] = status.created_at
            tweet[u'tweet_id'] = status.id_str
            tweet[u'user_id'] = status.user.id_str

            try:
                tweet[u'text'] = status.extended_tweet['full_text']
            except (AttributeError, KeyError):
                tweet[u'text'] = status.text
            
            tweet[u'text_clean'] = clean_tweets([tweet[u'text']])[0]
            tweet[u'sentiment_scores'] = sentiment_scores(tweet[u'text_clean'])
            tweet[u'sentiment_rating'] = sentiment_compound_score(tweet[u'text_clean'])

            if firestore_write('tweet_data_#bitcoin', status.id_str, tweet):
                logger.info('Stored tweet {}'.format(status.id_str))
            else:
                logger.error('Failed to store tweet {}'.format(status.id_str))

        else:
            logger.info('Skipping tweet {}'.format(status.id_str))

        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False

# Setup APIs and listener/stream instances
try:
    tApi = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    listener = StreamListener()
    stream = tweepy.Stream(auth=tApi.auth, listener=listener)
    analyser = SentimentIntensityAnalyzer()
    db = firestore.Client()
except Exception as e:
    logger.error('Failed to setup APIs and listener/stream instances!', exc_info=True)
    raise

if __name__ == '__main__':
    logger.info("===================================================")
    logger.info('BitFEAR Tweet Streaming for Sentiment Analysis')

    while True:
        try:
            logger.info('Running:')
            stream.filter(languages=focus_languages, track=focus_hashtags)
        except KeyboardInterrupt:
            logger.info('Stopping...')
            stream.disconnect()
            logger.info('Stopped.')
            break
        except Exception as e:
            logger.error('Unexpected error!', exc_info=True)
            continue