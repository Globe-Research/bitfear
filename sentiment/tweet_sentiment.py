import tweepy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
import re

analyser = SentimentIntensityAnalyzer()

consumer_key = 'YOUR KEY HERE'
consumer_secret = 'YOUR KEY HERE'
access_token = 'YOUR KEY HERE'
access_token_secret = 'YOUR KEY HERE'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def list_tweets(user_id, count, prt=False):
    tweets = api.user_timeline(
        "@" + user_id, count=count, tweet_mode='extended')
    tw = []
    for t in tweets:
        tw.append(t.full_text)
        if prt:
            print(t.full_text)
            print()
    return tw

def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt

def clean_tweets(lst):
    # remove twitter Return handles (RT @xxx:)
    lst = np.vectorize(remove_pattern)(lst, "RT @[\w]*:")
    # remove twitter handles (@xxx)
    lst = np.vectorize(remove_pattern)(lst, "@[\w]*")
    # remove URL links (httpxxx)
    lst = np.vectorize(remove_pattern)(lst, "https?://[A-Za-z0-9./]*")
    # remove special characters, numbers, punctuations (except for #)
    lst = np.core.defchararray.replace(lst, "[^a-zA-Z#]", " ")
    return lst


def sent(tweet):
    sent_dict = analyser.polarity_scores(tweet)
    return sent_dict #a dictionary with compound, neg, neu, pos as the keys and floats as the values

def sentiment_analyzer_scores(tweet): #uses compound score
    score = analyser.polarity_scores(tweet)
    lb = score['compound']
    if lb >= 0.5:
        return 1 #negative
    elif (lb > -0.5) and (lb < 0.05):
        return 0 #neutral
    else:
        return -1 #positive

if __name__ == '__main__':
    print(sent("I hated this movie, but still enjoyed the soundtrack."))
