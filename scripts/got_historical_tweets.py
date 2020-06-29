import csv
import GetOldTweets3 as got
import numpy as np
import pandas as pd
import re
import time

from datetime import datetime, timezone, date, timedelta
from urllib.error import HTTPError, URLError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

counter = 0

since = pd.to_datetime('2019-07-22').replace(tzinfo=timezone.utc)
until = pd.to_datetime('2020-04-01').replace(tzinfo=timezone.utc)

since_str = since.strftime('%Y-%m-%d')
until_str = until.strftime('%Y-%m-%d')

analyser = SentimentIntensityAnalyzer()

incomplete = ['2019-05-30', '2019-06-26', '2019-06-29', '2019-07-13', '2019-09-04', '2019-09-24', '2019-10-12', '2019-10-24', '2019-11-07']

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

def sleepBetweenFailedRequests(request, error, proxy):
    # A unique request ID and the proxy it used are passed
    # for more advanced rate limiting preventing strategies.

    # Deal with all the potential URLLib errors that may happen
    # https://docs.python.org/3/library/urllib.error.html
    if (isinstance(error, HTTPError) and error.code in [429, 503]):
      # Sleep for 60 seconds
      print('Received HTTP 429 (too many requests). Waiting 2 min...')
      time.sleep(120)
      return True

    if (isinstance(error, URLError) and error.errno in [111]):
      # Sleep for 60 seconds
      print('Received HTTP 111 (connection failed). Waiting 2 min...')
      time.sleep(120)
      return True

    # To stop execution of the scraper
    # raise Exception("Rate Limiting Strategy received an error it doesn't know how to deal with")

    # To stop just this single request, return False
    return False

def DownloadTweets(SinceDate, UntilDate, Query):
    '''
    Downloads all tweets from a certain month in three sessions in order to avoid sending too many requests. 
    Date format = 'yyyy-mm-dd'. 
    Query=string.
    '''
    since = datetime.strptime(SinceDate, '%Y-%m-%d')
    until = datetime.strptime(UntilDate, '%Y-%m-%d')
    tenth = since + timedelta(days = 10)
    twentieth = since + timedelta(days=20)
    
    print (f'starting first download {str(since)} to {str(tenth)}')
    first = got.manager.TweetCriteria().setQuerySearch(Query).setSince(since.strftime('%Y-%m-%d')).setUntil(tenth.strftime('%Y-%m-%d'))
    firstdownload = got.manager.TweetManager.getTweets(first, rateLimitStrategy=sleepBetweenFailedRequests)
    firstlist=[[tweet.date, tweet.id, sentiment_scores(clean_tweets([tweet.text])[0])['compound']] for tweet in firstdownload if not '…' in tweet.text and not 'RT' in tweet.text]
    
    df_1 = pd.DataFrame.from_records(firstlist, columns = ["date", "id", "sentiment_compound"])
    df_1.to_csv("%s_1.csv" % SinceDate)
    
    print('Done. Waiting 2 mins for rate limit...')
    time.sleep(120)
    
    print (f'starting second download {str(tenth)} to {str(twentieth)}')
    second = got.manager.TweetCriteria().setQuerySearch(Query).setSince(tenth.strftime('%Y-%m-%d')).setUntil(twentieth.strftime('%Y-%m-%d'))
    seconddownload = got.manager.TweetManager.getTweets(second, rateLimitStrategy=sleepBetweenFailedRequests)
    secondlist=[[tweet.date, tweet.id, sentiment_scores(clean_tweets([tweet.text])[0])['compound']] for tweet in seconddownload if not '…' in tweet.text and not 'RT' in tweet.text]
    
    df_2 = pd.DataFrame.from_records(secondlist, columns = ["date", "tweet", "sentiment_compound"])
    df_2.to_csv("%s_2.csv" % SinceDate)
    
    print('Done. Waiting 2 mins for rate limit...')
    time.sleep(120)
    
    print (f'starting third download {str(twentieth)} to {str(until)}')
    third = got.manager.TweetCriteria().setQuerySearch(Query).setSince(twentieth.strftime('%Y-%m-%d')).setUntil(until.strftime('%Y-%m-%d'))
    thirddownload = got.manager.TweetManager.getTweets(third, rateLimitStrategy=sleepBetweenFailedRequests)
    thirdlist=[[tweet.date, tweet.id, sentiment_scores(clean_tweets([tweet.text])[0])['compound']] for tweet in thirddownload if not '…' in tweet.text and not 'RT' in tweet.text]
    
    df_3 = pd.DataFrame.from_records(thirdlist, columns = ["date", "tweet", "sentiment_compound"])
    df_3.to_csv("%s_3.csv" % SinceDate)
    
    #df=pd.concat([df_1,df_2,df_3])
    #df.to_csv("%s.csv" % SinceDate)
  
    return# df
'''
while since < until:
    #DownloadTweets(since.strftime('%Y-%m-%d'), (since + timedelta(days=1)).strftime('%Y-%m-%d'), '#bitcoin')
    twc = got.manager.TweetCriteria().setQuerySearch('#bitcoin').setSince(since.strftime('%Y-%m-%d')).setUntil((since + timedelta(days=1)).strftime('%Y-%m-%d'))#.setTopTweets(True).setMaxTweets(1000)
    print(f'Starting {str(since)}')
    twd = got.manager.TweetManager.getTweets(twc, rateLimitStrategy=sleepBetweenFailedRequests)
    twlist = [[tweet.date, tweet.id, sentiment_scores(clean_tweets([tweet.text])[0])['compound']] for tweet in twd if not '…' in tweet.text and not 'RT' in tweet.text]
    df = pd.DataFrame.from_records(twlist, columns = ["date", "tweet", "sentiment_compound"])
    df.to_csv(f"M:\\coinapi-deribit-btc-options-1905-2005\\twitter_hashbtc\\test-{since.strftime('%Y-%m-%d')}.csv", index=False)
    print(f'Done {str(since)}')
    #time.sleep(120)
    since = since + timedelta(days=1)
'''
'''
while until >= since:

    twC = got.manager.TweetCriteria().setQuerySearch('#bitcoin').setSince(since_str).setUntil(until_str)
    tweets = got.manager.TweetManager().getTweets(twC)
    print(f'Got {len(tweets)} tweets.')

    csvFile = open(f'{counter}.csv', 'a', newline='')
    csvWriter = csv.writer(csvFile)

    for tweet in tweets:
        if not '…' in tweet.text and not 'RT' in tweet.text:
            clean = clean_tweets([tweet.text])[0]
            sentiment_compound = sentiment_scores(clean)['compound']

            csvWriter.writerow([str(tweet.date), tweet.id, tweet.username, sentiment_compound])

    csvFile.close()

    until = tweets[-1].date
    until_str = until.strftime('%Y-%m-%d')

    counter += 1

    print(f'Finished loop {counter} at date {until_str}.')
'''

for date in incomplete:
    dateplusone = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    twc = got.manager.TweetCriteria().setQuerySearch('#bitcoin').setSince(date).setUntil(dateplusone)#.setTopTweets(True).setMaxTweets(1000)
    print(f'Starting {date}-{dateplusone}')
    twd = got.manager.TweetManager.getTweets(twc, rateLimitStrategy=sleepBetweenFailedRequests)
    twlist = [[tweet.date, tweet.id, sentiment_scores(clean_tweets([tweet.text])[0])['compound']] for tweet in twd if not '…' in tweet.text and not 'RT' in tweet.text]
    df = pd.DataFrame.from_records(twlist, columns = ["date", "tweet", "sentiment_compound"])
    df.to_csv(f"M:\\coinapi-deribit-btc-options-1905-2005\\twitter_hashbtc\\retry-{date}.csv", index=False)
    print(f'Done retry-{date}.csv')
    #time.sleep(120)
    #since = since + timedelta(days=1)