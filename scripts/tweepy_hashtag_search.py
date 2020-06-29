'''
Short script to test Twitter authentication and searching hashtags
using Tweepy
'''

import csv
import json
import os
import time
import tweepy

resolution = 60

twitter_data_folder = './sentiment/twitter_data'
count_file = 'volume_data'

focus_hashtags = {'#bitcoin': '1233510203238309888'} # '#crypto', '#cryptocurrency'

hash_headers = ['id', 'created_at', 'user_id', 'full_text']
count_headers = ['time']
count_headers.extend(list(focus_hashtags.keys()))

# Load Twitter credentials
with open('./secrets/twitter.json', 'r') as f:
    twCreds = json.load(f)

consumer_key = twCreds['consumer_key']
consumer_secret = twCreds['consumer_secret']
access_token = twCreds['access_token']
access_token_secret = twCreds['access_token_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

tApi = tweepy.API(auth) # Cannot use parser=tweepy.parsers.JSONParser() due to issues (https://github.com/tweepy/tweepy/issues/538)

def ascii_only(text):
    return ''.join([i if ord(i) < 256 else '()' for i in text])

def scrape(hashtags):
    results = {}
    count_result = {}

    for hashtag in hashtags.keys():
        results[hashtag] = {'time': 0, 'last_id': '', 'count': 0, 'tweets': []}
        tweet_counter = 0
        for tweet in tweepy.Cursor(tApi.search, q=hashtag, count=100, lang='en', result_type='recent', since_id=hashtags[hashtag], tweet_mode='extended').items():
            if not 'retweeted_status' in tweet._json.keys():
                results[hashtag]['tweets'].append({
                    'id': tweet._json['id_str'],
                    'created_at': tweet._json['created_at'],
                    'user_id': tweet._json['user']['id_str'],
                    'full_text': ascii_only(tweet._json['full_text'])
                    })
                tweet_counter += 1
            results[hashtag]['count'] = tweet_counter
            results[hashtag]['time'] = time.strftime('%H:%M:%S')
            
            
        results[hashtag]['last_id'] = results[hashtag]['tweets'][0]['id']
        hashtags[hashtag] = results[hashtag]['last_id']

        print('{}: got {} tweets for {}, last id {}'.format(results[hashtag]['time'], results[hashtag]['count'], hashtag, results[hashtag]['last_id']))

        filename = twitter_data_folder + '/{}.csv'.format(hashtag)
        csv_exists = os.path.isfile(filename)

        try:
            with open(filename, 'a+', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=hash_headers)

                if not csv_exists:
                    writer.writeheader()

                writer.writerows(tweet for tweet in reversed(results[hashtag]['tweets']))
            
            print('Successfully updated {}'.format(filename))

        except PermissionError:
            # File unwritable for some temporary reason (e.g. been opened by another process) so skip
            print('Skipping {}: file unavailable'.format(filename))
        
        count_result['time'] = time.strftime('%H:%M:%S')
        count_result[hashtag] = results[hashtag]['count']

    filename = twitter_data_folder + '/{}.csv'.format(count_file)
    csv_exists = os.path.isfile(filename)

    try:
        with open(filename, 'a+', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=count_headers)

            if not csv_exists:
                writer.writeheader()

            writer.writerow(count_result)
        
        print('Successfully updated {}'.format(filename))

    except PermissionError:
        # File unwritable for some temporary reason (e.g. been opened by another process) so skip
        print('Skipping {}: file unavailable'.format(filename))

    return hashtags


if __name__ == '__main__':
    while True:
        focus_hashtags = scrape(focus_hashtags)
        print('Waiting {} seconds...\n'.format(resolution))
        time.sleep(int(resolution))