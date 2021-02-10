import tweepy
import csv
import sys
import datetime
import time
import pytz
import schedule
import json
from tweepy import OAuthHandler
from tweepy import API

# Twitter API credentials
# Please fill your twitter developer credantials in order to proceed further.

# consumer_key = ""
# consumer_secret = ""
# access_key = ""
# access_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True) 

queries = []
tweet_ids = []
old_data_tweet_list = []
like_count = []
retweet_count = []
tweet_count_order = []

start_time = time.time()
# local_tz = pytz.timezone('Asia/Kolkata')

filename = 'Testing.json'   # This will be the name of the json file

local_tz = pytz.timezone('UTC')

script_start = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
# script_start = local_tz.localize(script_start)
script_start_time = script_start.astimezone(pytz.UTC)

def main():
    
    # This is for building an initial json file
    initial_data = {'TweetData': []}
    with open(filename, 'w') as out_file:
        out_file.write(json.dumps(initial_data, default = JSONserializer))
    
    print("\n\n\n -----------------PROGRAM STARTS------------- \n\n\n")
    
    schedule.every(30).minutes.do(read_queries)
    schedule.every(1).hour.do(get_ImageTweets, queries)
    schedule.every(1).hour.do(get_counts, old_data_tweet_list)
    schedule.every(1).hour.do(append_like_count,filename, tweet_count_order, old_data_tweet_list, like_count, retweet_count)
    
    while True:
        schedule.run_pending()

# Function that reads the queries from the "queries.txt" file
def read_queries():
    with open('queries.txt', 'r') as targets_file:
        targets_list = targets_file.readlines()
    
    global queries
    
    for item in targets_list:
        if item.strip('\n') not in queries and item.strip('\n') != "":
            queries.append(item.strip('\n'))
    
def JSONserializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__() 

# This function extracts the like and retweet counts of the tweetsIDs.
def get_counts(list):
    global like_count
    global retweet_count
    global tweet_count_order
    
    tweet_count_order.clear()
    like_count.clear()
    retweet_count.clear()
    for tweetID in list:
        try:
            tweet = api.get_status(tweetID)
            tweet_count_order.append(tweetID)
            like_count.append(tweet.favorite_count)
            retweet_count.append(tweet.retweet_count)
            
        except (TweepError):
            pass

def get_ImageTweets(query_list):
    #initialize a list to hold all the tweepy Tweets
    global tweet_ids
    
    alltweets = []
    tweets_created_at = []
    tweet_data = None
    
    for query in query_list:
    
        new_tweets = api.search(q = query,count=100)

        alltweets.extend(new_tweets)

        oldest = alltweets[-1].id - 1

        for x in range(0,0):
            new_tweets = api.search(q = query,count=100,max_id=oldest)
            alltweets.extend(new_tweets)
            oldest = alltweets[-1].id - 1

        outtweets = {'TweetData': []}
        for tweet in alltweets:
            #not all tweets will have media url, so lets skip them
            try:
                    print (tweet.entities['media'][0]['media_url'])
            except (NameError, KeyError):
                    #we dont want to have any entries without the media_url so lets do nothing
                    pass
            else:
                    replies=[]
                    tweet_id = tweet.id
                    max_id = None

                    tweet_time = local_tz.localize(tweet.created_at)
                    tweet_time = tweet_time.astimezone(pytz.UTC)
                    
                    tweets_created_at.append(tweet_time)

                    if (tweet_time > script_start_time):

                        outtweets['TweetData'].append({"ScreenName": tweet.user.screen_name,
                                                       "TweetID": tweet.id_str,
                                                       "TweetTime": tweet_time,
                                                        "LikeCountList": [{"Like _Count": tweet.favorite_count,
                                                                          "Time": datetime.datetime.utcnow()}],
                                                        "RetweetCountList": [{"Retweet_Count": tweet.retweet_count,
                                                                             "Time": datetime.datetime.utcnow()}],
                                                        "Reply_to_screen_Name": tweet.in_reply_to_screen_name,
                                                        "Is_Quote": tweet.is_quote_status,
                                                        "Source": tweet.source,
                                                        "Text": tweet.text,
                                                        "Media_url": tweet.entities['media'][0]['media_url'],
                                                        "Media_type": tweet.extended_entities['media'][0]['type']})
                        
                        if tweet_id not in tweet_ids:
                            tweet_ids.append(tweet_id)

        # tweet_data = json.dumps(outtweets, default = JSONserializer)
        print("\n\n *************** Outtweets Are ***************** \n\n", outtweets)
        write_JSON(outtweets)

def write_JSON(data):
    #write the json 
    
    global old_data_tweet_list
    
    with open(filename, 'r+') as out_file:
        old_data = json.load(out_file)
        for tweet in data['TweetData']:
            if tweet['TweetID'] not in old_data_tweet_list:
                old_data['TweetData'] = old_data['TweetData'] + [tweet]
                old_data_tweet_list.append(tweet['TweetID'])   
                
        out_file.seek(0)
        old_data = json.dumps(old_data, default = JSONserializer)
        out_file.write(old_data)

def append_like_count(filename,tweet_count_order,tweet_list,new_like_count,new_retweet_count):
    """ Append a column in existing json"""
    
    global not_useful_tweets
    rows_ = []
    
    with open(filename, 'r+') as in_file:
        tweets_data = json.load(in_file)
        index = 0
        
        for i in range(len(tweet_count_order)):
            for tweet in tweets_data['TweetData']:
                if str(tweet_count_order[i]) == tweet['TweetID']:
                    updated_like_count = {"Like _Count": new_like_count[i],
                                          "Time": datetime.datetime.utcnow()}

                    updated_retweet_count = {"Retweet_Count": new_retweet_count[i],
                                             "Time": datetime.datetime.utcnow()}

                    if len(tweet['LikeCountList']) <= 10:
                        tweet['LikeCountList'].append(updated_like_count)

                    if len(tweet['RetweetCountList']) <= 10:
                        tweet['RetweetCountList'].append(updated_retweet_count)

            
        in_file.seek(0)
        tweets_data = json.dumps(tweets_data, default = JSONserializer)
        in_file.write(tweets_data)

    return 

# Driver code 
if __name__ == '__main__':
    main()