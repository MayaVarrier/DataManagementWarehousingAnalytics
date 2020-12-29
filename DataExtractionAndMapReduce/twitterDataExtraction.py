# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 17:17:33 2020

@author: vmaya
"""

from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient

CONSUMER_KEY = "ConsumerKeyGenerated"
CONSUMER_SECRET = "ConsumerSecretKeyGenerated"
ACCESS_TOKEN = "AccessTokenGenerated"
ACCESS_TOKEN_SECRET = "SecretAccessTokenGenerated"

client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client.RawDb

WORDS = ['Storm', 'Winter', 'Canada', 'Temperature', 'Flu', 'Snow', 'Indoor', 'Safety']
tweets = []
class MyStreamListener(tweepy.StreamListener):
    
    def __init__(self, api=None):
        super(MyStreamListener, self).__init__()
        self.count = 0

        
    def on_data(self, data):
        if(len(tweets) < 1000):
            datajson = json.loads(data)
            tweets.append(datajson)
            db.tweetStreamed.insert_one(datajson)
            return True
        else:
            return False
 
        
        
    def on_error(self, status_code):
      if status_code == 420:
          #returning False in on_error disconnects the stream
          print("Error occured while streaming")
          return False
      
       
    
myStreamListener = MyStreamListener()
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)
api = tweepy.API(auth, wait_on_rate_limit=False)
myStream.filter(track=WORDS, languages=['en'])

searchTweets = []
for searchTerm in WORDS:
    tweets = tweepy.Cursor(api.search, q = searchTerm, languages = "en").items(200)
    for tweet in tweets:
        tweetjson = tweet._json
        searchTweets.append(tweetjson)

db.tweetSearched.insert_many(searchTweets)

    



    