# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 18:55:42 2020

@author: vmaya
"""

import json
import re
import pandas as pd
from pymongo import MongoClient
from collections import Counter




def preprocess_text(text):
    clean_str = text.lower()
    clean_str = re.sub('@[^\s]+', '', clean_str)
    clean_str = re.sub(r'#([^\s]+)', r'\1', clean_str)
    clean_str = clean_str.encode('ascii', 'ignore').decode('ascii')
    clean_str = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))', '', clean_str)
    clean_str = re.sub(r'http\S+', '', clean_str)
    clean_str = clean_str.replace('\n','')
    clean_str = clean_str.replace("rt","")
    
    return str(clean_str.strip())


client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client['ProcessedDb']
collection_1 = db['tweetsCleaned']

coll = []
        
for each in collection_1.find():
    coll.append(preprocess_text(each['text']))
# print (coll)    

bagOfWordsList = []
    
for i in coll:
    print("Tweet :" + i + "\n")
    bagOfWordsList.append(Counter(i.split(" ")) )
    print("Bow : ", Counter(i.split(" ")) ,"\n")
    
f = open("positive.txt")
positive = f.read().split("\n")

f = open("negative.txt")
negative = f.read().split("\n")

polarityList = []
counter = 0

for bows in bagOfWordsList:
    positiveList = []
    negativeList = []
    temp = {}
    temp["Tweet"] = counter + 1
    temp["Message/Tweet"] = coll[counter]
    counter+= 1
    for bow in bows:
        if bow in positive: 
            positiveList.append(bow)
        elif bow in negative:
            negativeList.append(bow)
    if len(positiveList) > len(negativeList):
        temp["Match"] = positiveList
        temp["Polarity"] = "Positive"
    elif len(negativeList) > len(positiveList):
        temp["Match"] = negativeList
        temp["Polarity"] = "Negative"
    else: 
        temp["Match"] = ""
        temp["Polarity"] = "Neutral"
    polarityList.append(temp)

pd.DataFrame(polarityList).to_csv("SentimentAnalysisOutput.csv")
    
    
        
        
    