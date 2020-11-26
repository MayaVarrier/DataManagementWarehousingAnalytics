# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 15:04:32 2020

@author: vmaya
"""

import json
from pymongo import MongoClient
import re


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
db = client['RawDb']
collection_1 = db['tweetSearched']
collection_2 = db['tweetStreamed']

coll = []
        
for each in collection_1.find():
    each['text'] = preprocess_text(each['text'])
    coll.append(each)
    
for each in collection_2.find():
     each['text'] = preprocess_text(each['text'])
     coll.append(each)


db_new = client['ProcessedDb']
collection_new = db_new['tweetsCleaned']
collection_new.insert_many(coll)