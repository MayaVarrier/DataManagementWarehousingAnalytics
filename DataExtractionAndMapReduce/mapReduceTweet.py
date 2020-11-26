# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 16:16:17 2020

@author: vmaya
"""

import pymongo
import json
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pymongo import MongoClient


conf = SparkConf().setAppName("Word count Tweet")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client['ProcessedDb']
collection_1 = db['tweetsCleaned']
fetch_data = []
for each in collection_1.find():
    fetch_data.append(each['text'])

rdd = sc.parallelize(fetch_data)
row_rdd = rdd.map(lambda x: Row(x))
df_my = sqlContext.createDataFrame(row_rdd,['words'])
tweetWordsCount = df_my.count()

job = df_my.withColumn('word', func.explode(func.split(func.col('words'), ' '))).groupBy('word').count().sort('count', ascending=False)
results = job.toJSON().map(lambda j: json.loads(j)).collect()
search_keywords = ["storm","winter","canada","hot","cold","flu","snow","indoor","safety","rain","ice"]

for i in results:
    if i["word"] in search_keywords:
        print(str(i['word']) + " --> " + str(i['count']))