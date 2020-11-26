# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 16:59:02 2020

@author: vmaya
"""

import pymongo
import json
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pymongo import MongoClient
from pyspark.sql import Row

conf = SparkConf().setAppName("Word Count Reuter")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client['ReuterDb']
fetch_data = []
collection_1 = db['data']
for each in collection_1.find():
    fetch_data.append(each['text'])

rdd1 = sc.parallelize(fetch_data)
row_rdd = rdd1.map(lambda x: Row(x))
df_my = sqlContext.createDataFrame(row_rdd,['numbers'])
reuterWordsCount = df_my.count()

job = df_my.withColumn('word', func.explode(func.split(func.col('numbers'), ' '))).groupBy('word').count().sort('count', ascending=False)

results = job.toJSON().map(lambda j: json.loads(j)).collect()
search_keywords = ["storm","winter","canada","hot","cold","flu","snow","indoor","safety","rain","ice"]
for i in results:
    if i["word"] in search_keywords:
        print(str(i['word']) + " --> " + str(i['count']))