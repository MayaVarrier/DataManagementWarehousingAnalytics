# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 20:12:42 2020

@author: vmaya
"""

import pymongo
import json
import math
import pandas as pd
from pymongo import MongoClient


client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client['ReuterDb']
fetch_data = []
collection_1 = db['data']
for each in collection_1.find():
    fetch_data.append(each['text'])
    
N = len(fetch_data)
search_keywords = ["canada","rain","cold","hot"]

documentList = []
canadaList = []

for keyword in search_keywords:
    counter = 0
    canadaCounter = 1
    temp = {}
    for news in fetch_data:
        if keyword in news:   
            tempCanada = {}
            if keyword == "canada":
                tempCanada["Canada appeared in documents"] = "Article#"+str(canadaCounter)
                canadaCounter+= 1
                tempCanada["Total Words (m)"] = len(news.split(" "))
                tempCanada["Frequency (f)"] = news.count(keyword)
                tempCanada["f/m"] = news.count(keyword) / len(news.split(" "))
                canadaList.append(tempCanada)
            counter+= 1
    temp["Search Query"] = keyword
    temp["Document Containing Term (df)"] = counter
    temp["Total Documents(N)/number of documents term appeared (df)"] = N/counter
    temp["Log10(N/df)"] = math.log10(N/counter)
    documentList.append(temp);

pd.DataFrame(documentList).to_csv("SemanticAnalysisOutput.csv")
pd.DataFrame(canadaList).to_csv("SemanticAnalysisCanadaOutput.csv")

df = pd.DataFrame(canadaList).sort_values("f/m",ascending = False)[:1]
print(df)            