from bs4 import BeautifulSoup as bs
import re
import json
from pymongo import MongoClient

client = MongoClient('mongodb://assignment:assignment@assignment3-shard-00-00.zzszd.mongodb.net:27017,assignment3-shard-00-01.zzszd.mongodb.net:27017,assignment3-shard-00-02.zzszd.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-nab8sn-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client['ReuterDb']
collection = db['data']
print(collection.find_one())

def preprocess_text(text):
    cleanedString = text.lower()
    cleanedString = re.sub('@[^\s]+', '', cleanedString)
    cleanedString = re.sub(r'#([^\s]+)', r'\1', cleanedString)
    cleanedString = cleanedString.encode('ascii', 'ignore').decode('ascii')
    cleanedString = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))', '', cleanedString)
    cleanedString = re.sub(r'http\S+', '', cleanedString)
    cleanedString = cleanedString.replace('\n','')
    
    return str(cleanedString.strip())

extracted = []

with open("reut2-009.sgm", "r") as file:
    # Read each line in the file, readlines() returns a list of lines
    content = file.readlines()
    # Combine the lines in the list into a string
    content = "".join(content)
    bs_content = bs(content, "lxml")

    result = bs_content.find_all("reuters")
    for each in result:
        try:
            temp = {}
            title = preprocess_text(each.find("title").text)
            text = preprocess_text(each.find("text").text)
            temp['title'] = title
            temp['text'] = text
            extracted.append(temp)
        except:
            pass
        
with open("reut2-014.sgm", "r") as file:
    # Read each line in the file, readlines() returns a list of lines
    content = file.readlines()
    # Combine the lines in the list into a string
    content = "".join(content)
    bs_content = bs(content, "lxml")

    result = bs_content.find_all("reuters")
    for each in result:
        try:
            temp = {}
            title = preprocess_text(each.find("title").text)
            text = preprocess_text(each.find("text").text)
            temp['title'] = title
            temp['text'] = text
            extracted.append(temp)
        except:
            pass
        
print("inserting...")
collection.insert_many(extracted)