import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

eventsCol = phaladb['events']   
cnt = 0 

while True:
    events = eventsCol.find({
            "method": 'NFTBurned',
            "account_id":None,
            "section": 'rmrkCore'
            }).limit(500)
    
    for e in events:
        e["account_id"] = e["data"][0]
        e["cid"] = e["data"][1]
        e["nft_id"] = e["data"][2]
        eventsCol.replace_one({"_id":e["_id"]},e)
        cnt = cnt + 1

    print(cnt)
