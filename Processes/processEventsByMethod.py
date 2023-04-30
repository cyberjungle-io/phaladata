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
    events = eventsCol.find({"$or": [
            {
            "method": 'Wrapped'
            },
            {
            "method": 'Unwrapped'
            }
            ],
            "account_id":None,
            "section": 'phalaWrappedBalances'
            }).limit(500)
    
    for e in events:
        e["account_id"] = e["data"][0]
        try:
            e["amount"] = e["data"][1] / 1000000000000
        except:
            e["amount"] = literal_eval(e["data"][1]) / 1000000000000
        eventsCol.replace_one({"_id":e["_id"]},e)
        cnt = cnt + 1

    print(cnt)
