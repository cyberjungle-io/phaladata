
from pymongo import MongoClient
import pymongo
import requests
from ast import literal_eval

import json
import time
import math

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

eventsCol = phaladb['events']

contributionList = eventsCol.find({"method":"Contribution","section":"phalaStakePool","pid":None})

for c in contributionList:
    c["pid"] = c["data"][0]
    c["account_id"] = c["data"][1]
    try:
        c["amount"] = c["data"][2] / 1000000000000
    except:
        c["amount"] = literal_eval(c["data"][2]) / 1000000000000
    if len(c["data"]) == 4:
        try:
            c["shares"] = c["data"][3] / 1000000000000
        except:
            c["shares"] = literal_eval(c["data"][3]) / 1000000000000
            
    eventsCol.replace_one({"_id":c["_id"]},c)

