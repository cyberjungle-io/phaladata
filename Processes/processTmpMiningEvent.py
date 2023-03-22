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
sourceCol = phaladb['tmpPhalaMiningEvents']
tmpEventCol = phaladb['tmpevents']


sArray = sourceCol.find({"$or":[{"result.method":"MinerStarted"},{"result.method":"MinerStopped"},{"result.method":"MinerExitUnresponsive"},{"result.method":"MinerExitUnresponive"},{"result.method":"MinerEnterUnresponsive"}]})

for s in sArray:
    
    n = {}
    n["blockNumber"] = s["blockNumber"]
    n["timestamp"] = s["timestamp"]
    n["method"] = s["result"]["method"]
    n["section"] = s["result"]["section"]
    n["account_id"] = s["result"]["data"][0]
    tmpEventCol.insert_one(n)

    
    