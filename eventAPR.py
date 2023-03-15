import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import phalaBlockchain 

import time


client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol = phaladb['miners']
eventsCol = phaladb['events']

miners = minersCol.find({"state":{"$ne":"Inactive"}})

for miner in miners:
    lastTime = 0
    events = eventsCol.find({"workerId":miner["_id"],"method":"SessionSettled"}).sort("timestamp")
    for event in events:
        if lastTime > 0:
            