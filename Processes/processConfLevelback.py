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
minersCol = phaladb['miners']
eventsCol = phaladb['events']

inactive_miners = minersCol.find({"state": {"$ne": "Inactive"}})
cnt = 0
for miner in inactive_miners:
    miner_id = miner["_id"]
    confidence_level = miner["confidenceLevel"]
    cnt += 1
    print("miner_id: " + miner_id + "   count: " + str(cnt) + "   confidence_level: " + str(confidence_level))
    eventsCol.update_many(
        {"workerId": miner_id, "method": "SessionSettled", "confidenceLevel": {"$exists": False}, "timestamp": {"$gt": 1696173834000}},
        {"$set": {"confidenceLevel": confidence_level}}
    )