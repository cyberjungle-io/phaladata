
import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import phalaBlockchain 
import phalaUtils


import time


def processRewardReceivedAccounts(eventId):
    sourceEvent = eventsCol.find_one({"_id":eventId})

    accountList,shares = phalaUtils.getTotalSharesByPoolAllAccounts(sourceEvent["pid"],sourceEvent["blockNumber"])

    for ac in accountList:
        print(ac["account_id"] +"   Ratio: " +  str(ac["ratio"]) + "  Shares: " + str(ac["shares"]))
        ne = {
            "_id": sourceEvent["_id"] + "-" + ac["account_id"],
            "eventNumber": sourceEvent["eventNumber"],
            "blockNumber": sourceEvent["blockNumber"],
            "timestamp": sourceEvent["timestamp"],
            "pid": sourceEvent["pid"],
            "account_id": ac["account_id"],
            "method": "RewardReceivedAccount",
            "section": "Derived",
            "accountShares": ac["shares"],
            "ownershipRatio": ac["ratio"],
            "pha": sourceEvent["to_staker"] * ac["ratio"]
        }

        eventsCol.replace_one({"_id":ne["_id"]},ne,upsert=True)
    



client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

eventsCol = phaladb['events']

processRewardReceivedAccounts("3549589-341")
