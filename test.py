import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import time


client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsCol = phaladb['events']
poolsCol = phaladb['pools']


def getStakeByPool(pid,tm):
    pools = poolsCol.find_one({"_id":pid})

    totshares = 0
    for acct in pools["stakers"]:
        nft = eventsCol.find({"pid":pid,"method":"NftCreated","account_id":acct["account_id"],"timestamp":{"$lte":tm}}).sort("nft_id",pymongo.DESCENDING)
        totshares = totshares + nft[0]["shares"]


    print(totshares * 1.092746)



def getPoolsByStaker(uid,tm):
    nfts = list(eventsCol.find({"method":"NftCreated","account_id":uid,"timestamp":{"$lte":tm}}).sort([("pid",pymongo.ASCENDING),("nft_id",pymongo.DESCENDING)]))
    lastpid = 0
    for nft in nfts:
        if (nft["shares"] > 0):
            if lastpid != nft["pid"]:
                print(nft["pid"])
            lastpid = nft["pid"]
    

# getStakeByPool(1674,1674157824011)
getPoolsByStaker("44RGVAd8sadC7Bitqe3tj5NTeXMrojE1NqGecdBiQX2bLUbG",16784157824011)