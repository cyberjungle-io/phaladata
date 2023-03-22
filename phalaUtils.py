import phalaBlockchain
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol= phaladb['miners']
eventsCol = phaladb['events']


def getTotalSharesByPool(pid,block):
    
    agg = [
            {
                '$match': {
                    'method': 'NftCreated', 
                    'pid': pid, 
                    'blockNumber':{'$lt':block}
                    
                }
            }, {
                '$group': {
                    '_id': '$account_id', 
                    'maxNft': {
                        '$max': '$nft_id'
                    }
                }
            }
        ]

    rw = list(eventsCol.aggregate(agg))
    shares = 0
    for t in rw:
        e = eventsCol.find_one({"method": 'NftCreated',"pid":pid,"nft_id":t["maxNft"]})
        
        if e["account_id"] != "42qnPyfw3sbWMGGtTPPc2YFNZRKPGXswRszyQQjGs2FDxdim":
            print("account: " + e["account_id"] + "   Shares: " + str(e["shares"]))
            wdrl = eventsCol.find({'method': 'Withdrawal','section': 'phalaBasePool','pid': pid,'account_id': e["account_id"],'blockNumber':{'$gt':e['blockNumber']},'blockNumber':{'$lt':block}})
            wshares = 0
            for w in wdrl:
                wshares = wshares + w["shares"]


            shares = shares + e["shares"] - wshares
        


    

    return shares
   
    
def getTotalSharesByPoolAllAccounts(pid,block):
    
    agg = [
            {
                '$match': {
                    'method': 'NftCreated', 
                    'pid': pid, 
                    'blockNumber':{'$lt':block}
                    
                }
            }, {
                '$group': {
                    '_id': '$account_id', 
                    'maxNft': {
                        '$max': '$nft_id'
                    }
                }
            }
        ]

    rw = list(eventsCol.aggregate(agg))
    shares = 0
    acArray = []
    for t in rw:
        e = eventsCol.find_one({"method": 'NftCreated',"pid":pid,"nft_id":t["maxNft"]})
        
        if e["account_id"] != "42qnPyfw3sbWMGGtTPPc2YFNZRKPGXswRszyQQjGs2FDxdim":
            print("account: " + e["account_id"] + "   Shares: " + str(e["shares"]))
            wdrl = eventsCol.find({'method': 'Withdrawal','section': 'phalaBasePool','pid': pid,'account_id': e["account_id"],'blockNumber':{'$gt':e['blockNumber']},'blockNumber':{'$lt':block}})
            wshares = 0
            for w in wdrl:
                wshares = wshares + w["shares"]
            shares = shares + e["shares"] - wshares
            tshares = e['shares'] + wshares
            if tshares > 0:
                tacc = {"account_id":e["account_id"],"shares":tshares}
                acArray.append(tacc)


    for a in acArray:
        a["ratio"] = a['shares']/shares

    return acArray,shares
   
ac,sh = getTotalSharesByPoolAllAccounts(1673,3037733)


print("Pha: " + str(sh * 1.182114) )
for x in ac:
    print(x )
    