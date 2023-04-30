import phalaBlockchain
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math




def getTotalSharesByPool(pid):
    client = MongoClient('10.2.2.11', 27017)
    phaladb = client['phala']
    minersCol= phaladb['miners']
    eventsCol = phaladb['events']
    poolsCol = phaladb['pools']
    accountsCol = phaladb['accounts']
    agg = [
            {
                '$match': {
                    'method': 'NftCreated', 
                    'pid': pid, 
                    
                    
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
        


    
    client.close()
    return shares
   
    
def getTotalSharesByPoolAllAccounts(pid,block):
    client = MongoClient('10.2.2.11', 27017)
    phaladb = client['phala']
    minersCol= phaladb['miners']
    eventsCol = phaladb['events']
    poolsCol = phaladb['pools']
    accountsCol = phaladb['accounts']
    agg = [
            {
                '$match': {
                    'method': 'NftCreated', 
                    'pid': pid, 
                   
                    
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
    totalShares = 0
    acArray = []
    for t in rw:
        shares = 0
        e = eventsCol.find_one({"method": 'NftCreated',"pid":pid,"nft_id":t["maxNft"]})
       
        if e["account_id"] != "42qnPyfw3sbWMGGtTPPc2YFNZRKPGXswRszyQQjGs2FDxdim" and e["shares"] > 0:
            #print("account: " + e["account_id"] + "   Shares: " + str(e["shares"]) + "    Block: " + str(e["blockNumber"]))
            wdrl = eventsCol.find({'method': 'Withdrawal','section': 'phalaBasePool','pid': pid,'account_id': e["account_id"]})
            wshares = 0
            for w in wdrl:
                if (w["blockNumber"] > e["blockNumber"]):
                    wshares = wshares + w["shares"]
                    #print("wshares: " + str(w["shares"]) + "  Block: " + str(w["blockNumber"]))
            
           
 

            shares = shares + e["shares"] - wshares
            tshares = e['shares'] + wshares
            if shares > 0.000001:
                tacc = {"account_id":e["account_id"],"shares":shares}
                acArray.append(tacc)
            totalShares = totalShares + shares



    totratio = 0
    for a in acArray:
        a["ratio"] = a['shares']/totalShares
        totratio = totratio + a["ratio"]

    # print("Total Shares: " + str(totalShares))
    # print("Total Ratio: " + str(totratio))
    client.close()
    return acArray,totalShares



def updatePoolStakers(pid):
    client = MongoClient('10.2.2.11', 27017)
    phaladb = client['phala']
    minersCol= phaladb['miners']
    eventsCol = phaladb['events']
    poolsCol = phaladb['pools']
    accountsCol = phaladb['accounts']
    pool = poolsCol.find_one({"_id":pid})
    if pool != None:
        ac, st = getTotalSharesByPoolAllAccounts(pid,100000000)
        stakers = []
        for a in ac:
            a["amount"] = a["shares"] * pool["sharePrice"]
            stakers.append(a)
        pool["stakers"] = stakers
        poolsCol.replace_one({"_id":pool["_id"]},pool)
        for s in pool["stakers"]:
            updateAccountDelegation(s["account_id"])
    client.close()

            
def updateAccountDelegation(account_id):
    client = MongoClient('10.2.2.11', 27017)
    phaladb = client['phala']
    
    poolsCol = phaladb['pools']
    

    pools = poolsCol.find({'stakers.account_id': account_id})
    delegation = 0
    for p in pools:
        if "stakers" in p:
            for s in p["stakers"]:
                if s["account_id"] == account_id:
                    delegation = delegation + s["amount"]

    client.close()
    return delegation




tdg = updateAccountDelegation("43G4DVpemfWn3hkNG3SvuDEQbuV3o368Rht7Z629EJNxC5Cr")
print(tdg)



#updatePoolStakers(1828)
   
