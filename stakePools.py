import phalaBlockchain

import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import time

def UpdatePool(pid):
    
    
    pool = phalaBlockchain.getBasePool(pid)
    newpool = {}
    if "StakePool" in pool:
        print(pool["StakePool"]["basepool"]["pid"])
        newpool["_id"] = pool["StakePool"]["basepool"]["pid"]
        newpool["owner"] = pool["StakePool"]["basepool"]["owner"]
        if pool["StakePool"]["payout_commission"] == None:
            newpool["payoutCommission"] = 0
        else:
            newpool["payoutCommission"] = pool["StakePool"]["payout_commission"] / 10000
        if pool["StakePool"]["cap"] == None:
            newpool["cap"] = 0
        else:
            newpool["cap"] = pool["StakePool"]["cap"] / 1000000000000
        newpool["totalShares"] = pool["StakePool"]["basepool"]["total_shares"] / 1000000000000
        newpool["totalStake"] = pool["StakePool"]["basepool"]["total_value"] / 1000000000000
        try:
            newpool["sharePrice"] = newpool["totalStake"] / newpool["totalShares"]
        except:
            newpool["sharePrice"] = 0
        newpool["freeStake"] = phalaBlockchain.getPoolFree(pool["StakePool"]["basepool"]["pool_account_id"])
        newpool["pool_account_id"] = pool["StakePool"]["basepool"]["pool_account_id"]
        newpool["value_subscribers"] = pool["StakePool"]["basepool"]["value_subscribers"]
        newpool["withdraw_queue"] = pool["StakePool"]["basepool"]["withdraw_queue"]
        newpool["cd_workers"] = pool["StakePool"]["cd_workers"]
        newpool["lock_account"] = pool["StakePool"]["lock_account"]
        newpool["owner_reward_account"] = pool["StakePool"]["owner_reward_account"]
        newpool["workers"] = pool["StakePool"]["workers"]
        newpool["workers_count"] = len(pool["StakePool"]["workers"])
        newpool["white_list"] = phalaBlockchain.getPoolWhitelist(pool["StakePool"]["basepool"]["pid"])
        newpool["white_list_count"] = len(newpool["white_list"])
        newpool["stakers"] = getStakers(pool["StakePool"]["basepool"]["pid"])
        activeStaker = 0
        deadStaker = 0
        for stake in newpool["stakers"]:
            if stake["shares"] > 0:
                activeStaker = activeStaker + 1
            else:
                deadStaker = deadStaker + 1
        newpool["staker_count"] = len(newpool["stakers"])
        newpool["active_staker_count"] = activeStaker
        newpool["dead_staker_count"] = deadStaker
        print(newpool)
        
        try:
            poolsCol.insert_one(newpool)
        except:
            poolsCol.replace_one({"_id":pool["StakePool"]["basepool"]["pid"]},newpool)
 



def UpdateAllPools():
    poolcount = phalaBlockchain.getPoolCount()
    print("PoolCount: " + str(poolcount))
    for x in range(1,poolcount):
        pc = processControlCol.find_one({"_id":"pools"})
        pc["currentPool"] = x
        pc["status"] = "processing"
        processControlCol.replace_one({"_id":"pools"},pc)
        pool = phalaBlockchain.getBasePool(x)
        newpool = {}
        if "StakePool" in pool:
            print(pool["StakePool"]["basepool"]["pid"])
            newpool["_id"] = pool["StakePool"]["basepool"]["pid"]
            newpool["owner"] = pool["StakePool"]["basepool"]["owner"]
            if pool["StakePool"]["payout_commission"] == None:
                newpool["payoutCommission"] = 0
            else:
                newpool["payoutCommission"] = pool["StakePool"]["payout_commission"] / 10000
            if pool["StakePool"]["cap"] == None:
                newpool["cap"] = 0
            else:
                newpool["cap"] = pool["StakePool"]["cap"] / 1000000000000
            newpool["totalShares"] = pool["StakePool"]["basepool"]["total_shares"] / 1000000000000
            newpool["totalStake"] = pool["StakePool"]["basepool"]["total_value"] / 1000000000000
            try:
                newpool["sharePrice"] = newpool["totalStake"] / newpool["totalShares"]
            except:
                newpool["sharePrice"] = 0
            newpool["freeStake"] = phalaBlockchain.getPoolFree(pool["StakePool"]["basepool"]["pool_account_id"])
            newpool["pool_account_id"] = pool["StakePool"]["basepool"]["pool_account_id"]
            newpool["value_subscribers"] = pool["StakePool"]["basepool"]["value_subscribers"]
            newpool["withdraw_queue"] = pool["StakePool"]["basepool"]["withdraw_queue"]
            newpool["cd_workers"] = pool["StakePool"]["cd_workers"]
            newpool["lock_account"] = pool["StakePool"]["lock_account"]
            newpool["owner_reward_account"] = pool["StakePool"]["owner_reward_account"]
            newpool["workers"] = pool["StakePool"]["workers"]
            newpool["workers_count"] = len(pool["StakePool"]["workers"])
            newpool["white_list"] = phalaBlockchain.getPoolWhitelist(pool["StakePool"]["basepool"]["pid"])
            newpool["white_list_count"] = len(newpool["white_list"])
            newpool["stakers"] = getStakers(pool["StakePool"]["basepool"]["pid"])
            activeStaker = 0
            deadStaker = 0
            for stake in newpool["stakers"]:
                if stake["shares"] > 0:
                    activeStaker = activeStaker + 1
                else:
                    deadStaker = deadStaker + 1
            newpool["staker_count"] = len(newpool["stakers"])
            newpool["active_staker_count"] = activeStaker
            newpool["dead_staker_count"] = deadStaker
            
            try:
                poolsCol.insert_one(newpool)
            except:
                poolsCol.replace_one({"_id":pool["StakePool"]["basepool"]["pid"]},newpool)

    pc = processControlCol.find_one({"_id":"pools"})
    pc["currentPool"] = 0
    pc["status"] = "idle"
    processControlCol.replace_one({"_id":"pools"},pc)
            
def getStakers(pid):
    agg =   [
                {
                    '$match': {
                        'method': 'NftCreated', 
                        'pid': 195
                    }
                }, {
                    '$project': {
                        'account_id': 1, 
                        'nft_id': 1, 
                        'shares': 1
                    }
                }, {
                    '$group': {
                        '_id': '$account_id', 
                        'nft': {
                            '$max': '$nft_id'
                        }, 
                        'share': {
                            '$max': '$shares'
                        }
                    }
                }
            ]
    agg[0]['$match']['pid'] = pid
    nfts = eventsCol.aggregate(agg)
    tot = 0
    result = []
    for nft in nfts:
        event = eventsCol.find_one({"pid":pid,"method":"NftCreated","account_id":nft["_id"],"nft_id":nft["nft"]})
        tstake = {
            "account_id": event["account_id"],
            "shares": event["shares"]
        }
        result.append(tstake)
    return result
   
##################################
### Main

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
poolsCol = phaladb['pools']
eventsCol = phaladb['events']
processControlCol = phaladb['processcontrol'] 

#getStakers(195)



#UpdatePool(1674)