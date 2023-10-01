from pymongo import MongoClient
import pymongo
import json
import phalaBlockchain 
import time

def doProcessSnapshot():
    client = MongoClient('10.2.2.11', 27017)
    phaladb = client['phala']

    poolCol = phaladb['poolsnapshot'] 
    vaultCol = phaladb['vaultsnapshot']
    chainCol = phaladb['currentchainstats']
    eventsCol = phaladb['events']

    #get the last pool
    poolCount = phalaBlockchain.getPoolCount()
    print ("pool count: " + str(poolCount))

    #loop from 0 to poolCount
    cnt = 0
    value = 0
    for i in range(0,poolCount):
        #print("processing pool: " + str(i))
        pool = phalaBlockchain.getBasePool(i)
        if pool != None:
            #print the pool
            #print(pool)
            if "StakePool" in pool:
                tmpPool = pool["StakePool"]["basepool"]
                tmpPool["_id"] = tmpPool["pid"]
                tmpPool["total_shares"] = tmpPool["total_shares"] / 1000000000000
                tmpPool["total_value"] = tmpPool["total_value"] / 1000000000000
                if tmpPool["total_shares"] == 0:
                    tmpPool["sharePrice"] = 0
                else:
                    tmpPool["sharePrice"] = tmpPool["total_value"] / tmpPool["total_shares"]
                    
                cnt = cnt + 1
                poolCol.replace_one({"_id":tmpPool["_id"]},tmpPool,upsert=True)
                value = value + tmpPool["total_value"]
                print("pool: " + str(tmpPool["_id"]))
                
            if "Vault" in pool:
                print(pool)
                tmpvault = pool["Vault"]["basepool"]
                tmpvault["_id"] = tmpvault["pid"]
                tmpvault["total_shares"] = tmpvault["total_shares"] / 1000000000000
                tmpvault["total_value"] = tmpvault["total_value"] / 1000000000000
                if tmpvault["total_shares"] == 0:
                    tmpvault["sharePrice"] = 0
                else:
                    tmpvault["sharePrice"] = tmpvault["total_value"] / tmpvault["total_shares"]
                vaultCol.replace_one({"_id":tmpvault["_id"]},tmpvault,upsert=True)
                print("vault: " + str(tmpvault["_id"]))
                
        else:
            print("pool: " + pool + " None")


    #get the current chain stats for the current epoch time in miliseconds
    totalIssuance = phalaBlockchain.getTotalIssuance()/1000000000000

    tmpIssue = {"_id":"totalIssuance","totalIssuance":totalIssuance,"timestamp":int(time.time())*1000}

    chainCol.replace_one({"_id":"totalIssuance"},tmpIssue,upsert=True)

    #get the last 24 hour RewardReceived events with aggregate summing to_staker and to_owner

    tm = int(time.time()*1000) - 86400000

    agg = [
        {
            '$match': {
                'method': 'RewardReceived', 
                'timestamp': {
                    '$gt': tm
                }
            }
        }, {
            '$group': {
                '_id': 1, 
                'to_owner': {
                    '$sum': '$to_owner'
                },
                'to_staker': {
                    '$sum': '$to_staker'
                },
                'count': {
                    '$sum': 1,
                        },
            }
        }
    ]

    result = list(eventsCol.aggregate(agg))
    print(result)
    delegationApr = (result[0]["to_staker"]*365) / value  * 100
    workerApr = (result[0]["to_owner"] + result[0]["to_staker"])*365 / value * 100


    #store 24 hour totals in currentchainstats
    hr24 = {"_id":"24HourRewardStats",
                "to_owner":result[0]["to_owner"],
                "to_staker":result[0]["to_staker"],
                "rewardCount":result[0]["count"],
                "delegationApr":delegationApr,
                "workerApr":workerApr,
                "timestamp":int(time.time())*1000,
                "totalDelegation":value
                }
    chainCol.replace_one({"_id":"24HourRewardStats"},hr24,upsert=True)

    client.close()
