import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval



#baseUrl = "http://localhost:3001"
def getMiners(baseUrl,blockNumber,minerBindings,workers,workerAssignments,stakes):
    unknowncnt = 0
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/miners?number=" + str(blockNumber)
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        #print(json.dumps(tmpjson[0],indent=4))
    except:
        print("get failed")
        return {}
    #minerkeys = tmpjson["result"].keys()
    minerRec = {}
    jarray = []

    for key in tmpjson["result"].keys():
        tminer = tmpjson["result"][key]
        minerRec = {}
        minerRec["_id"] = key
        minerRec["state"] = tminer["state"]
        try:
            minerRec["ve"] = int(tminer["ve"]) /(2**64) 
        except:
            minerRec["ve"] = literal_eval(tminer["ve"])/(2**64) 
        try:
            minerRec["v"] = int(tminer["v"])/(2**64) 
        except:
            minerRec["v"] = literal_eval(tminer["v"]) /(2**64) 
        minerRec["vUpdatedAt"] = convertDate(tminer["vUpdatedAt"])
        minerRec["pInit"] = tminer["benchmark"]["pInit"]
        minerRec["pInstant"] = tminer["benchmark"]["pInstant"]
        minerRec["iterations"] = tminer["benchmark"]["iterations"]
        minerRec["miningStartTime"] = convertDate(tminer["benchmark"]["miningStartTime"])
        minerRec["challengeTimeLast"] = convertDate(tminer["benchmark"]["challengeTimeLast"])
        minerRec["coolDownStart"] = tminer["coolDownStart"]
        try:
            minerRec["totalReward"] = int(tminer["stats"]["totalReward"]) / 1000000000000
        except:
            minerRec["totalReward"] = literal_eval(tminer["stats"]["totalReward"]) / 1000000000000
        
        if key in stakes["result"]:
            minerRec["stake"] = int(stakes["result"][key])  / 1000000000000
        else:
            minerRec["stake"] = 0
        
        if key not in minerBindings["result"]:
            #print(key)
            #print(json.dumps(tmpjson["result"][key],indent=4))
            unknowncnt = unknowncnt + 1
            minerRec["pubkey"] = "unknown"
            minerRec["pid"] = "none"
            minerRec["runtimeVersion"] = 0
            minerRec["confidenceLevel"] = 0
            minerRec["operator"] = "unknown"
            minerRec["workerLastUpdated"] = 0


        else:
            pubkey = minerBindings["result"][key]
            minerRec["pubkey"] = pubkey
            if pubkey in workerAssignments["result"]:
                minerRec["pid"] = workerAssignments["result"][pubkey]
            else:
                minerRec["pid"] = "None"
            if pubkey in workers:
                minerRec["runtimeVersion"] = workers[pubkey]["runtimeVersion"]
                minerRec["confidenceLevel"] = workers[pubkey]["confidenceLevel"]
                minerRec["operator"] = workers[pubkey]["operator"]
                minerRec["workerLastUpdated"] = convertDate(workers[pubkey]["lastUpdated"])
            else:
                minerRec["runtimeVersion"] = 0
                minerRec["confidenceLevel"] = 0
                minerRec["operator"] = "unknown"
                minerRec["workerLastUpdated"] = 0
            
        jarray.append(minerRec)
            
        #print(minerBindings["result"][key])
    print(unknowncnt)
    return jarray

def convertDate(ep):
   
    if ep > 10000000000:
        ep = trunc(ep/1000)
        
    
    return datetime.datetime.fromtimestamp(ep).isoformat()
def getHeader(baseUrl):
    headers = {'Content-type': 'application/json'} 
    
    #try:
    url = baseUrl +"/header"
    r = requests.get(url, headers=headers)        
    tmpjson = r.json()
    #print(json.dumps(tmpjson,indent=4))
    rec1 = {}
    rec1["blockNumber"] = tmpjson["blockNumber"]
    rec1["blockHash"] = tmpjson["blockHash"]
    rec1["timestamp"] = convertDate(tmpjson["timestamp"])
    
    return rec1
        #print(json.dumps(tmpjson[0],indent=4))
    """ except:
        print("get failed")
        return {"failed":""}

    print(tmpjson["blockNumber"]) """
def getMinerBindings(baseUrl):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/miner_bindings"
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        return tmpjson
        #print(json.dumps(tmpjson[0],indent=4))
    except:
        print("get failed")
        return {}

    print(tmpjson["blockNumber"])

def getStakes(baseUrl):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/stakes"
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        return tmpjson
        #print(json.dumps(tmpjson[0],indent=4))
    except:
        print("get failed")
        return {}

def getWorkers(baseUrl):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/workers"
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        return tmpjson["result"]
        #print(json.dumps(tmpjson[0],indent=4))
    except:
        print("get failed")
        return {}


    print(tmpjson["blockNumber"])

def getWorkerAssignments(baseUrl):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/worker_assignments"
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        return tmpjson
        #print(json.dumps(tmpjson[0],indent=4))
    except:
        print("get failed")
        return {}


    print(tmpjson["blockNumber"])
def getWorkerDb():
    twork = workersCol.find()
    newWork = {}
    for work in twork:
        newWork[work["pubkey"]] = work

    return newWork

def getStakePools(baseUrl,poolStakers,h):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/stake_pools?number=" + str(h["blockNumber"])
        
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
    except:
        print("get failed")
        return {}
    newRec = {}
    newArray = []
    for pool in tmpjson["result"]:
        newRec = {}
        newRec["_id"] = pool["pid"]
        
        newRec["owner"] = pool["owner"]
        if (pool["payoutCommission"] == None):
            newRec["payoutCommission"] = 0
        else:
            newRec["payoutCommission"] = pool["payoutCommission"]/10000
        try:
            newRec["ownerReward"] = int(pool["ownerReward"])  / 1000000000000
        except:
            newRec["ownerReward"] = literal_eval(pool["ownerReward"])  / 1000000000000
        if pool["cap"] == None:
            newRec["cap"] = 0
        else:
            try:
                newRec["cap"] = int(pool["cap"])  / 1000000000000
            except:
                newRec["cap"] = literal_eval(pool["cap"])  / 1000000000000
        try:
            newRec["rewardAcc"] = int(pool["rewardAcc"])  / (2**64)
        except:
            newRec["rewardAcc"] = literal_eval(pool["rewardAcc"])  / (2**64)
        try:
            newRec["totalShares"] = int(pool["totalShares"])  / 1000000000000
        except:
            newRec["totalShares"] = literal_eval(pool["totalShares"])  / 1000000000000
        try:
            newRec["totalStake"] = int(pool["totalStake"])  / 1000000000000
        except:
            newRec["totalStake"] = literal_eval(pool["totalStake"])  / 1000000000000
        try:
            newRec["freeStake"] = int(pool["freeStake"])  / 1000000000000
        except:
            newRec["freeStake"] = literal_eval(pool["freeStake"])  / 1000000000000
        try:
            newRec["releasingStake"] = int(pool["releasingStake"])  / 1000000000000
        except:
            newRec["releasingStake"] = literal_eval(pool["releasingStake"])  / 1000000000000
        newRec["workers"] = pool["workers"]
        qarray = []
        for queue in pool["withdrawQueue"]:
            tq = {}
            tq["account"] = queue["user"]
            tq["startTime"] = convertDate(queue["startTime"])
            
            try:
                tq["shares"] = int(queue["shares"])  / 1000000000000
            except:
                tq["shares"] = literal_eval(queue["shares"])  / 1000000000000
            qarray.append(tq)
        newRec["withdrawQueue"] = qarray
        if newRec["_id"] in poolStakers:
            newRec["stakers"] = poolStakers[newRec["_id"]]
        else:
            newRec["stakers"] = []
        for p in newRec["stakers"]:
            p["claimable"] = (newRec["rewardAcc"] * p["shares"]) - (p["rewardDebt"] - p["availableRewards"])
        
        #if pool["pid"] == 1674:
         #   print(newRec)
      
        newArray.append(newRec)

    return newArray
    #print(json.dumps(tmpjson[0],indent=4))
    
def getPoolStakers(baseUrl,h):
    headers = {'Content-type': 'application/json'} 
    try:
        url = baseUrl +"/pool_stakers?number=" + str(h["blockNumber"])
       
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        
    except:
        print("get failed")
        return {}
    newRec = {}
        
    stakers = {}
    for pool in tmpjson["result"]:
        newRec = {}
        newRec["account"] = pool[1]["user"]
        try:
            newRec["locked"] = int(pool[1]["locked"])  / 1000000000000
        except:
            newRec["locked"] = literal_eval(pool[1]["locked"])  / 1000000000000
        try:
            newRec["shares"] = int(pool[1]["shares"])  / 1000000000000
        except:
            newRec["shares"] = literal_eval(pool[1]["shares"])  / 1000000000000
        try:
            newRec["availableRewards"] = int(pool[1]["availableRewards"])  / 1000000000000
        except:
            newRec["availableRewards"] = literal_eval(pool[1]["availableRewards"])  / 1000000000000
        try:
            newRec["rewardDebt"] = int(pool[1]["rewardDebt"])  / 1000000000000
        except:
            newRec["rewardDebt"] = literal_eval(pool[1]["rewardDebt"])  / 1000000000000
        if newRec["locked"] > 0 or newRec["shares"]or newRec["availableRewards"] or newRec["rewardDebt"]:
            tkey = pool[0][0]
            #print(tkey)
            if tkey in stakers:
               # print("Key in staker")
                stakers[tkey].append(newRec)
            else:
                
                stakers[tkey] = []
                stakers[tkey].append(newRec)

        
    #print(json.dumps(stakers[1673],indent=4))
    
    return stakers
        #print(json.dumps(tmpjson[0],indent=4))

    print(tmpjson["blockNumber"])
def getAccounts(baseUrl):
    headers = {'Content-type': 'application/json'} 
    blockNumber = 0
    try:
        url = baseUrl +"/accounts"
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        blockNumber = tmpjson["blockNumber"]
    except:
        print("get failed")
        return {}
    tarray = []
    for rec in tmpjson["result"]:
        tRec = {}
        tRec["_id"] = rec[0]
        
        try:
            tRec["free"] = int(rec[1]["free"])  / 1000000000000
        except:
            tRec["free"] = literal_eval(rec[1]["free"])  / 1000000000000
        
        try:
            tRec["reserved"] = int(rec[1]["reserved"])  / 1000000000000
        except:
            tRec["reserved"] = literal_eval(rec[1]["reserved"])  / 1000000000000

        try:
            tRec["miscFrozen"] = int(rec[1]["miscFrozen"])  / 1000000000000
        except:
            tRec["miscFrozen"] = literal_eval(rec[1]["miscFrozen"])  / 1000000000000
        
        try:
            tRec["feeFrozen"] = int(rec[1]["feeFrozen"])  / 1000000000000
        except:
            tRec["feeFrozen"] = literal_eval(rec[1]["feeFrozen"])  / 1000000000000
        
        tarray.append(tRec)
    
    return tarray, blockNumber
        
       
        
        
        
        #return tmpjson
        #print(json.dumps(tmpjson[0],indent=4))
    


    print(tmpjson["blockNumber"])

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol = phaladb['miners']
workersCol = phaladb['workers']
poolsCol = phaladb['pools']
poolStakersCol = phaladb['poolstakers']
accountsCol = phaladb['accounts']
"""

minerBindings = getMinerBindings("http://10.2.2.67:3001")
print("Got miner bindings")
stakes = getStakes("http://10.2.2.67:3001")
print("got stakes")

workers = getWorkers("http://10.2.2.67:3001")
#print("got workers")
workersCol.insert_many(workers["result"])

workers = getWorkerDb()
#print(workers.keys())

workerAssignments = getWorkerAssignments("http://10.2.2.67:3001")
print("got worker assignments")
poolStakers = getPoolStakers("http://10.2.2.67:3001")
minersCol.delete_many({})
minersCol.insert_many(getMiners("http://10.2.2.67:3001"))


 

accountsCol.delete_many({})
#accountsCol.insert_many(getAccounts("http://10.2.2.67:3001"))
#poolStakersCol.insert_many(getPoolStakers())
 """
#h = getHeader("http://10.2.2.65:3001")
#ps = getPoolStakers("http://10.2.2.65:3001",h)
#getStakePools("http://10.2.2.65:3001",ps,h)
#poolsCol.delete_many({})
#poolsCol.insert_many()