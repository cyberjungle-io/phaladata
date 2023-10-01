from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math


def getCurrentAPR(miner):
    apr = 0
    try:
        eventresult = list(eventsCol.find({"method": "SessionSettled","workerId":miner["_id"]}).sort("timestamp",pymongo.DESCENDING).limit(2))
        
        if len(eventresult) == 2:
            etime = (eventresult[0]["timestamp"] - eventresult[1]["timestamp"])/60000
            apr = eventresult[0]["pha"] / miner["stake"] / etime * 525960 * 100
    except:
        apr = 0  
    return apr

def getHalvingInterval():
    wkrid = conn[0].query('PhalaComputation', 'ComputingHalvingInterval', [])
    #print("worker id: " + str(wkrid.value))
    
    return wkrid.value


def getWorkerId(pubkey):
    wkrid = conn[0].query('PhalaComputation', 'WorkerBindings', [pubkey])
    #print("worker id: " + str(wkrid.value))
    
    return wkrid.value

def getStake(wkid):
    stake = conn[0].query('PhalaComputation', 'Stakes', [wkid])
    print("stake id: " + str(stake))
    return stake.value / 1000000000000

def getSession(wkid):
    ses = conn[0].query('PhalaComputation', 'Sessions', [wkid])
    return ses.value 

def getPoolId(pubkey):
    pid = conn[0].query('PhalaStakePoolv2', 'WorkerAssignments', [pubkey])
    print(pid)
    return pid.value 


def getMiners(worker):
    
    workerid = getWorkerId(worker["pubkey"])
    pid = getPoolId(worker["pubkey"])
    if workerid != None:
        print("pubkey: " + str(worker["pubkey"]))
        #print("getMiner workerid: " + workerid)
        ses = getSession(workerid)
        if ses["state"] != "Ready":
            stake = getStake(str(workerid))
            if stake == None:
                stake = 0
        else:
            stake = 0
        

        # print("Session: " + str(ses))
        # print()
        miner = minersCol.find_one({"_id":workerid})
        if miner == None:
            miner = {
                "_id": workerid,
                "pubkey":worker["pubkey"],
                "totalReward": int(ses["stats"]["total_reward"])  / 1000000000000,
                "lastRewardTime": time.time(),
                "apr": 0
            }
            minersCol.insert_one(miner)
    
        if miner["totalReward"] != int(ses["stats"]["total_reward"])  / 1000000000000:
            miner["lastRewardTime"] = time.time()

        if ses["state"] == "Ready":
            ct = math.trunc(time.time())
            tm = ct - 173279
            if ses["v_updated_at"] < tm:
                miner["state"] = "Inactive"
            else:
                miner["state"] = "Ready"
        else:
            miner["state"] = ses["state"]
        
        
        
            
            
        miner["pid"] = pid
        miner["ve"] = int(ses["ve"]) /(2**64)
        miner["v"] = int(ses["v"]) /(2**64)
        miner["vUpdatedAt"] = ses["v_updated_at"]
        miner["pInit"] = ses["benchmark"]["p_init"]
        miner["pInstant"] = ses["benchmark"]["p_instant"]
        miner["iterations"] = ses["benchmark"]["iterations"]
        miner["workerStartTime"] = ses["benchmark"]["working_start_time"]
        miner["challengeTimeLast"] = ses["benchmark"]["challenge_time_last"]
        miner["coolDownStart"] = ses["cool_down_start"]
        miner["totalReward"] = int(ses["stats"]["total_reward"])  / 1000000000000
        miner["stake"] = stake
        miner["operator"] = worker["operator"]
        miner["confidenceLevel"] = worker["confidenceLevel"]
        miner["runtimeVersion"] = worker["runtimeVersion"]
        if miner["stake"] > 0:
            miner["apr"] = getCurrentAPR(miner)
        minersCol.replace_one({"_id":workerid},miner)

    
def getWorkerPublicKey(bnbr):
    global workerBlockNumber
    global workerkey
    workerkey = {}
    url = baseUrl + "/workers" 
    headers = {'Content-type': 'application/json'}
    r = requests.get(url, headers=headers)        
    workerArray = r.json()
    workerBlockNumber = bnbr
    # print(json.dumps(tmpjson["result"][0],indent=4))
    # print("Number of workers: " + str(len(tmpjson["result"])) )
    for w in workerArray["result"]:
        workerkey[w["pubkey"]] = w


def getWorkers():
    global workerBlockNumber
    global workerArray
    url = baseUrl + "/workers"

    #try:
    headers = {'Content-type': 'application/json'}
    r = requests.get(url, headers=headers)        
    workerArray = r.json()
    workerBlockNumber = workerArray["blockNumber"]
    # print(json.dumps(tmpjson["result"][0],indent=4))
    # print("Number of workers: " + str(len(tmpjson["result"])) )
    for w in workerArray["result"]:
        getMiners(w)

def updateWorkerbyPubKey(pubkey,blocknumber):
    global workerBlockNumber
    global workerkey
    url = baseUrl + "/workers"

    #try:
    print("workerblockNumber: " + str(workerBlockNumber))
    if blocknumber != workerBlockNumber:
        print("getting getWorkerPublicKey")
        getWorkerPublicKey(blocknumber)
    print(pubkey)
    try:
        getMiners(workerkey[pubkey])
    except:
        print("Failed Public Key")
    
   # except:
    #    print("get failed")
    #    return {}
    
def getBasePool(pid):
    pool = conn[0].query('PhalaBasePool', 'Pools', [pid])
    #print("worker id: " + str(wkrid.value))
    
    return pool.value

def getPoolCount():
    pool = conn[0].query('PhalaBasePool', 'PoolCount', [])
    #print("worker id: " + str(wkrid.value))
    
    return pool.value
def getTotalIssuance():
    issue = substrate.query('Balances', 'TotalIssuance', [])
    #print("worker id: " + str(wkrid.value))
    
    return issue.value
def getPoolFree(acct):
    free = conn[0].query('Assets', 'Account', [10000,acct])
    #print("worker id: " + str(wkrid.value))
    
    if free.value == None:
        return 0
    else:
        return free["balance"].value   / 1000000000000   

def getPoolWhitelist(pid):
    wl = conn[0].query('PhalaBasePool', 'PoolContributionWhitelists', [pid])
    #print("worker id: " + str(wkrid.value))
    if wl == None:
        return []
    else:
        return wl.value



    
#####################################################################    
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol= phaladb['miners']
eventsCol = phaladb['events']
baseUrl = "http://10.2.4.1:3001"
workerBlockNumber = 0
workerkey = {}

conn = []


try:
    substrate = SubstrateInterface(
        url="ws://10.2.4.1:9944",
        type_registry_preset="substrate-node-template"
    )
    conn.append(substrate)
except ConnectionRefusedError:
    print("⚠️ No local Substrate node running, try running 'start_local_substrate_node.sh' first")
    exit()

#getWorkers()  #gets all workers and mining data


#print('Stake: ', getStake()) 
#print('Stake: ', getSession()) 


# print(str(math.trunc(time.time())))
#pid = getPoolId("0x2e57de207bf26b88586aa05960f75d0d1c1db48270c75b4a5462fb34723f6d1b")
# wkrid = conn[0].query('PhalaStakePoolv2', 'WorkerAssignments', ["0x2e57de207bf26b88586aa05960f75d0d1c1db48270c75b4a5462fb34723f6d1b"])
# print(wkrid)
# stake = conn[0].query('PhalaComputation', 'Sessions', [wkrid])
# print(stake) 
#

#print(getMiners('0x62f107c9a5ba567f491d312a24280b69a6d5eed52a5f26757da18e888a9b7711'))

issue = getTotalIssuance()
print(issue)



""" result = substrate.query_map('System', 'Account')

for account, account_info in result:
    if account.value == "44RGVAd8sadC7Bitqe3tj5NTeXMrojE1NqGecdBiQX2bLUbG":
        print(f"Free balance of account '{account.value}': {account_info.value}")


        era_stakers = substrate.query_map(
    module='Staking',
    storage_function='ErasStakers',
    params=[2100]
)

print(era_stakers.value) """

#print(getWorkerId("0x1c0a2de16acbcde5e6a217b230efddda301a2288a5f39c59a65bc787f4def165"))