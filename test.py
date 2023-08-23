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
    

# get prb peers from /ptp/discover in the prb monitor service at 10.2.3.2:3000. 
def getPrbLifecyclePeerId():
    r = requests.post('http://10.2.4.1:3000/ptp/discover')
    peers = r.json()
    # print(json.dumps(peers, indent=4, sort_keys=True))
    # print("Peer ID: " + peers["lifecycleManagers"][0]["peerId"])
    return peers["lifecycleManagers"][0]["peerId"]

# gets the peerid from getPrbLifecyclePeer, then using the peerid it gets the workers  from the ListWorker endpoint
def getPrbWorkers():
    peerid = getPrbLifecyclePeerId()
    r = requests.post('http://10.2.4.1:3000/ptp/proxy/' + peerid + '/ListWorker')
    workers = r.json()
    #print(json.dumps(workers, indent=4, sort_keys=True))
    #print the count of data.worker array
    #print("Worker Count: " + str(len(workers["data"]["workers"])))

import requests
import json

def prbGetWorkerStatus():
    peerid = getPrbLifecyclePeerId()
    url = 'http://10.2.4.1:3000/ptp/proxy/' + peerid + '/GetWorkerStatus'
    headers = {'Content-Type': 'application/json'}
    
    data = {"ids":['2f70fe37-bc58-46b7-b20b-81231fe9449d']}
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        result = response.json()
        
        print("response: " + json.dumps(result["data"]["workerStates"][0], indent=4, sort_keys=True))
        
    else:
        print(f"Worker creation failed. Status code: {response.status_code}")
        print(response.text)

prbGetWorkerStatus()
# getStakeByPool(1674,1674157824011)
#getPoolsByStaker("44RGVAd8sadC7Bitqe3tj5NTeXMrojE1NqGecdBiQX2bLUbG",16784157824011)