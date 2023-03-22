#########
## Process raw event by block.  This is use to update the mining collection

import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import phalaBlockchain 


import time





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

def processEventBlock(blocknum):
    pevents = eventsCol.find({"blockNumber":blocknum})
    ts = 0
    
    
    print("Processing Blocknumber" + str(blocknum))
    
    
    
    for e in pevents:
        ts = e["timestamp"]
       
        # State
        if e["method"] == "WorkerEnterUnresponsive":
           
            tminer = minersCol.find_one({"_id":e["workerId"]})
            if tminer != None:
                tminer["state"] = "WorkerUnresponsive"
                minersCol.replace_one({"_id":tminer["_id"]},tminer)
        if e["method"] == "WorkerExitUnresponsive":
           
            tminer = minersCol.find_one({"_id":e["workerId"]})
            if tminer != None:
                tminer["state"] = "WorkerIdle"
                minersCol.replace_one({"_id":tminer["_id"]},tminer)


        if e["method"] == "SessionSettled":
            
            tminer = minersCol.find_one({"_id":e["workerId"]})
            
            if tminer != None:
                tminer["apr"] = getCurrentAPR(tminer)
                tminer["totalReward"] = tminer["totalReward"] + e["pha"]
                tminer["lastRewardTime"] = e["timestamp"]
                tminer["v"] = e["vscore"]
                tminer["vUpdatedAt"] = e["timestamp"]

                minersCol.replace_one({"_id":tminer["_id"]},tminer)
               
                e["apr"] = tminer["apr"]
                e["stake"] = tminer["stake"]
                
                eventsCol.replace_one({"_id":e["_id"]},e)
                

        if e["method"] == "BenchmarkUpdated":
            
            tminer = minersCol.find_one({"_id":e["workerId"]})
            if tminer != None:
                
                tminer["pInstant"] = e["pInstant"]
                tminer["challengeTimeLast"] = e["timestamp"]

                minersCol.replace_one({"_id":tminer["_id"]},tminer)
        if e["method"] == "WorkerStopped":
            
            tminer = minersCol.find_one({"_id":e["workerId"]})
            if tminer != None:
                
                phalaBlockchain.updateWorkerbyPubKey(tminer["pubkey"],blocknum)

               

        if e["method"] == "WorkerStarted"  or e["method"] == "SessionBound" or e["method"] == "SessionUnbound":
           
            phalaBlockchain.updateWorkerbyPubKey(e["pubkey"],blocknum)

        if e["method"] == "WorkingStarted":
            
            tminer = minersCol.find_one({"pubkey":e["pubkey"]})
            if tminer != None:
                
                tminer["stake"] = e["stake"]
                tminer["state"] = "WorkerIdle"
                minersCol.replace_one({"_id":tminer["_id"]},tminer)

                e["workerId"] = tminer["_id"]
                eventsCol.replace_one({"_id":e["_id"]},e)

    


client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol = phaladb['miners']
eventsCol = phaladb['events']

processControlCol = phaladb['processcontrol'] 
eventsBlockRawCol = phaladb['eventblockraw']




doProcess = True
while doProcess == True:
   
 
    pc = processControlCol.find_one({"_id":"processControl"})
    bl =  processControlCol.find_one({"_id":"processEvents"})
    if pc["processEvents"] == "running":
        lb = processControlCol.find_one({"_id":"eventItems"})
        if bl["nextBlock"] < lb["nextBlock"] -1:
            bl["nextBlock"] = bl["nextBlock"]+ 1
            print(bl["nextBlock"])
            if bl["nextWorkerUpdate"] < bl["nextBlock"]:
                phalaBlockchain.getWorkers()
                bl["nextWorkerUpdate"] = bl["nextBlock"] + pc["autoUpdateWorkersBlocks"]
                bl["lastWorkerUpdate"] = bl["nextBlock"]
            else:
                if pc["updateWorkers"] == True:
                    pc["updateWorkers"] = False
                    processControlCol.replace_one({"_id":"processControl"},pc)
                    print("about to get workers")
                    phalaBlockchain.getWorkers()
                    bl["lastWorkerUpdate"] = bl["nextBlock"]

            processEventBlock(bl["nextBlock"])
            

            processControlCol.replace_one({"_id":"processEvents"},bl)
            
            
        else:
            time.sleep(10)
    
    if pc["processEvents"] == "exit":
        doProcess = False
#miner = minersCol.find_one({"_id":"43E9fDcDY5um3ZeG3j6cEjqhRj9xBK9hdZVHsrsaXQN1vsBG"})
#apr = getCurrentAPR(miner)

