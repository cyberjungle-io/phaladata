import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import time






def processBlock(block):
    ######### processes by raw block  ###########
    for eventNum in range(len(block["result"])):
        event = block["result"][eventNum]
        newEvent = {}
        newEvent["_id"] = str(block["blockNumber"]) + "-" + str(eventNum)
        newEvent["eventNumber"] = eventNum
        newEvent["blockNumber"] = block["blockNumber"]
        newEvent["timestamp"] = block["timestamp"]
        newEvent["method"] = event["method"]
        newEvent["section"] = event["section"]
        newEvent["data"] = event["data"]

        processed = False



def processEvents(blknum): # calculating apr
    events = eventsCol.find({"blockNumber":blknum,"$or":[{"method":"SessionSettled"},{"method":"MinerSettled"}]})

    for e in events:
        try:
            bound = eventsCol.find_one({"workerId":e["workerId"],"$or":[{"method":"SessionBound"},{"method":"MinerBound"},{"method":"SessionUnbound"},{"method":"MinerUnbound"}]})
            #print("pubkey: " + bound["pubkey"] + "    workerId: " + e["workerId"])


            st = list(eventsCol.find({"pubkey":bound["pubkey"],"$or":[{"method":"WorkingStarted"},{"method":"MiningStarted"}],"timestamp":{"$lt":e["timestamp"]}}).sort("timestamp",pymongo.DESCENDING))
            
            if len(st) == 0:
                #print("find by miner")
                #print(e)
                miner = minersCol.find({"_id":e["workerId"]})
                stake = miner[0]["stake"]
            else:
                stake = st[0]["stake"]

            apr = 0
            try:
                eventresult = list(eventsCol.find({"method": "SessionSettled","workerId":e["workerId"]}).sort("timestamp",pymongo.DESCENDING).limit(2))
                
                if len(eventresult) == 2:
                    etime = (eventresult[0]["timestamp"] - eventresult[1]["timestamp"])/60000
                    apr = eventresult[0]["pha"] / stake / etime * 525960 * 100
            except:
                apr = 0  

            #print(e["_id"] + "   Stake: " + str(stake) + "   apr: " + str(apr))
            e["apr"] = apr
            e["stake"]= stake
            eventsCol.replace_one({"_id":e["_id"]},e)
        except:
            print("failed")


      




####################################################################
########  Main 

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
#### Production
eventsCol = phaladb['events']    
eventErrorsCol = phaladb['eventerrors']  
minersCol = phaladb['miners']


#### Test
# eventsCol = phaladb['eventtest']   
# eventErrorsCol = phaladb['eventtesterrors']  
# blk = eventsBlockRawCol.find_one({"_id":3093889})
# processBlock(blk)

#processEvents(3189974)