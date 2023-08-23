import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time


def blockTime(blk):
    lblk = blk - 1
    block = eventsBlockRawCol.find_one({"_id":blk})
    lastBlock = eventsBlockRawCol.find_one({"_id":lblk})

    stat = blockstatsCol.find_one({"_id":blk})
    if stat == None:
        stat = {
            "_id":blk,
            "blockElapseTime": trunc((block["timestamp"] - lastBlock["timestamp"])/1000),
            "timestamp": block["timestamp"]
        }
    else:
        stat["blockElapseTime"] = trunc((block["timestamp"] - lastBlock["timestamp"])/1000)
        stat["timestamp"] = block["timestamp"]
    
    blockstatsCol.replace_one({"_id":blk},stat,upsert=True)
    
   

def getEvents(baseUrl,blockNumber):
    global doProcess
    headers = {'Content-type': 'application/json'} 
    start_time = time.time()
    try:
        url = baseUrl +"/events?number=" + str(blockNumber)
        print(url)
        r = requests.get(url, headers=headers)        
        tmpjson = r.json()
        tmpjson["_id"] = tmpjson["blockNumber"]
        
        tmpjson["etime"] = time.time() - start_time
        tmpjson["eventCount"] = len(tmpjson["result"])
        eventsBlockRawCol.insert_one(tmpjson)

        return True
        
    except Exception as e:
       
        
        print("get failed" + str(e))
        return False
   

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
processControlCol = phaladb['processcontrol']
blockstatsCol = phaladb['blockstats'] 

doProcess = True

while doProcess == True:
    pc = processControlCol.find_one({"_id":"processControl"})
    bl =  processControlCol.find_one({"_id":"eventblockraw"})
    if pc["eventblockraw"] == "running":
        bl["nextBlock"] = bl["nextBlock"]+ 1
        ge = getEvents("http://10.2.4.1:3001",bl["nextBlock"])
        
        if ge == True:
            blockTime(bl["nextBlock"])
            processControlCol.replace_one({"_id":"eventblockraw"},bl)
        else:
            time.sleep(30)
    else:
        time.sleep(10)
        
    if pc["eventblockraw"] == "exit":
        doProcess = False
   
        



