# process the first 1 million blocks
import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time



def getEvents(baseUrl,blockNumber):
    global doProcess
    headers = {'Content-type': 'application/json'} 
    start_time = time.time()
    try:
        url = baseUrl +"/events?number=" + str(blockNumber)
        #print(url)
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

doProcess = True

while doProcess == True:
    pc = processControlCol.find_one({"_id":"processControl"})
    bl =  processControlCol.find_one({"_id":"eventBlockFirst"})
    
    bl["nextBlock"] = bl["nextBlock"]+ 1
    ge = getEvents("http://10.2.2.49:3001",bl["nextBlock"])
    if ge == True:
        processControlCol.replace_one({"_id":"eventBlockFirst"},bl)
    else:
        doProcess = False

        
   
        



