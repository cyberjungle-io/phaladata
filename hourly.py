import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
import pymongo
from ast import literal_eval
import time
import processHourly



#####################################################################
######### Production events
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
hourlyCol = phaladb['eventshourlys']
processControlCol = phaladb['processcontrol'] 
eventsBlockRawCol = phaladb['eventblockraw']  
 

doProcess = True
lastFullBlock = 0 

while doProcess == True:
   
 
    pc = processControlCol.find_one({"_id":"processControl"})
    hrl =  hourlyCol.find().sort("_id",pymongo.DESCENDING).limit(1)
    processevents = processControlCol.find_one({"_id":"processEvents"})
    rawblock = eventsBlockRawCol.find_one({"_id":processevents["nextBlock"]})
    ts = rawblock["timestamp"]
    if pc["hourly"] == "running":
        print(hrl[0]["_id"])
       
        currHour = trunc(ts/3600000)
        print(currHour)
        nextHour = hrl[0]["_id"] + 1
        if nextHour > currHour:
            nextHour = currHour
            if lastFullBlock != nextHour -1:
                lastFullBlock = nextHour -1
                print("process LastFullBlock")
                processHourly.calcHourly(lastFullBlock)

        processHourly.calcHourly(nextHour)
        time.sleep(60)
           
        
        
    
    if pc["hourly"] == "exit":
        doProcess = False
    
 ####################################################################################   




