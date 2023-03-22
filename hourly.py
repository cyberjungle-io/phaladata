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
  
 

doProcess = True
while doProcess == True:
   
 
    pc = processControlCol.find_one({"_id":"processControl"})
    hrl =  hourlyCol.find().sort("_id",pymongo.DESCENDING).limit(1)
    if pc["hourly"] == "running":
        print(hrl[0]["_id"])
        ts = int(time.time())
        currHour = trunc(ts/3600)
        print(currHour)
        nextHour = hrl[0]["_id"] + 1
        if nextHour > currHour:
            nextHour = currHour
        processHourly.calcHourly(nextHour)
        time.sleep(30)
           
        
        
    
    if pc["hourly"] == "exit":
        doProcess = False
    
 ####################################################################################   




