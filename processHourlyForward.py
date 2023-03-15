import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time
import processHourly



#####################################################################
######### Production events
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
processControlCol = phaladb['processcontrol'] 


doProcess = True
while doProcess == True:
   
 
    
    bl =  processControlCol.find_one({"_id":"processHourlyForward"})
   
    curHour = trunc(time.time()/3600)
    if bl["nextBlock"] < curHour:
        bl["nextBlock"] = bl["nextBlock"] + 1
        processHourly.calcHourly(bl["nextBlock"])
        processControlCol.replace_one({"_id":"processHourlyForward"},bl)
    else:
        doProcess = False       
    