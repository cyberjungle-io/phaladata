import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time
import processEventBlock



#



#####################################################################
######### test events
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
processControlCol = phaladb['processcontrol'] 
eventsCol = phaladb['eventtest']    


doProcess = True
while doProcess == True:
   

    pc = processControlCol.find_one({"_id":"processControl"})
    bl =  processControlCol.find_one({"_id":"eventItemsTest"})
    if pc["eventItemsTest"] == "running":
        lb = processControlCol.find_one({"_id":"eventblockraw"})
        if bl["nextBlock"] < lb["nextBlock"]:
            bl["nextBlock"] = bl["nextBlock"]+ 1
            processControlCol.replace_one({"_id":"eventItemsTest"},bl)
            blk = eventsBlockRawCol.find_one({"blockNumber":bl["nextBlock"]})
            if blk != None:
                eventsCol.delete_many({"blockNumber":bl["nextBlock"]})
                processEventBlock.processBlock(blk)
        else:
            time.sleep(15)
    
    if pc["eventItemsTest"] == "exit":
        doProcess = False
    
 ####################################################################################         