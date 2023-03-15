import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time

def blockTime(blk):
    global lastTime
    newblk = {}
    block = eventsBlockRawCol.find_one({"_id":blk})
    if lastTime > 0:
        newblk = {
            "_id":blk,
            "blockElapseTime": trunc((block["timestamp"] - lastTime)/1000),
            "timestamp": block["timestamp"]
        }
    blockstatsCol.replace_one({"_id":blk},newblk,upsert=True)
    lastTime = block["timestamp"]



#####################################################################
######### Production events
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
processControlCol = phaladb['processcontrol'] 
blockstatsCol = phaladb['blockstats'] 

lastTime = 0

doProcess = True
while doProcess == True:
   
 
    pc = processControlCol.find_one({"_id":"processControl"})
    bl =  processControlCol.find_one({"_id":"processBlocks"})
   
    if pc["processBlocks"] == "running":
        lastblk = processControlCol.find_one({"_id":"eventblockraw"})
        if bl["nextBlock"] < lastblk["nextBlock"]:
            bl["nextBlock"] = bl["nextBlock"] + 1
            blockTime(bl["nextBlock"])
            processControlCol.replace_one({"_id":"processBlocks"},bl)
        else:
            doProcess = False       

    
    if pc["processBlocks"] == "exit":    
        doProcess = False