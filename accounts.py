import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import miners
import time
import phalaUtils



def processAccounts():

    accounts, blockNumber = miners.getAccounts("http://10.2.4.1:3001")
   
    for account in accounts:
        account["delegated"] = phalaUtils.updateAccountDelegation(account["_id"])
        accountsCol.replace_one({"_id":account["_id"]},account,upsert=True)
        
    return blockNumber


client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
accountsCol = phaladb['accounts']
processControlCol = phaladb['processcontrol'] 
   
doProcess = True
while doProcess == True:
   
    pc = processControlCol.find_one({"_id":"processControl"})
    
    if pc["accounts"] == "running":
        blk = processAccounts()
        ac = {
            "_id": "accounts",
            "lastblock": blk
        }
        processControlCol.replace_one({"_id":"accounts"},ac)
        print(ac)
        time.sleep(120)
    if pc["accounts"] == "exit":
        doProcess = False
    time.sleep(15)

        
