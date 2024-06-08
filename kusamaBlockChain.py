from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math
   
#####################################################################    
client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

baseUrl = "http://10.2.4.1:3001"
workerBlockNumber = 0
workerkey = {}

conn = []
def getBlockHeight():
    wkrid = conn[0].query('System', 'Number', [])
    #print("worker id: " + str(wkrid.value))
    
    return wkrid.value

try:
    substrate = SubstrateInterface(
        url="ws://10.2.4.1:9945",
        type_registry_preset="substrate-node-template"
    )
    conn.append(substrate)
except ConnectionRefusedError:
    print("⚠️ No local Substrate node running, try running 'start_local_substrate_node.sh' first")
    exit()


x = getBlockHeight()
print(x)