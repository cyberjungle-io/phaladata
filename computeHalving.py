import phalaBlockchain
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math


client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
processControlCol= phaladb['processcontrol']
eventsCol = phaladb['events']
hourlyCol = phaladb['eventshourlys']


#--------------------------------------------------------------------------------------


def CalculateNextHalving():
    processControl = processControlCol.find_one({"_id":"processControl"})
    block = processControlCol.find_one({"_id":"eventblockraw"})
    currentBlock = block["nextBlock"]
    halving = processControlCol.find_one({"_id":"halving"})

    if halving["nextBlock"] < currentBlock:
        halving["nextBlock"] = processControl["calcHalvingInterval"] + currentBlock



        evnt = eventsCol.find({'method': 'SubsidyBudgetHalved'}).sort("blockNumber",-1)
        lastHalfBlock = evnt[0]["blockNumber"]
        lastHalfTime = evnt[0]["timestamp"]

        
        halvingInterval = phalaBlockchain.getHalvingInterval()
        tm = (int(time.time()) - (86400 * 90)) * 1000
        print(tm)
        agg = [
                {
                    '$match': {
                        'minTime': {
                            '$gt': 0
                        }
                    }
                }, {
                    '$group': {
                        '_id': 1, 
                        'average': {
                            '$avg': '$avgBlockTime'
                        }
                    }
                }
            ]

        agg[0]["$match"]["minTime"]["$gt"] = tm
        avg = list(hourlyCol.aggregate(agg))[0]["average"]
        print(avg)
        nextHalvingBlock = lastHalfBlock + halvingInterval
        halvingDate = int(time.time() + ((nextHalvingBlock - currentBlock) * avg))

       
        halving["lastHalfBlock"] = lastHalfBlock
        halving["lastHalfTime"] = lastHalfTime
        halving["halvingInterval"] = halvingInterval
        halving["nextHalvingBlock"] = nextHalvingBlock
        halving["averageBlockTime"] = avg
        halving["halvingDate"] = halvingDate * 1000
        
        processControlCol.replace_one({"_id":"halving"},halving)
       


#CalculateNextHalving()