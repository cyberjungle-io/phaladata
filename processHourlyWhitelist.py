import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time


def calcHourly(hour): #hour for 0 epoch
    start = hour * 3600000
    end = (hour + 1) * 3600000
    hr = {"_id":hour}
    
    hr["minTime"] = start
    hr["maxTime"] = end


    ####### rewards
    agg = [
    {
        '$match': {
            '$or': [
                {
                    'method': 'PoolWhitelistCreated'
                }, {
                    'method': 'PoolWhitelistDeleted'
                }, {
                    'method': 'PoolWhitelistStakerAdded'
                }, {
                    'method': 'PoolWhitelistStakerRemoved'
                }
            ], 
            '$and': [
                {
                    'timestamp': {
                        '$gte': 1674061200000
                    }
                }, {
                    'timestamp': {
                        '$lt': 1674064800000
                    }
                }
            ]
        }
    }, {
        '$project': {
            'hour': {
                '$trunc': {
                    '$divide': [
                        '$timestamp', 3600000
                    ]
                }
            }, 
            'method': 1, 
            'timestamp': 1, 
            'section': 1, 
            'pha': 1
        }
    }, {
        '$sort': {
            'timestamp': -1
        }
    }, {
        '$group': {
            '_id': '$hour', 
            'mintime': {
                '$min': '$timestamp'
            }, 
            'maxtime': {
                '$max': '$timestamp'
            }, 
            'poolWhitelistCreated': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'PoolWhitelistCreated'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'poolWhitelistDeleted': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'PoolWhitelistDeleted'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'poolWhitelistStakerAdded': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'PoolWhitelistStakerAdded'
                            ]
                        }, 1, 0
                    ]
                }
            }, 
            'poolWhitelistStakerRemoved': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'PoolWhitelistStakerRemoved'
                            ]
                        }, 1, 0
                    ]
                }
            }
        }
    }
]

    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    
    try:
        rw = list(eventsCol.aggregate(agg))[0]

        
        hr["poolWhitelistCreated"] = rw["poolWhitelistCreated"]
        hr["poolWhitelistDeleted"] = rw["poolWhitelistDeleted"]
        hr["poolWhitelistStakerAdded"] = rw["poolWhitelistStakerAdded"]
        hr["poolWhitelistStakerRemoved"] = rw["poolWhitelistStakerRemoved"]
        
        
    except:
        
        hr["poolWhitelistCreated"] = 0
        hr["poolWhitelistDeleted"] = 0
        hr["poolWhitelistStakerAdded"] = 0
        hr["poolWhitelistStakerRemoved"] = 0
        







    eventsHourlyCol.update_one({"_id":hour}, {"$set": hr}, upsert=True)


    




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

#### Production
eventsCol = phaladb['events']    
eventsBlockRawCol = phaladb['eventblockraw']
eventsHourlyCol = phaladb['eventshourlys']  
blockstatsCol = phaladb['blockstats'] 

#calcHourly(470713)



