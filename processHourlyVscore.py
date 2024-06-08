import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time


def calcHourly(hour): 
    #calc starting 24 hours back
    start = (hour - 24) * 3600000
    end = (hour + 1) * 3600000
    hr = {"_id":hour}
    
    #hr["minTime"] = (hour) * 3600000
    #hr["maxTime"] = end


    ####### rewards
    agg = [
    {
        '$match': {
            'method': 'SessionSettled',
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
    },
    {
        '$sort': {
            'timestamp': -1  # Sort by timestamp in descending order
        }
    },
    {
        '$group': {
            '_id': "$workerId",  # Group by workerId to get unique workers
            'lastVscore': { '$last': "$vscore" }  # Get the last vscore for each worker
        }
    },
    {
        '$bucket': {
            'groupBy': "$lastVscore",  # Field to group by
            'boundaries': [0, 5000, 10000, 15000, 20000, float('inf')],  # Start of each range, including above 20000
            'default': "Other",  # This should not be needed since Infinity covers all higher values
            'output': {
                "count": { '$sum': 1 }
            }
        }
    }
]


    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    
    try:
        rw = list(eventsCol.aggregate(agg))

        
        hr["vscore24_5000"] = rw[0]["count"]
        hr["vscore24_10000"] = rw[1]["count"]
        hr["vscore24_15000"] = rw[2]["count"]
        hr["vscore24_20000"] = rw[3]["count"]
        hr["vscore24_20000gt"] = rw[4]["count"]


        
        
        
    except:
        
        hr["vscore24_5000"] = 0
        hr["vscore24_10000"] = 0
        hr["vscore24_15000"] = 0
        hr["vscore24_20000"] = 0
        hr["vscore24_20000gt"] = 0





#calc starting 1 hours back
    start = (hour) * 3600000
    end = (hour + 1) * 3600000
   

    ####### rewards
    agg = [
    {
        '$match': {
            'method': 'SessionSettled',
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
    },
    {
        '$sort': {
            'timestamp': -1  # Sort by timestamp in descending order
        }
    },
    {
        '$group': {
            '_id': "$workerId",  # Group by workerId to get unique workers
            'lastVscore': { '$last': "$vscore" }  # Get the last vscore for each worker
        }
    },
    {
        '$bucket': {
            'groupBy': "$lastVscore",  # Field to group by
            'boundaries': [0, 5000, 10000, 15000, 20000, float('inf')],  # Start of each range, including above 20000
            'default': "Other",  # This should not be needed since Infinity covers all higher values
            'output': {
                "count": { '$sum': 1 }
            }
        }
    }
]


    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    
    try:
        rw = list(eventsCol.aggregate(agg))

        
        hr["vscore_5000"] = rw[0]["count"]
        hr["vscore_10000"] = rw[1]["count"]
        hr["vscore_15000"] = rw[2]["count"]
        hr["vscore_20000"] = rw[3]["count"]
        hr["vscore_20000gt"] = rw[4]["count"]


        
        
        
    except:
        
        hr["vscore_5000"] = 0
        hr["vscore_10000"] = 0
        hr["vscore_15000"] = 0
        hr["vscore_20000"] = 0
        hr["vscore_20000gt"] = 0







    eventsHourlyCol.update_one({"_id":hour}, {"$set": hr}, upsert=True)


    




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

#### Production
eventsCol = phaladb['events']    
eventsBlockRawCol = phaladb['eventblockraw']
eventsHourlyCol = phaladb['eventshourlys']  
blockstatsCol = phaladb['blockstats'] 

#calcHourly(472216)



