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
                    'method': 'Transfer'
                }, {
                    'method': 'Withdrawal'
                }, {
                    'method': 'OwnerRewardsWithdrawn'
                }, {
                    'method': 'RewardReceived'
                }
            ], 
            '$and': [
                {
                    'timestamp': {
                        '$gte': 1679238000000
                    }
                }, {
                    'timestamp': {
                        '$lt': 1679313600000
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
            'pha': 1, 
            'amount': 1, 
            'to_owner': 1, 
            'to_staker': 1
        }
    }, {
        '$sort': {
            'timestamp': -1
        }
    }, {
        '$group': {
            '_id': '$hour', 
            'transfers': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'Transfer'
                            ]
                        }, '$pha', 0
                    ]
                }
            }, 
            'withdrawal': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'Withdrawal'
                            ]
                        }, '$amount', 0
                    ]
                }
            }, 
            'rewardsToStaker': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'RewardReceived'
                            ]
                        }, '$to_staker', 0
                    ]
                }
            }, 
            'rewardsToOwner': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'RewardReceived'
                            ]
                        }, '$to_owner', 0
                    ]
                }
            }, 
            'ownerRewardsWithdrawn': {
                '$sum': {
                    '$cond': [
                        {
                            '$eq': [
                                '$method', 'OwnerRewardsWithdrawn'
                            ]
                        }, '$amount', 0
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

        
        hr["transfers"] = rw["transfers"]
        hr["poolWithdrawal"] = rw["withdrawal"]
        hr["rewardsToStaker"] = rw["rewardsToStaker"]
        hr["rewardsToOwner"] = rw["rewardsToOwner"]
        hr["ownerRewardsWithdrawn"] = rw["ownerRewardsWithdrawn"]
        
    except:
        
        hr["transfers"] = 0
        hr["poolWithdrawal"] = 0
        hr["rewardsToStaker"] = 0
        hr["rewardsToOwner"] = 0
        hr["ownerRewardsWithdrawn"] = 0
        







    eventsHourlyCol.update_one({"_id":hour}, {"$set": hr}, upsert=True)


    




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

#### Production
eventsCol = phaladb['events']    
eventsBlockRawCol = phaladb['eventblockraw']
eventsHourlyCol = phaladb['eventshourlys']  
blockstatsCol = phaladb['blockstats'] 

#calcHourly(470713)



