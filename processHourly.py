import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time
import processHourlyWhitelist
import processHourlyPools
import processHourlyVscore

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
                            'method': 'SessionSettled'
                        }, {
                            'method': 'MinerSettled'
                        }
                    ], 
                    '$and': [
                        {
                            'timestamp': {
                                '$gte': 1672246416189
                            }
                        }, {
                            'timestamp': {
                                '$lt': 1673654028469
                            }
                        }
                    ]
                }
            }, {
                '$sort': {
                    'timestamp': 1
                }
            }, {
                '$lookup': {
                    'from': 'miners', 
                    'localField': 'workerId', 
                    'foreignField': '_id', 
                    'as': 'miner'
                }
            }, {
                '$unwind': {
                    'path': '$miner'
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
                    'apr': 1, 
                    'miner': 1
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
                    'count': {
                        '$sum': 1
                    }, 
                    'totalpha': {
                        '$sum': '$pha'
                    }, 
                    'averageapr': {
                        '$avg': '$apr'
                    }, 
                    'level1': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 1
                                    ]
                                }, '$pha', 0
                            ]
                        }
                    }, 
                    'level2': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 2
                                    ]
                                }, '$pha', 0
                            ]
                        }
                    }, 
                    'level3': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 3
                                    ]
                                }, '$pha', 0
                            ]
                        }
                    }, 
                    'level4': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 4
                                    ]
                                }, '$pha', 0
                            ]
                        }
                    }, 
                    'level5': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 5
                                    ]
                                }, '$pha', 0
                            ]
                        }
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]

    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    
    try:
        rw = list(eventsCol.aggregate(agg))[0]

        
        hr["rewardCount"] = rw["count"]
        hr["totalRewards"] = rw["totalpha"]
        hr["rewardsLevel1"] = rw["level1"]
        hr["rewardsLevel2"] = rw["level2"]
        hr["rewardsLevel3"] = rw["level3"]
        hr["rewardsLevel4"] = rw["level4"]
        hr["rewardsLevel5"] = rw["level5"]
        
    except:
        
        hr["rewardCount"] = 0
        hr["totalRewards"] = 0
        hr["rewardsLevel1"] = 0
        hr["rewardsLevel2"] = 0
        hr["rewardsLevel3"] = 0
        hr["rewardsLevel4"] = 0
        hr["rewardsLevel5"] = 0
        
    print("after rewards")
####### responsive

    agg = [
            {
                '$match': {
                    '$or': [
                        {
                            'method': 'WorkerEnterUnresponsive'
                        }, {
                            'method': 'WorkerExitUnresponsive'
                        }, {
                            'method': 'MinerEnterUnresponsive'
                        }, {
                            'method': 'MinerExitUnresponsive'
                        }, {
                            'method': 'MinerExitUnresponive'
                        },
                         {
                            'method': 'PoolCreated'
                        },
                         {
                            'method': 'PoolWorkerAdded'
                        },
                         {
                            'method': 'PoolWorkerRemoved'
                        },
                         {
                            'method': 'MinerStarted'
                        },
                         {
                            'method': 'WorkerStarted'
                        },
                         {
                            'method': 'MinerStopped'
                        },
                         {
                            'method': 'WorkerStopped'
                        },
                         {
                            'method': 'NewAccount'
                        },
                         {
                            'method': 'KilledAccount'
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
                    'count': {
                        '$sum': 1
                    }, 
                    'poolCreated': {
                        '$sum': {
                            '$cond': [
                               {
                                            '$eq': [
                                                '$method', 'PoolCreated'
                                            ]
                                        }, 1, 0
                            ]
                        }
                    }, 
                    'poolWorkerAdded': {
                        '$sum': {
                            '$cond': [
                               {
                                            '$eq': [
                                                '$method', 'PoolWorkerAdded'
                                            ]
                                        }, 1, 0
                            ]
                        }
                    }, 
                    'poolWorkerRemoved': {
                        '$sum': {
                            '$cond': [
                               {
                                            '$eq': [
                                                '$method', 'PoolWorkerRemoved'
                                            ]
                                        }, 1, 0
                            ]
                        }
                    }, 
                    'newAccount': {
                        '$sum': {
                            '$cond': [
                               {
                                            '$eq': [
                                                '$method', 'NewAccount'
                                            ]
                                        }, 1, 0
                            ]
                        }
                    }, 
                    'killedAccount': {
                        '$sum': {
                            '$cond': [
                               {
                                            '$eq': [
                                                '$method', 'KilledAccount'
                                            ]
                                        }, 1, 0
                            ]
                        }
                    }, 
                    'workerStarted': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$method', 'MinerStarted'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$method', 'WorkerStarted'
                                            ]
                                        }
                                    ]
                                }, 1, 0
                            ]
                        }
                    }, 
                    'workerStopped': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$method', 'MinerStopped'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$method', 'WorkerStopped'
                                            ]
                                        }
                                    ]
                                }, 1, 0
                            ]
                        }
                    }, 
                    'unresponsive': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$method', 'WorkerEnterUnresponsive'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$method', 'MinerEnterUnresponsive'
                                            ]
                                        }
                                    ]
                                }, 1, 0
                            ]
                        }
                    }, 
                    'responsive': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$or': [
                                        {
                                            '$eq': [
                                                '$method', 'WorkerExitUnresponsive'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$method', 'MinerExitUnresponsive'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$method', 'MinerExitUnresponive'
                                            ]
                                        }
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
    print(agg)
    # print("")
      
    try:
        rw = list(eventsCol.aggregate(agg))[0]

        hr["responsive"] = rw["responsive"]
        hr["unresponsive"] = rw["unresponsive"]
        hr["poolCreated"] = rw["poolCreated"]
        hr["poolWorkerAdded"] = rw["poolWorkerAdded"]
        hr["poolWorkerRemoved"] = rw["poolWorkerRemoved"]
        hr["newAccount"] = rw["newAccount"]
        hr["killedAccount"] = rw["killedAccount"]
        hr["workerStarted"] = rw["workerStarted"]
        hr["workerStopped"] = rw["workerStopped"]

    except:
        hr["responsive"] = 0
        hr["unresponsive"] = 0
        hr["eventCount"] = 0
        hr["poolWorkerAdded"] = 0
        hr["poolWorkerRemoved"] = 0
        hr["newAccount"] = 0
        hr["killedAccount"] = 0
        hr["workerStarted"] = 0
        hr["workerStopped"] = 0
    #print(hr)
    print("after responsive")


####### block time

    agg = [
            {
                '$match': {
                    '$and': [
                        {
                            'timestamp': {
                                '$gte': 1626175686365
                            }
                        }, {
                            'timestamp': {
                                '$lt': 1628175686365
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'hour': {
                        '$trunc': {
                            '$divide': [
                                '$timestamp', 3600000
                            ]
                        }
                    }, 
                    'blockElapseTime': 1, 
                    'timestamp': 1
                }
            }, {
                '$group': {
                    '_id': '$hour', 
                    'mintime': {
                        '$min': '$blockElapseTime'
                    }, 
                    'maxtime': {
                        '$max': '$blockElapseTime'
                    }, 
                    'count': {
                        '$sum': 1
                    }, 
                    'avgtime': {
                        '$avg': '$blockElapseTime'
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]
    


    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    # print(agg)
    # print("")
      
    try:
        rw = list(blockstatsCol.aggregate(agg))[0]

        hr["minBlockTime"] = rw["mintime"]
        hr["maxBlockTime"] = rw["maxtime"]
        hr["blockCount"] = rw["count"]
        hr["avgBlockTime"] = rw["avgtime"]
    except:
        hr["minBlockTime"] = 0
        hr["maxBlockTime"] = 0
        hr["blockCount"] = 0
        hr["avgBlockTime"] = 0
    #print(hr)
    print("after block time")

####### apr
    agg = [
            {
                '$match': {
                    '$or': [
                        {
                            'method': 'SessionSettled'
                        }, {
                            'method': 'MinerSettled'
                        }
                    ], 
                    '$and': [
                        {
                            'timestamp': {
                                '$gt': 1672246416189
                            }
                        }, {
                            'timestamp': {
                                '$lt': 1673654028469
                            }
                        }
                    ]
                }
            }, {
                '$sort': {
                    'timestamp': 1
                }
            }, {
                '$lookup': {
                    'from': 'miners', 
                    'localField': 'workerId', 
                    'foreignField': '_id', 
                    'as': 'miner'
                }
            }, {
                '$unwind': {
                    'path': '$miner'
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
                    'apr': 1, 
                    'miner': 1, 
                    'stake': 1
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
                    'count': {
                        '$sum': 1
                    }, 
                    'totalpha': {
                        '$sum': '$pha'
                    }, 
                    'averageapr': {
                        '$avg': '$apr'
                    }, 
                    'level1': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 1
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level2': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 2
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level3': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 3
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level4': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 4
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$miner.confidenceLevel', 5
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$lte': [
                                        '$stake', 5000
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$gt': [
                                        '$stake', 15000
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level1_stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 1
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level1_stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 1
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level1_stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 1
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level1_stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 1
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 15000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level2_stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 2
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level2_stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 2
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level2_stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 2
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level2_stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 2
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 15000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level3_stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 3
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level3_stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 3
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level3_stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 3
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level3_stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 3
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 15000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level4_stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 4
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level4_stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 4
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level4_stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 4
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level4_stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 4
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 15000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level5_stakelt5': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 5
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level5_stake5_10': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 5
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 10000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 5000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level5_stake10_15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 5
                                            ]
                                        }, {
                                            '$lte': [
                                                '$stake', 15000
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 10000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }, 
                    'level5_stakegt15': {
                        '$avg': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$miner.confidenceLevel', 5
                                            ]
                                        }, {
                                            '$gt': [
                                                '$stake', 15000
                                            ]
                                        }
                                    ]
                                }, '$apr', None
                            ]
                        }
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]

    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    
    try:
        rw = list(eventsCol.aggregate(agg))[0]

        hr["averageapr"] = rw["averageapr"]
        hr["apr_level1"] = rw["level1"]
        hr["apr_level2"] = rw["level2"]
        hr["apr_level3"] = rw["level3"]
        hr["apr_level4"] = rw["level4"]
        hr["apr_level5"] = rw["level5"]

        hr["apr_stake_lte5"] = rw["stakelt5"]
        hr["apr_stake_5_10"] = rw["stake5_10"]
        hr["apr_stake_10_15"] = rw["stake10_15"]
        hr["apr_stake_gt15"] = rw["stakegt15"]

        hr["apr_l1_stake_lte5"] = rw["level1_stakelt5"]
        hr["apr_l1_stake_5_10"] = rw["level1_stake5_10"]
        hr["apr_l1_stake_10_15"] = rw["level1_stake10_15"]
        hr["apr_l1_stake_gt15"] = rw["level1_stakegt15"]

        hr["apr_l2_stake_lte5"] = rw["level2_stakelt5"]
        hr["apr_l2_stake_5_10"] = rw["level2_stake5_10"]
        hr["apr_l2_stake_10_15"] = rw["level2_stake10_15"]
        hr["apr_l2_stake_gt15"] = rw["level2_stakegt15"]

        hr["apr_l3_stake_lte5"] = rw["level3_stakelt5"]
        hr["apr_l3_stake_5_10"] = rw["level3_stake5_10"]
        hr["apr_l3_stake_10_15"] = rw["level3_stake10_15"]
        hr["apr_l3_stake_gt15"] = rw["level3_stakegt15"]

        hr["apr_l4_stake_lte5"] = rw["level4_stakelt5"]
        hr["apr_l4_stake_5_10"] = rw["level4_stake5_10"]
        hr["apr_l4_stake_10_15"] = rw["level4_stake10_15"]
        hr["apr_l4_stake_gt15"] = rw["level4_stakegt15"]

        hr["apr_l5_stake_lte5"] = rw["level5_stakelt5"]
        hr["apr_l5_stake_5_10"] = rw["level5_stake5_10"]
        hr["apr_l5_stake_10_15"] = rw["level5_stake10_15"]
        hr["apr_l5_stake_gt15"] = rw["level5_stakegt15"]

        

    except:
        
        hr["averageapr"] = 0
        hr["apr_level1"] = 0
        hr["apr_level2"] = 0
        hr["apr_level3"] = 0
        hr["apr_level4"] = 0
        hr["apr_level5"] = 0

        hr["apr_stake_lte5"] = 0
        hr["apr_stake_5_10"] = 0
        hr["apr_stake_10_15"] = 0
        hr["apr_stake_gt15"] = 0

        hr["apr_l1_stake_lte5"] = 0
        hr["apr_l1_stake_5_10"] = 0
        hr["apr_l1_stake_10_15"] = 0
        hr["apr_l1_stake_gt15"] = 0

        hr["apr_l2_stake_lte5"] = 0
        hr["apr_l2_stake_5_10"] = 0
        hr["apr_l2_stake_10_15"] = 0
        hr["apr_l2_stake_gt15"] = 0

        hr["apr_l3_stake_lte5"] = 0
        hr["apr_l3_stake_5_10"] = 0
        hr["apr_l3_stake_10_15"] = 0
        hr["apr_l3_stake_gt15"] = 0

        hr["apr_l4_stake_lte5"] = 0
        hr["apr_l4_stake_5_10"] = 0
        hr["apr_l4_stake_10_15"] = 0
        hr["apr_l4_stake_gt15"] = 0

        hr["apr_l5_stake_lte5"] = 0
        hr["apr_l5_stake_5_10"] = 0
        hr["apr_l5_stake_10_15"] = 0
        hr["apr_l5_stake_gt15"] = 0

    
    print("after apr")  

####### events

    agg = [
            {
                '$match': {
                    '$and': [
                        {
                            'timestamp': {
                                '$gte': 1656175686365
                            }
                        }, {
                            'timestamp': {
                                '$lt': 1658175686365
                            }
                        }
                    ]
                }
            }, {
                '$sort': {
                    'timestamp': 1
                }
            }, {
                '$project': {
                    'timeIncrement': {
                        '$trunc': {
                            '$divide': [
                                '$timestamp', 3600000
                            ]
                        }
                    }, 
                    'eventCount': 1
                }
            }, {
                '$group': {
                    '_id': '$timeIncrement', 
                    'eventCount': {
                        '$sum': '$eventCount'
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]
    


    agg[0]["$match"]["$and"][0]["timestamp"]["$gte"] = start
    agg[0]["$match"]["$and"][1]["timestamp"]["$lt"] = end
    # print(agg)
    # print("")
      
    try:
        rw = list(eventsBlockRawCol.aggregate(agg))[0]

        hr["sumEventCount"] = rw["eventCount"]
        
    except:
        hr["sumEventCount"] = 0
        
    #print(hr)







    eventsHourlyCol.replace_one({"_id":hour},hr,upsert=True)

    processHourlyWhitelist.calcHourly(hour)
    processHourlyPools.calcHourly(hour)
    processHourlyVscore.calcHourly(hour)

    




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

#### Production
eventsCol = phaladb['events']    
eventsBlockRawCol = phaladb['eventblockraw']
eventsHourlyCol = phaladb['eventshourlys']  
blockstatsCol = phaladb['blockstats'] 

#calcHourly(470951)



