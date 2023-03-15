from math import trunc
import time
import datetime
import json
from turtle import radians
from pymongo import MongoClient
import dateutil.parser
import aprUtil



client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol = phaladb['miners']
workersCol = phaladb['workers']
poolsCol = phaladb['pools']
poolStakersCol = phaladb['poolstakers']
accountsCol = phaladb['accounts']


mnr = minersCol.find({"operator":"42mKhRT6T3huTi52b7F18ZFnyQPFub5fRvPQ4U5BP2Cvu7LJ"})
for mn in mnr:
#rewArray = mn["totalRewardHistory"]
    rewArray = sorted(mn["totalRewardHistory"],key=lambda x: x["blockNumber"],reverse=False)
    last = 0
    dtlast = datetime.datetime(1971, 1, 1, 0, 0, 0)
    for m in rewArray:
        tmin = trunc((datetime.datetime.fromisoformat(m["timestamp"]) - dtlast).seconds/60)
        m["deltaTime"] = tmin
        
        #print(m["deltaTime"])
        #print(ttime.seconds)
        
        dtlast = datetime.datetime.fromisoformat(m["timestamp"])
        #print(dtlast)
        last = m["totalReward"]
        stake24 = aprUtil.getStakeByDeltaMinutes(1440,dtlast,mn["stakeHistory"])
        m["stake24"] = stake24
        if stake24 == 0:
            stake24 = mn["stake"]
        #print("Stake24: " + str(stake24))
        if stake24 > 0:
            apr24 = aprUtil.getRewardRecordAPR(1440,m,rewArray,stake24)
            print("Apr24: " + str(apr24))
            m["apr24"] = trunc(apr24)
        else:
            print("stake is 0")
            m["apr24"] = 0

        currStake = aprUtil.getStakeByDeltaMinutes(0,dtlast,mn["stakeHistory"])
        m["stake"] = currStake
        apr = m["pha"] / currStake / tmin * 525960
        m["apr"] = trunc(apr*100)
        mn["apr"] = trunc(apr*100)
        mn["apr24"] = m["apr24"]

    mn["totalRewardHistory"] = rewArray
    minersCol.find_one_and_replace({"_id":mn["_id"]},mn)


    #print(json.dumps(mn,indent=4))
    #print(json.dumps(rewArray,indent=4))
