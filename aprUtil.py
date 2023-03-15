from math import trunc
import datetime
from turtle import radians


def diffTimeInMinutes(start,end):
    tm = start - end
    min = (tm.days*(24*60)) + tm.seconds/60
    return min
def getRewardRecordAPR(deltaMinutes,startRec,recArray,stake):
    #print(startRec)
    stTime = datetime.datetime.fromisoformat(startRec["timestamp"])
    firstTime = datetime.datetime.fromisoformat(recArray[0]["timestamp"])
    if diffTimeInMinutes(stTime,firstTime) < deltaMinutes:
        return 0
    
    
    
    etime = 0
    minedValue = 0
    for r in recArray:
        rtime = datetime.datetime.fromisoformat(r["timestamp"])
        
        
        ttime = diffTimeInMinutes(stTime,rtime)
       # print("ttime: " + str(rtime) + "  Min:" + str(ttime))
       
        if ttime > etime and ttime <= deltaMinutes and ttime > 0:
            etime = ttime
            minedValue = startRec["totalReward"] - r["totalReward"]
           
            #print("blockNumber: " + str(r["blockNumber"]) + "  start: " + str(startRec["totalReward"]) + "   end: " + str(r["totalReward"]))
            #print(r["timestamp"] + "    value: " + str(minedValue) + "    ttime: " + str(ttime))
            #print("") 
    print("Mined Value: " + str(minedValue) + "  stake: " + str(stake) + "   etime: " + str(etime))
    try:
        apr = minedValue / stake / etime * 525960 * 100
    except:
        apr = 0
    #print("apr: " + str(apr))
    if apr < 0:
        apr = 0
    #print("================================")
    #print("")
    return apr
    
def getStakeByDeltaMinutes(deltaMinutes,stTime,recArray):
    stake = 0
    
    for r in recArray:
        endTime = datetime.datetime.fromisoformat(r["timestamp"])
        ttime = int(diffTimeInMinutes(stTime,endTime))
        if ttime >= deltaMinutes:
            #etime = ttime
            stake = r["stake"]
            #print(r["timestamp"])
    return stake

