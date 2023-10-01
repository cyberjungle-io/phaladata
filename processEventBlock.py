import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval
import time
import stakePools
import derivedEvents






def processBlock(block):
    
    for eventNum in range(len(block["result"])):
        event = block["result"][eventNum]
        newEvent = {}
        newEvent["_id"] = str(block["blockNumber"]) + "-" + str(eventNum)
        newEvent["eventNumber"] = eventNum
        newEvent["blockNumber"] = block["blockNumber"]
        newEvent["timestamp"] = block["timestamp"]
        newEvent["method"] = event["method"]
        newEvent["section"] = event["section"]
        newEvent["data"] = event["data"]

        processed = False

        if event["method"] == "Wrapped" and event["section"] == "phalaWrappedBalances":
            processed = True
            try:
                newEvent["account_id"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                err["eventNumber"] = eventNum
                eventErrorsCol.insert_one(err)

        if event["method"] == "Unwrapped" and event["section"] == "phalaWrappedBalances":
            processed = True
            try:
                newEvent["account_id"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                err["eventNumber"] = eventNum
                eventErrorsCol.insert_one(err)


        if event["method"] == "Deposit" and event["section"] == "balances":
            processed = True
            try:
                newEvent["account_id"] = event["data"][0]
                try:
                    newEvent["pha"] = event["data"][1] / 1000000000000
                except:
                    newEvent["pha"] = literal_eval(event["data"][1]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                err["eventNumber"] = eventNum
                eventErrorsCol.insert_one(err)
        if event["method"] == "Withdraw" and event["section"] == "balances":
            processed = True
            try:
                newEvent["account_id"] = event["data"][0]
                try:
                    newEvent["pha"] = event["data"][1] / 1000000000000
                except:
                    newEvent["pha"] = literal_eval(event["data"][1]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                err["eventNumber"] = eventNum
                eventErrorsCol.insert_one(err)



    #########################################            
    ################## Mining & Computations
    ### Settled
        if event["method"] == "MinerSettled" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["pha"] = literal_eval(event["data"][2]) /(2**64)
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["pha"] = event["data"][2] /(2**64) 
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "SessionSettled" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["pha"] = literal_eval(event["data"][2]) /(2**64) 
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["pha"] = event["data"][2] / 1000000000000
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


### Bound
        if event["method"] == "MinerBound" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "SessionBound" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


### Unbound
        if event["method"] == "MinerUnbound" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "SessionUnbound" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


### Started
        if event["method"] == "MinerStarted" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                newEvent["pInit"] = event["data"][2]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                    newEvent["pInit"] = event["data"][2]
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "WorkerStarted" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                newEvent["pInit"] = event["data"][2]
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                    newEvent["pInit"] = event["data"][2]
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

###  Stopped
        if event["method"] == "MinerStopped" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "WorkerStopped" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

###  Unresponsive
        if event["method"] == "MinerEnterUnresponsive" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "WorkerEnterUnresponsive" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



###  Responsive
        if event["method"] == "MinerExitUnresponsive" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "WorkerExitUnresponsive" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


###  Benchmark
        if event["method"] == "BenchmarkUpdated":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pInstant"] = event["data"][1]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


###  SettlementDropped
        if event["method"] == "MinerSettlementDropped" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["pha"] = literal_eval(event["data"][2]) /(2**64) 
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["pha"] = literal_eval(event["data"][2]) /(2**64) 
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "SessionSettlementDropped" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["pha"] = literal_eval(event["data"][2]) /(2**64) 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["vscore"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["pha"] = literal_eval(event["data"][2]) /(2**64) 
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



###  Reclaimed
        if event["method"] == "MinerReclaimed" and event["section"] == "phalaMining":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["originalStake"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["slashed"] = literal_eval(event["data"][2]) /(2**64) 
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["originalStake"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["slashed"] = literal_eval(event["data"][2]) /(2**64) 
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


        if event["method"] == "WorkerReclaimed" and event["section"] == "phalaComputation":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["originalStake"] = literal_eval(event["data"][1]) /(2**64) 
                newEvent["slashed"] = literal_eval(event["data"][2]) /(2**64) 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    newEvent["originalStake"] = literal_eval(event["data"][1]) /(2**64) 
                    newEvent["slashed"] = literal_eval(event["data"][2]) /(2**64)  
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

###  InternalErrorWorkerSettleFailed
        if event["method"] == "InternalErrorWorkerSettleFailed":
            processed = True
            try:
                newEvent["workerId"] = event["data"][0]
                newEvent["pInstant"] = event["data"][1]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["workerId"] = event["data"][0]
                    
                   
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

        


###############################################
##### PhalaBasePool 
##### NftCreate
        if event["method"] == "NftCreated" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["cid"] = event["data"][1] 
                newEvent["nft_id"] = event["data"][2] 
                newEvent["account_id"] = event["data"][3] 
                
                try:
                    newEvent["shares"] = event["data"][4] / 1000000000000
                except:
                    newEvent["shares"] = literal_eval(event["data"][4]) / 1000000000000
                
                
                eventsCol.insert_one(newEvent)
                

            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["cid"] = event["data"][1] 
                    newEvent["nft_id"] = event["data"][2] 
                    newEvent["account_id"] = event["data"][3] 
                    
                    try:
                        newEvent["shares"] = event["data"][4] / 1000000000000
                    except:
                        newEvent["shares"] = literal_eval(event["data"][4]) / 1000000000000
                
                
                    eventsCol.insert_one(newEvent)
                    
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

##### WithdrawalQueued
        if event["method"] == "WithdrawalQueued" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["shares"] = event["data"][24] / 1000000000000
                except:
                    newEvent["shares"] = literal_eval(event["data"][2]) / 1000000000000
                newEvent["nft_id"] = event["data"][3] 
                newEvent["vault_id"] = event["data"][4] 

                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]
                    try:
                        newEvent["shares"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["shares"] = literal_eval(event["data"][2]) / 1000000000000
                    newEvent["nft_id"] = event["data"][3] 
                    newEvent["vault_id"] = event["data"][4] 
                    
                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

##### Withdrawal
        if event["method"] == "Withdrawal" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["amount"] = event["data"][2] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000

                try:
                    newEvent["shares"] = event["data"][3] / 1000000000000
                except:
                    newEvent["shares"] = literal_eval(event["data"][3]) / 1000000000000
                

                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]
                    try:
                        newEvent["amount"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000

                    try:
                        newEvent["shares"] = event["data"][3] / 1000000000000
                    except:
                        newEvent["shares"] = literal_eval(event["data"][3]) / 1000000000000
                    

                    
                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### PoolWhitelistCreated
        if event["method"] == "PoolWhitelistCreated" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    
                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### PoolWhitelistDeleted
        if event["method"] == "PoolWhitelistDeleted" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    
                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)




##### PoolWhitelistStakerAdded
        if event["method"] == "PoolWhitelistStakerAdded" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]

                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)




##### PoolWhitelistStakerRemoved
        if event["method"] == "PoolWhitelistStakerRemoved" and event["section"] == "phalaBasePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]

                    eventsCol.insert_one(newEvent)
                    
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)






###############################################
##### StakepoolV2 

##### WorkingStarted 
        if event["method"] == "WorkingStarted" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1] 
                try:
                    newEvent["stake"] = event["data"][2] / 1000000000000
                except:
                    newEvent["stake"] = literal_eval(event["data"][2]) / 1000000000000
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1] 
                    try:
                        newEvent["stake"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["stake"] = literal_eval(event["data"][2]) / 1000000000000 
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

##### MiningStarted 
        if event["method"] == "MiningStarted" and event["section"] == "phalaStakePool":
            processed = True
            try:

                newEvent["pid"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1] 
                try:
                    newEvent["stake"] = event["data"][2] / 1000000000000
                except:
                    newEvent["stake"] = literal_eval(event["data"][2]) / 1000000000000
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1] 
                    try:
                        newEvent["stake"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["stake"] = literal_eval(event["data"][2]) / 1000000000000 
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



##### PoolCreated 
        if event["method"] == "PoolCreated" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                newEvent["account_id"] = event["data"][0] 
                newEvent["pid"] = event["data"][1]
                newEvent["cid"] = event["data"][2] 
                newEvent["pool_account_id"] = event["data"][3] 
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["account_id"] = event["data"][0] 
                    newEvent["pid"] = event["data"][1]
                    newEvent["cid"] = event["data"][2] 
                    newEvent["pool_account_id"] = event["data"][3]
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



##### PoolCommissionSet 
        if event["method"] == "PoolCreated" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["commission"] = event["data"][1] /10000
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["commission"] = event["data"][1] /10000
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### PoolCapacitySet 
        if event["method"] == "PoolCapacitySet" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                try:
                    newEvent["cap"] = event["data"][1] / 1000000000000
                except:
                    newEvent["cap"] = literal_eval(event["data"][1]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    try:
                        newEvent["cap"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["cap"] = literal_eval(event["data"][1]) / 1000000000000 
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



##### PoolWorkerAdded 
        if event["method"] == "PoolCapacitySet" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                newEvent["workerId"] = event["data"][2]
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                    newEvent["workerId"] = event["data"][2] 
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### Contribution 
        if event["method"] == "Contribution" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["amount"] = event["data"][2] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                try:
                    newEvent["shares"] = event["data"][3] / 1000000000000
                except:
                    newEvent["shares"] = literal_eval(event["data"][3]) / 1000000000000 
                newEvent["vault_id"] = event["data"][4]
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]
                    try:
                        newEvent["amount"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                    try:
                        newEvent["shares"] = event["data"][3] / 1000000000000
                    except:
                        newEvent["shares"] = literal_eval(event["data"][3]) / 1000000000000 
                    newEvent["vault_id"] = event["data"][4] 
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### OwnerRewardsWithdrawn 
        if event["method"] == "OwnerRewardsWithdrawn" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["amount"] = event["data"][2] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]
                    try:
                        newEvent["amount"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### PoolSlashed 
        if event["method"] == "OwnerRewardsWithdrawn" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    try:
                        newEvent["amount"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### SlashSettled 
        if event["method"] == "SlashSettled" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["amount"] = event["data"][2] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["account_id"] = event["data"][1]
                    try:
                        newEvent["amount"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][2]) / 1000000000000 
                    
                        
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)

##### RewardDismissedNotInPool 
        if event["method"] == "RewardDismissedNotInPool" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pubkey"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pubkey"] = event["data"][0]
                    try:
                        newEvent["amount"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### RewardDismissedNoShare 
        if event["method"] == "RewardDismissedNoShare" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    try:
                        newEvent["amount"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### RewardDismissedDust 
        if event["method"] == "RewardDismissedDust" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                try:
                    newEvent["amount"] = event["data"][1] / 1000000000000
                except:
                    newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    try:
                        newEvent["amount"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["amount"] = literal_eval(event["data"][1]) / 1000000000000 
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### PoolWorkerRemoved 
        if event["method"] == "PoolWorkerRemoved" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)


##### WorkerReclaimed 
        if event["method"] == "WorkerReclaimed" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
                newEvent["pubkey"] = event["data"][1]
                
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                    newEvent["pubkey"] = event["data"][1]
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)



##### RewardReceived 
        if event["method"] == "RewardReceived" and event["section"] == "phalaStakePoolv2":
            processed = True
            try:
                
                newEvent["pid"] = event["data"][0]
               
                try:
                    newEvent["to_owner"] = event["data"][1] / 1000000000000
                except:
                    newEvent["to_owner"] = literal_eval(event["data"][1]) / 1000000000000 
                try:
                    newEvent["to_staker"] = event["data"][2] / 1000000000000
                except:
                    newEvent["to_staker"] = literal_eval(event["data"][2]) / 1000000000000 
              
                eventsCol.insert_one(newEvent)
                derivedEvents.processRewardReceivedAccounts(newEvent["_id"])
                #
                # stakePools.UpdatePool(newEvent["pid"])
            
            except:
                try:
                    newEvent["pid"] = event["data"][0]
                
                    try:
                        newEvent["to_owner"] = event["data"][1] / 1000000000000
                    except:
                        newEvent["to_owner"] = literal_eval(event["data"][1]) / 1000000000000 
                    try:
                        newEvent["to_staker"] = event["data"][2] / 1000000000000
                    except:
                        newEvent["to_staker"] = literal_eval(event["data"][2]) / 1000000000000 
              
                    
                    eventsCol.insert_one(newEvent)
                    derivedEvents.processRewardReceivedAccounts(newEvent["_id"])
                    #stakePools.UpdatePool(newEvent["pid"])
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)
            # Recalculate Pool



##############################################
##############################################
        if event["method"] == "RewardWithdrawn" and event["section"] == "phalaStakePool":
            processed = True
            try:
                newEvent["pool_id"] = event["data"][0]
                newEvent["account_id"] = event["data"][1]
                try:
                    newEvent["pha"] = event["data"][2] / 1000000000000
                except:
                    newEvent["pha"] = literal_eval(event["data"][2]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                eventErrorsCol.insert_one(err)
        if event["method"] == "Transfer" and event["section"] == "balances":
            processed = True
            try:
                newEvent["fromAccount"] = event["data"][0]
                newEvent["toAccount"] = event["data"][1]
                try:
                    newEvent["pha"] = event["data"][2] / 1000000000000
                except:
                    newEvent["pha"] = literal_eval(event["data"][2]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                eventErrorsCol.insert_one(err)
        if event["method"] == "Deposit" and event["section"] == "treasury":
            processed = True
            try:
                try:
                    newEvent["pha"] = event["data"][0] / 1000000000000
                except:
                    newEvent["pha"] = literal_eval(event["data"][0]) / 1000000000000
                eventsCol.insert_one(newEvent)
            except:
                err = event
                err["blockNumber"] = block["blockNumber"]
                err["timestamp"] = block["timestamp"]
                eventErrorsCol.insert_one(err)
    


    
################################################
#### Remark Core
################################################

##### NFTBurned 
        if event["method"] == "NFTBurned" and event["section"] == "rmrkCore":
            processed = True
            try:
                
                newEvent["account_id"] = event["data"][0]
                newEvent["cid"] = event["data"][1]
                newEvent["nft_id"] = event["data"][2]
                
                
                
                eventsCol.insert_one(newEvent)
            
            except:
                try:
                    newEvent["account_id"] = event["data"][0]
                    newEvent["cid"] = event["data"][1]
                    newEvent["nft_id"] = event["data"][2]
                    
                    
                    eventsCol.insert_one(newEvent)
                except:
                    err = event
                    err["blockNumber"] = block["blockNumber"]
                    err["timestamp"] = block["timestamp"]
                    err["eventNumber"] = eventNum
                    eventErrorsCol.insert_one(err)









        if processed == False:
            eventsCol.insert_one(newEvent) 








####################################################################
########  Main 

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
eventsBlockRawCol = phaladb['eventblockraw']
#### Production
eventsCol = phaladb['events']    
eventErrorsCol = phaladb['eventerrors']  


#### Test
# eventsCol = phaladb['eventtest']   
# eventErrorsCol = phaladb['eventtesterrors']  
