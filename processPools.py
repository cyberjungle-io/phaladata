
from math import trunc
from pytz import UTC

from pymongo import MongoClient
from ast import literal_eval

import time
import stakePools




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

processControlCol = phaladb['processcontrol'] 
   
doProcess = True
while doProcess == True:
   
    pc = processControlCol.find_one({"_id":"processControl"})
    
    if pc["pools"] == True:
        stakePools.UpdateAllPools()
        pc["pools"] = False
        
        processControlCol.replace_one({"_id":"processControl"},pc)
       
        
    
    time.sleep(120)

        
