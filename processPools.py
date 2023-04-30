
from math import trunc
from pytz import UTC

from pymongo import MongoClient
from ast import literal_eval

import time
import stakePools
import phalaUtils




client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

poolQueuesCol = phaladb['poolqueues'] 
   
doProcess = True
while doProcess == True:
   q = poolQueuesCol.find_one()
   if q != None:
      print("Processing Pool: " + str(q["pid"]))
      poolQueuesCol.delete_one({"_id":q["_id"]})
      stakePools.UpdatePool(q["pid"])
      phalaUtils.updatePoolStakers(q["pid"])
      
   else:
      print("sleep")
      time.sleep(10)

        
