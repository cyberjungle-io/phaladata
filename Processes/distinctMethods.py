from pymongo import MongoClient
import pymongo
import requests
from ast import literal_eval

import json
import time
import math

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']

eventsCol = phaladb['events']


methods = eventsCol.distinct("method",{"$and":[{"blockNumber":{"$gt":1500000}},{"blockNumber":{"$lt":1509000}}]})

for m in methods:
    print(m)
    