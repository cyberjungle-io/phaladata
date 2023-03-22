import phalaBlockchain
from pymongo import MongoClient
import pymongo
import requests
from miners import convertDate
import json
import time
import math


    
    





client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
minersCol= phaladb['miners']
eventsCol = phaladb['events']



