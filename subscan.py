import json
import datetime
from math import trunc
from pytz import UTC
import requests
from pymongo import MongoClient
from ast import literal_eval

baseUrl = "https://khala.api.subscan.io/api/scan/"
headers = {'Content-type': 'application/json','X-API-Key':'7517674b948b43569005060aad3c6e03'} 
url = baseUrl + "blocks"

tdata = {'row':1,'page':0}
data_json = json.dumps(tdata)

r = requests.get(url,data=data_json, headers=headers)    

print(r)