import requests

def getKucoinCurrentPrice():
    headers = {'Content-type': 'application/json'} 
    print("getheaders")
    #try:
    url = "https://api.kucoin.com/api/v1/prices?base=USD&currencies=PHA"
    r = requests.get(url, headers=headers)        
    tmpjson = r.json()
    #print(tmpjson)
    return float(tmpjson["data"]["PHA"])


##pha = getKucoinCurrentPrice()
#print(pha)