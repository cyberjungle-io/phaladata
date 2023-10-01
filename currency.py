
import pymongo
from pymongo import MongoClient
import phalaUtils
import time

client = MongoClient('10.2.2.11', 27017)
phaladb = client['phala']
tokenPriceCol = phaladb['tokenprice']

while True:
    #phala price
    price = phalaUtils.get_price_gecko("pha","usd")
    tokenprice = {
        "token": "pha",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)

#Polkadot price
    price = phalaUtils.get_price_gecko("polkadot","usd")
    tokenprice = {
        "token": "dot",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)

#astar price
    price = phalaUtils.get_price_gecko("astar","usd")
    tokenprice = {
        "token": "astr",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)

#moonbeam price
    price = phalaUtils.get_price_gecko("moonbeam","usd")
    tokenprice = {
        "token": "glmr",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)

#kilt price
    price = phalaUtils.get_price_gecko("kilt-protocol","usd")
    tokenprice = {
        "token": "kilt",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)


#mnta price
    price = phalaUtils.get_price_gecko("mantadao","usd")
    tokenprice = {
        "token": "mnta",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)


    #acala price
    price = phalaUtils.get_price_gecko("acala","usd")
    tokenprice = {
        "token": "aca",
        "price": price,
        "timestamp": int(time.time() * 1000),
        "source": "coingecko"
    }
    tokenPriceCol.insert_one(tokenprice)

    time.sleep(60)
    