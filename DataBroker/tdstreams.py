from requests import head
from DataBroker.Sources.TDAmeritrade.Streams.database import databaseHandler
import datetime
from DataBroker.Sources.TDAmeritrade.Streams.streams import streams

def tdStreams(dbHost="10.6.47.45",dbPort="5432",dbName="postgres",dbUser="postgres",dbPass="Mollitiam-0828",headers=None,kafkaLocation='10.6.47.45'):
    db = databaseHandler({
            "host": dbHost,
            "port": dbPort,
            "database": dbName,
            "user": dbUser,
            "password": dbPass
        })

    tzInfo  = datetime.timezone(datetime.timedelta(hours=-4))
    yesterday = datetime.datetime.now(tz=tzInfo) - datetime.timedelta(1)
    today = datetime.datetime.now(tz=tzInfo).strftime('%Y-%m-%d')
    yesterday = yesterday.strftime('%Y-%m-%d')
    db.getPennyOptionable(date=yesterday)
    db.getFutures(date=yesterday)
    #db.getOptionsChains(date=yesterday)
    #topVolOptionsStr = db.getTradedLastWk(True)
    #print(len(topVolOptionsStr['result']))

    if headers != None:
        data = streams(header=headers,stocks=list(db.pennyOptionable.index),futures=list(db.futures.array),kafkaAddress=kafkaLocation)
    return