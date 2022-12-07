from requests import head
from DataBroker.Sources.TDAmeritrade.Streams.database import databaseHandler
import datetime
from DataBroker.Sources.TDAmeritrade.Streams.streams import streams
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

'''def tdStreams(dbHost="10.6.47.45",dbPort="5432",dbName="postgres",dbUser="postgres",dbPass="Mollitiam-0828",headers=None,kafkaLocation='10.6.47.45:9092',debug=False):
    db = databaseHandler({
            "host": POSTGRES_LOCATION,
            "port": POSTGRES_PORT,
            "database": POSTGRES_DB,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD
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
        data = streams(header=headers,stocks=list(db.pennyOptionable.index),futures=list(db.futures.array),debug=debug,kafkaAddress=kafkaLocation)
    return'''

class tdStreams():
    def __init__(self):
        self.db = databaseHandler({
                "host": POSTGRES_LOCATION,
                "port": POSTGRES_PORT,
                "database": POSTGRES_DB,
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD
            })
        self.streamHandler = None


    def run(self,headers=None,kafkaLocation='10.6.47.45:9092',debug=False):
        tzInfo  = datetime.timezone(datetime.timedelta(hours=-4))
        yesterday = datetime.datetime.now(tz=tzInfo) - datetime.timedelta(1)
        today = datetime.datetime.now(tz=tzInfo).strftime('%Y-%m-%d')
        yesterday = yesterday.strftime('%Y-%m-%d')
        self.db.getPennyOptionable(date=yesterday)
        self.db.getFutures(date=yesterday)
        #db.getOptionsChains(date=yesterday)
        #topVolOptionsStr = db.getTradedLastWk(True)
        #print(len(topVolOptionsStr['result']))

        if headers != None:
            self.streamHandler = streams(header=headers,stocks=list(self.db.pennyOptionable.index),futures=list(self.db.futures.array),debug=debug,kafkaAddress=kafkaLocation)
        return