from requests import head
import logging
import asyncio
from DataBroker.Sources.TDAmeritrade.Streams.database import DatabaseHandler
import datetime
from DataBroker.Sources.TDAmeritrade.Streams.streams import Streams
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DEBUG, BOOTSTRAP_SERVER, SCHEMA_REG_ADDRESS, lastWeekday

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()
class TdStreams():
    def __init__(self):
        '''
        Class to handle the stream processing.
        '''
        self.db = DatabaseHandler({
                "host": POSTGRES_LOCATION,
                "port": POSTGRES_PORT,
                "database": POSTGRES_DB,
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD
            })
        self.streamHandler = None

    def run(self,headers=None,runFutures=True):
        '''
        Function to stream processing workflow.
        headers         -> (dict) Header for authentication requests
        kafkaLocation   -> (str) Bootstrap server of Kafka Cluster
        debug           -> (boolean) whether to log debug messages
        '''
        tzInfo  = datetime.timezone(datetime.timedelta(hours=-4))
        yesterday = lastWeekday()
        today = datetime.datetime.now(tz=tzInfo).strftime('%Y-%m-%d')
        yesterday = yesterday.strftime('%Y-%m-%d')
        #self.db.getPennyOptionable(date=yesterday)
        # Get Stocks that are in SP500, DJI; have Penny Increment Options or Weekly Options; Upcoming Earnings; ETFS at or over $500B Net Assets
        self.importantStocks = self.db.getSymbolsSql('''
            SELECT "Symbol"
            FROM PUBLIC."WATCHLIST"
        ''')
        self.db.getFutures(date=yesterday)
        logger.info('Headers: ')
        logger.info(headers)
        if headers != None:
            self.streamHandler = Streams(header=headers,stocks=list(self.importantStocks['Symbol']),futures=list(self.db.futures.array),kafkaAddress=BOOTSTRAP_SERVER,schemaRegAddress=SCHEMA_REG_ADDRESS,runFutures=runFutures,dbHandler=self.db)

            #self.tdStreamingData = self.streamHandler.runTdDataStream()
            #tdResults = loop.run_until_complete(self.tdStreamingData)
            asyncio.run(self.streamHandler.runTdDataStream(),debug=DEBUG)
        return