from pprint import pprint
import urllib
import requests
import dateutil.parser
import datetime
from aioscheduler import TimedScheduler
import logging
import inspect
import configparser
import json
import time
import pytz
import threading
from splinter import Browser
from selenium.webdriver.chrome.options import Options
import asyncio
import nest_asyncio
from DataBroker.Sources.TDAmeritrade.Streams.socketclient import WebSocketClient
import argparse
from DataBroker.Sources.TDAmeritrade.Streams.kafkaClient import kafkaClient
from constants import DEBUG, POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, lastWeekday

from confluent_kafka.serialization import SerializationContext, MessageField

nest_asyncio.apply()
logger = logging.getLogger(__name__)

class Streams:
    def __init__(self,header=None,stocks=None,options=None,futures=None,kafkaAddress='10.6.47.45:9092',schemaRegAddress='10.6.47.45:8081',runFutures=True,dbHandler=None,runBooks=False):
        '''
        Class to listen to TD Ameritrade Streaming API.
        header          -> (dict) Headers for request to TD Ameritrade \
                        Authentication API
        stocks          -> (list) List of stocks symbols to follow
        options         -> (list) List of options symbols to follow
        future          -> (list) List of futures symbols to follow
        kafkaAddress    -> (str) Kafka cluster bootstrap server
        '''
        if dbHandler is not None:
            self.startTime = time.time()
            caller = inspect.stack()[1][3].upper()
            
            # Create New Run in RunHistory
            toFetch = int(len(stocks) + len(options) + len(futures))
            self.db.cur.execute('''
                INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime","SymbolsToFetch") VALUES ('%s','%s',%s) RETURNING "Id";
            ''' % (caller,self.startTime,toFetch))
            self.runId = self.db.cur.fetchone()[0]
            self.db = dbHandler
            '''self.db = DatabaseHandler({
                    "host": POSTGRES_LOCATION,
                    "port": POSTGRES_PORT,
                    "database": POSTGRES_DB,
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD
                })'''
            yesterday = lastWeekday()
            yesterday = yesterday.strftime('%Y-%m-%d')
            self.db.getListed(date=yesterday)
            self.db.getNasdaq(date=yesterday)
            self.runFutures = runFutures
            self.runBook = runBooks
            # we need to go to the User Principals endpoint to get the info we need to make a streaming request
            endpoint = "https://api.tdameritrade.com/v1/userprincipals"
            self.subscriptions = {'stocks': None,'options': None,'futures': None}
            self.kafkaLoc = kafkaAddress
            self.schemaLoc = schemaRegAddress

            if logger != None:
                self.log = logger
            else:
                self.log = logging.getLogger(__name__)
            if DEBUG:
                logging.basicConfig(
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M:%S",
                    handlers=[logging.FileHandler(f'./logs/output_{datetime.date.today()}.txt'), logging.StreamHandler()],
                )
            else:
                logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M:%S",
                    handlers=[logging.FileHandler(f'./logs/output_{datetime.date.today()}.txt')],
                )

            if stocks != None:
                self.subscriptions['stocks'] = stocks
            if options != None:
                self.subscriptions['options'] = options
            if futures != None:
                self.subscriptions['futures'] = futures
                
            # get our access token
            if header==None:
                return
            else:
                headers = header

            # this endpoint, requires fields which are separated by ','
            params = {'fields':'streamerSubscriptionKeys,streamerConnectionInfo'}

            # make a request
            self.log.info('Starting Stream Processor TD Ameritrade\n')
            content = requests.get(url = endpoint, params = params, headers = headers)
            self.userPrincipalsResponse = content.json()

            # we need to get the timestamp in order to make our next request, but it needs to be parsed
            tokenTimeStamp = self.userPrincipalsResponse['streamerInfo']['tokenTimestamp']
            tokenTimeStampAsMs = self.unix_time_millis(tokenTimeStamp)

            # we need to define our credentials that we will need to make our stream
            self.credentials = {"userid": self.userPrincipalsResponse['accounts'][0]['accountId'],
                        "token": self.userPrincipalsResponse['streamerInfo']['token'],
                        "company": self.userPrincipalsResponse['accounts'][0]['company'],
                        "segment": self.userPrincipalsResponse['accounts'][0]['segment'],
                        "cddomain": self.userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
                        "usergroup": self.userPrincipalsResponse['streamerInfo']['userGroup'],
                        "accesslevel":self.userPrincipalsResponse['streamerInfo']['accessLevel'],
                        "authorized": "Y",
                        "timestamp": int(tokenTimeStampAsMs),
                        "appid": self.userPrincipalsResponse['streamerInfo']['appId'],
                        "acl": self.userPrincipalsResponse['streamerInfo']['acl'] }
            
            self.loop = asyncio.set_event_loop(asyncio.new_event_loop())
            return
        else:
            logger.error('No database handler passed to Streams')
    
    async def runTdDataStream(self):
        '''
        Main function to handle streaming data from TD Ameritrade.
        '''
        # Creating client object
        self.client = WebSocketClient(self.userPrincipalsResponse)

        self.messages_counter = 0
        self.messages_counter_thread_lock = threading.Lock()
        
        loop = asyncio.get_event_loop()
        login_encoded, self.messages_counter, self.messages_counter_thread_lock = self.loginMsg(self.userPrincipalsResponse,self.credentials,self.messages_counter,self.messages_counter_thread_lock)
        l1futures_encoded, self.messages_counter, self.messages_counter_thread_lock  = self.initMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'LEVELONE_FUTURES')

        # Start connection and get client connection protocol
        self.connection = await self.client.connect()
        self.log.info("Connected")
        scheduler = TimedScheduler()
        scheduler.start()

        self.messages_counter,self.messages_counter_thread_lock = self.scheduleHours(self.client.hours,scheduler,self.client,self.messages_counter,self.messages_counter_thread_lock)

        # Send login message and receive response
        await asyncio.wait_for(self.client.sendMessage(login_encoded),timeout=10)

        # Process login response
        login_response = await asyncio.wait_for(self.connection.recv(), timeout=10)
        login_response = str(login_response).replace(' \"C\" ','C')
        login_response_decoded = json.loads(login_response)
        self.log.debug(login_response_decoded['response'][0].get('command'))
        self.log.debug(login_response_decoded['response'][0].get('content').get('code'))

        # Check if login successful and follow up with subscription message
        if login_response_decoded['response'][0].get('command') == 'LOGIN' and login_response_decoded['response'][0].get('content').get('code') == 3:
            self.log.error("Login Failed")
            raise Exception("Login Failed")
        elif login_response_decoded['response'][0].get('command') == 'LOGIN' and login_response_decoded['response'][0].get('content').get('code') == 0:
            await asyncio.gather(
                self.client.sendMessage(l1futures_encoded),
                self.client.receiveMessage(self.connection,self.runFutures)
            )
        
    def initMsg(self,userPrincipalsResponse,counter,lock,asset_type='LEVELONE_FUTURES',assets={'stocks': None,'options': None,'futures': None}):
        '''
        Function to create first subscription message for individual data types.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        asset_type                      -> (str) asset to initialize message for
        assets                          -> (dict) Dict of lists with symbols to\
                                            subscribe to
        '''
        # define a request for different data sources
        message_id = self.generate_message_id(counter,lock)
        if asset_type == 'QUOTE':
            if self.subscriptions['stocks'] != None:
                asset = ",".join(self.subscriptions['stocks'])
                self.log.info('Quote Stocks: ')
                self.log.info(asset)
                data_request = {"requests": [{
                    "service": "QUOTE",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": asset,
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,23,24,25,26,28,29,30,31,32,33,34,37,40,41,42,43,44,45,46,47,48,49,50,51,52"
                    }
                }]}
            else:
                data_request = {"requests": [{
                    "service": "QUOTE",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": "AAPL,MSFT,TSLA,GME,LMT",
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,23,24,25,26,28,29,30,31,32,33,34,37,40,41,42,43,44,45,46,47,48,49"
                    }
                }]}
                self.subscriptions['stocks'] = data_request["requests"][0]["parameters"]["keys"].split(",")
            self.log.info("Level One Stocks:\n")
            self.log.info(self.subscriptions['stocks'])
        elif asset_type == 'OPTION':
            if self.subscriptions['options'] != None:
                asset = ",".join(self.subscriptions['options'])
                self.log.info('Options:')
                self.log.info(asset)
                data_request = {"requests": [{
                    "service": "OPTION",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": asset,
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,19,20,21,22,23,24,25,26,27,30,31,32,33,34,35,36,41"
                    }
                }]}
            else:
                data_request = {"requests": [{
                    "service": "OPTION",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": "",
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,19,20,21,22,23,24,25,26,27,30,31,32,33,34,35,36,41"
                    }
                }]}
                self.subscriptions['options'] = data_request["requests"][0]["parameters"]["keys"].split(",")
            self.log.info("Level One Options:\n")
            self.log.info(self.subscriptions['options'])
        elif asset_type == 'ACTIVES_NYSE':
            data_request = {"requests": [{
                "service": "ACTIVES_NYSE",
                "requestid": message_id,
                "command": "SUBS",
                "account": userPrincipalsResponse['accounts'][0]['accountId'],
                "source": userPrincipalsResponse['streamerInfo']['appId'],
                "parameters": {
                    "keys": "NYSE-ALL,NYSE-60",
                    "fields": "0,1"
                }
            }]}
        elif asset_type == 'ACTIVES_NASDAQ':
            data_request = {"requests": [{
                "service": "ACTIVES_NASDAQ", 
                "requestid": message_id, 
                "command": "SUBS", 
                "account": userPrincipalsResponse['accounts'][0]['accountId'], 
                "source": userPrincipalsResponse['streamerInfo']['appId'], 
                "parameters": {
                    "keys": "NASDAQ-ALL,NASDAQ-60", 
                    "fields": "0,1"
                }
            }]}
        elif asset_type == 'ACTIVES_OPTIONS':
            data_request = {"requests": [{
                "service": "ACTIVES_OPTIONS",
                "requestid": message_id, 
                "command": "SUBS",
                "account": userPrincipalsResponse['accounts'][0]['accountId'],
                "source": userPrincipalsResponse['streamerInfo']['appId'],
                "parameters": {
                    "keys": "OPTS-ALL,OPTS-60",
                    "fields": "0,1"
                }
            }]}
        elif asset_type == 'LEVELONE_FUTURES':
            if self.subscriptions['futures'] != None:
                if self.runFutures:
                    asset = ",".join(self.subscriptions['futures'])
                else:
                    asset = self.subscriptions['futures'][0]
                self.log.info('Level One Futures:')
                self.log.info(asset)
                data_request = {"requests": [{
                    "service": "LEVELONE_FUTURES",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": asset,
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35"
                    }
                }]}
            else:
                data_request = {"requests": [{
                    "service": "LEVELONE_FUTURES",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": "/ES,/ZN,/CL",
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35"
                    }
                }]}
                self.subscriptions['futures'] = data_request["requests"][0]["parameters"]["keys"].split(",")
            self.log.info("Level One Futures:\n")
            self.log.info(self.subscriptions['futures'])
        elif asset_type == 'CHART_EQUITY':
            if self.subscriptions['stocks'] != None:
                self.db

        # create it into a JSON string, as the API expects a JSON string.
        data_encoded = json.dumps(data_request)
        return data_encoded, message_id, lock

    def bookMsg(self,userPrincipalsResponse,counter,lock,exchange='LISTED_BOOK'):
        '''
        Function to create subscription message to order book.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        exchange                        -> (str) asset exchange to subscribe to
        '''
        # define a request for different data sources
        message_id = self.generate_message_id(counter,lock)
        symbols = []
        if exchange == 'LISTED_BOOK':
            for stock in self.subscriptions['stocks']:
                if stock in self.db.listed:
                    symbols.append(stock)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "LISTED_BOOK",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3"
                    }
                }]}
        elif exchange == 'NASDAQ_BOOK':
            for stock in self.subscriptions['stocks']:
                if stock in self.db.nasdaq:
                    symbols.append(stock)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "NASDAQ_BOOK",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3"
                    }
                }]}
        elif exchange == 'OPTIONS_BOOK':
            for option in self.subscriptions['options']:
                symbols.append(option)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "OPTIONS_BOOK",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3"
                    }
                }]}
        # create it into a JSON string, as the API expects a JSON string.
        data_encoded = json.dumps(data_request)
        return data_encoded, message_id, lock
    
    def chartMsg(self,userPrincipalsResponse,counter,lock,chartType='LISTED'):
        '''
        Function to create subscription message to chart history.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        chartType                        -> (str) chart type to subscribe to
        '''
        # define a request for different data sources
        self.log.info('ChartType: ' + str(chartType))
        indices = ['$SPX.X','$COMPX','$DJI']
        message_id = self.generate_message_id(counter,lock)
        symbols = []
        if chartType == 'LISTED':
            for stock in self.subscriptions['stocks']:
                if stock in self.db.listed:
                    symbols.append(stock)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "CHART_EQUITY",
                    "requestid": message_id,
                    "command": "ADD",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3,4,5,6,7,8"
                    }
                }]}
        elif chartType == 'NASDAQ':
            for stock in self.subscriptions['stocks']:
                if stock in self.db.nasdaq:
                    symbols.append(stock)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "CHART_EQUITY",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3,4,5,6,7,8"
                    }
                }]}
        elif chartType == 'INDEX':
            for option in indices:
                symbols.append(option)
            symbols = ",".join(symbols)
            data_request = {"requests": [{
                    "service": "CHART_EQUITY",
                    "requestid": message_id,
                    "command": "ADD",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3,4,5,6,7,8"
                    }
                }]}
        elif chartType == 'FUTURE':
            if self.runFutures:
                symbols = ",".join(self.subscriptions['futures'])
            else:
                symbols = self.subscriptions['futures'][0]
            data_request = {"requests": [{
                    "service": "CHART_FUTURES",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": symbols,
                        "fields": "0,1,2,3,4,5,6"
                    }
                }]}
        # create it into a JSON string, as the API expects a JSON string.
        data_encoded = json.dumps(data_request)
        return data_encoded, message_id, lock
    
    def addChartMsg(self,userPrincipalsResponse,counter,lock,chartType='LISTED',assets=''):
        '''
        Function to create subscription message to chart history.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        chartType                        -> (str) chart type to subscribe to
        '''
        # define a request for different data sources
        message_id = self.generate_message_id(counter,lock)
        if chartType == 'LISTED':
            data_request = {"requests": [{
                    "service": "CHART_EQUITY",
                    "requestid": message_id,
                    "command": "ADD",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": assets,
                        "fields": "0,1,2,3,4,5,6,7,8"
                    }
                }]}
        elif chartType == 'NASDAQ':
            data_request = {"requests": [{
                    "service": "CHART_EQUITY",
                    "requestid": message_id,
                    "command": "ADD",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": assets,
                        "fields": "0,1,2,3,4,5,6,7,8"
                    }
                }]}
        elif chartType == 'FUTURE':
            data_request = {"requests": [{
                    "service": "CHART_FUTURES",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": assets,
                        "fields": "0,1,2,3,4,5,6"
                    }
                }]}
        # create it into a JSON string, as the API expects a JSON string.
        data_encoded = json.dumps(data_request)
        return data_encoded, message_id, lock

    def addMsg(self,userPrincipalsResponse,counter,lock,asset_type='LEVELONE_FUTURES',assets=[]):
        '''
        Function to create messgae to add symbols to subscription.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        asset_type                      -> (str) asset to add symbols to
        assets                          -> (list) symbols to add to subscription
        '''
        message_id = self.generate_message_id(counter,lock)
        if asset_type == 'QUOTE':
            asset = ",".join(assets)
            data_request = {"requests": [{
                "service": "QUOTE",
                "requestid": message_id,
                "command": "SUBS",
                "account": userPrincipalsResponse['accounts'][0]['accountId'],
                "source": userPrincipalsResponse['streamerInfo']['appId'],
                "parameters": {
                    "keys": asset,
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,23,24,25,26,28,29,30,31,32,33,34,37,40,41,42,43,44,45,46,47,48,49,50,51,52"
                }
            }]}
        elif asset_type == 'OPTION':
            asset = ",".join(assets)
            data_request = {"requests": [{
                "service": "OPTION",
                "requestid": message_id,
                "command": "SUBS",
                "account": userPrincipalsResponse['accounts'][0]['accountId'],
                "source": userPrincipalsResponse['streamerInfo']['appId'],
                "parameters": {
                    "keys": asset,
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,19,20,21,22,23,24,25,26,27,30,31,32,33,34,35,36,41"
                }
            }]}
        elif asset_type == 'LEVELONE_FUTURES':
            asset = ",".join(assets)
            data_request = {"requests": [{
                "service": "LEVELONE_FUTURES",
                "requestid": message_id,
                "command": "SUBS",
                "account": userPrincipalsResponse['accounts'][0]['accountId'],
                "source": userPrincipalsResponse['streamerInfo']['appId'],
                "parameters": {
                    "keys": asset,
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35"
                }
            }]}
        
        data_encoded = json.dumps(data_request)

        return data_encoded, message_id, lock

    def scheduleHours(self,table,aioSched,client,counter,thread_lock):
        '''
        Function to schedule start time of asset subscriptions.
        table                           -> (Dataframe) dataframe of API hours
        aioSched                        -> (object) AP Scheduler object
        client                          -> (object) WebSocket Client object
        counter                         -> (int) Count of messages sent\
                                            through socket
        thread_lock                     -> (object) lock object
        '''
        nyt = pytz.timezone('America/New_York')
        utc = pytz.timezone('UTC')
        
        startedCheckActives = False
        for index, asset in table.iterrows():
            currentNYTime = datetime.datetime.now().astimezone(nyt)
            uniTime = datetime.datetime.utcnow()
            if (asset['td_service_name']=='OPTION' or \
            asset['td_service_name']=='LEVELONE_FUTURES' or \
            asset['td_service_name']=='ACTIVES_OPTIONS' or \
            asset['td_service_name']=='ACTIVES_NYSE' or \
            asset['td_service_name']=='ACTIVES_NASDAQ'):
                self.log.info("Level 1: " + str(index))
                # Prep For Time Checks
                start = asset['td_start']
                startNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {start.hour}:{start.minute}','%m/%d/%Y %H:%M'))
                startUniTime = datetime.datetime.utcnow().replace(hour=(start.hour-int(startNYTime.utcoffset().total_seconds()/60/60)),minute=startNYTime.minute,second=0,microsecond=0)
                end = asset['td_end']
                endNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {end.hour}:{end.minute}','%m/%d/%Y %H:%M'))

                # Draft Msgs
                if asset['td_service_name'] == 'QUOTE':
                    key = 'stocks'
                elif asset['td_service_name'] == 'LEVELONE_FUTURES':
                    key = 'futures'
                elif asset['td_service_name'] == 'OPTION':
                    key = 'options'
                data_encoded, counter, thread_lock = self.initMsg(self.userPrincipalsResponse,counter,thread_lock,asset['td_service_name'], self.subscriptions[key])

                if startNYTime > currentNYTime:
                    aioSched.schedule(client.sendMessage(data_encoded),startUniTime)
                    if not startedCheckActives:
                        aioSched.schedule(self.checkActives(),startUniTime + datetime.timedelta(seconds=50))
                        startedCheckActives = True
                elif startNYTime < currentNYTime and currentNYTime < endNYTime:
                    aioSched.schedule(client.sendMessage(data_encoded),uniTime + datetime.timedelta(seconds=10))
                    if not startedCheckActives:
                        aioSched.schedule(self.checkActives(),uniTime + datetime.timedelta(seconds=50))
                        startedCheckActives = True
            elif (asset['td_service_name']=='LISTED_BOOK' or \
            asset['td_service_name']=='NASDAQ_BOOK' or \
            asset['td_service_name']=='OPTIONS_BOOK') and self.runBook:
                self.log.info("Book: " + str(index))
                # Prep For Time Checks
                start = asset['td_start']
                startNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {start.hour}:{start.minute}','%m/%d/%Y %H:%M'))
                startUniTime = datetime.datetime.utcnow().replace(hour=(start.hour-int(startNYTime.utcoffset().total_seconds()/60/60)),minute=startNYTime.minute,second=0,microsecond=0)
                end = asset['td_end']
                endNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {end.hour}:{end.minute}','%m/%d/%Y %H:%M'))

                # Draft Msgs
                if asset['td_service_name'] == 'LISTED_BOOK' or \
                asset['td_service_name']=='NASDAQ_BOOK':
                    key = 'stocks'
                elif asset['td_service_name'] == 'OPTIONS_BOOK':
                    key = 'options'
                data_encoded, counter, thread_lock = self.bookMsg(self.userPrincipalsResponse,counter,thread_lock,asset['td_service_name'])
                
                if startNYTime > currentNYTime:
                    aioSched.schedule(client.sendMessage(data_encoded),startUniTime)
                elif startNYTime < currentNYTime and currentNYTime < endNYTime:
                    localTime = datetime.datetime.utcnow()
                    aioSched.schedule(client.sendMessage(data_encoded),uniTime + datetime.timedelta(seconds=45))
            elif (asset['td_service_name']=='CHART_EQUITY' or \
             asset['td_service_name']=='CHART_FUTURES'):
                self.log.info("Charts: " + str(index))
                # Prep For Time Checks
                start = asset['td_start']
                startNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {start.hour}:{start.minute}','%m/%d/%Y %H:%M'))
                startUniTime = datetime.datetime.utcnow().replace(hour=(start.hour-int(startNYTime.utcoffset().total_seconds()/60/60)),minute=startNYTime.minute,second=0,microsecond=0)
                end = asset['td_end']
                endNYTime = nyt.localize(datetime.datetime.strptime(f'{currentNYTime.month}/{currentNYTime.day}/{currentNYTime.year} {end.hour}:{end.minute}','%m/%d/%Y %H:%M'))

                # Draft Msgs
                data_encoded, counter, thread_lock = self.chartMsg(self.userPrincipalsResponse,counter,thread_lock,str(index))
                
                if startNYTime > currentNYTime:
                    aioSched.schedule(client.sendMessage(data_encoded),startUniTime)
                elif startNYTime < currentNYTime and currentNYTime < endNYTime:
                    localTime = datetime.datetime.utcnow()
                    aioSched.schedule(client.sendMessage(data_encoded),uniTime + datetime.timedelta(seconds=45))
            elif (asset['td_service_name']=='QUOTE'):
                self.log.info("Level 1: " + str(index))

        return counter, thread_lock

    def loginBrowser(self,config):
        '''
        Log into TD Ameritrade Authentication portal and return browser object.
        config      -> (dict) Configurations passed to app with TD Ameritrade \
                        details
        '''
        # define the location of the Chrome Driver - YOU MUST CHANGE THE PATH SO IT POINTS TO YOUR CHROMEDRIVER
        executable_path = 'C:\\Program Files (x86)\\chrome-win\\chrome.exe'
        option = Options()
        option.binary_location = executable_path

        # Create a new instance of the browser, make sure we can see it (Headless = False)
        browser = Browser('chrome',executable_path='C:\\Users\\faqui\\Webdriver\\chromedriver.exe',options=option, headless=False)

        # define the components to build a URL
        method = 'GET'
        url = 'https://auth.tdameritrade.com/auth?'
        client_code = config['TD']['client_id'] + '@AMER.OAUTHAP'
        payload = {'response_type':'code', 'redirect_uri':'http://localhost/test', 'client_id':client_code}

        # build the URL and store it in a new variable
        p = requests.Request(method, url, params=payload).prepare()
        myurl = p.url
        self.log.debug("Built URL: \n")
        self.log.debug(myurl)

        # go to the URL
        browser.visit(myurl)

        # define items to fillout form
        payload = {'username': config['TD']['account_number'],
                'password': config['TD']['password']}

        time.sleep(1)
        # fill out each part of the form and click submit
        username = browser.find_by_id("username0").first.fill(payload['username'])
        password = browser.find_by_id("password1").first.fill(payload['password'])
        submit   = browser.find_by_id("accept").first.click()

        # click the Accept terms button
        browser.find_by_id("accept").first.click() 
        return browser

    def unix_time_millis(self,dt):
        '''
        Convert token timestamp to unix time in milliseconds.
        dt      -> (object) datetime object to convert
        '''
        dt = dateutil.parser.parse(dt, ignoretz = True)
        # grab the starting point, so time '0'
        epoch = datetime.datetime.utcfromtimestamp(0)
        
        return (dt - epoch).total_seconds() * 1000.0

    def generate_message_id(self,counter,lock):
        '''
        Increment counter to generate socket message id.
        counter     -> (int) Count of messages sent through socket
        lock        -> (object) lock object
        '''
        self.log.info(lock)
        with lock:
            counter += 1
            return counter

    def loginMsg(self,userPrincipalsResponse,credentials,counter,lock):
        '''
        Function to create log in message to Stream API.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream 
        credentials                     -> (dict) TD Ameritrade auth credentials
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        '''
        # define a request
        self.log.info("Generating Login Msg")
        message_id = self.generate_message_id(counter,lock)
        login_request = {"requests": [{
            "service": "ADMIN",
            "requestid": message_id,
            "command": "LOGIN",
            "account": userPrincipalsResponse['accounts'][0]['accountId'],
            "source": userPrincipalsResponse['streamerInfo']['appId'],
            "parameters": {
                "credential": urllib.parse.urlencode(credentials),
                "token": userPrincipalsResponse['streamerInfo']['token'],
                "version": "1.0"}}]}
        # create it into a JSON string, as the API expects a JSON string.
        login_encoded = json.dumps(login_request)
        return login_encoded, message_id, lock

    def logoutMsg(self,userPrincipalsResponse,counter,lock):
        '''
        Function to create log out message to Stream API.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream 
        counter                         -> (int) Count of messages sent\
                                            through socket
        lock                            -> (object) lock object
        '''
        self.log.info("Generating Logout Msg")
        message_id = self.generate_message_id(counter,lock)
        logout_request = {"requests": [{
            "service": "ADMIN",
            "requestid": message_id,
            "command": "LOGOUT",
            "account": userPrincipalsResponse['accounts'][0]['accountId'],
            "source": userPrincipalsResponse['streamerInfo']['appId'],
            "parameters": { }}]}
        logout_encoded = json.dumps(logout_request)
        return logout_encoded, message_id, lock

    async def checkActives(self):
        '''
        Function to consume messages from the Kafka Cluster. Will consume processed messages from the ACTIVES API.
        '''
        self.log.info("Checking Actives")
        kafka = kafkaClient(groupid='checkActives',kafkaAddress=self.kafkaLoc,schemaRegAddress=self.schemaLoc)
        self.consumer = kafka.consumer
        self.msg_count = 0
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                self.log.info('Polling...')
                msgs = self.consumer.consume(50,1)
                if len(msgs) == 0:
                    await asyncio.sleep(2)
                    continue
                else:
                    self.log.info('Msg: %s' % str(msgs))
                for msg in msgs:
                    if msg.error() is not None:
                        self.log.error("AvroConsumer error: {}".format(msgs.error()))
                        continue
                    key = msg.key().decode()
                    self.log.info(str(key))
                    security = kafka.avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    self.log.info(type(security))
                    if 'NASDAQ-ALL' in key or 'NYSE-ALL' in key or 'NASDAQ-60' in key or 'NYSE-60' in key:
                        self.log.info(security)
                        sym = security['SYMBOL']
                        if sym not in self.subscriptions['stocks']:
                            self.subscriptions['stocks'].append(sym)
                            data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,asset_type='QUOTE',assets=self.subscriptions['stocks'])
                            if 'NASDAQ-ALL' in key:
                                data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addChartMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'NASDAQ',assets=sym)
                            else:
                                data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addChartMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'LISTEd',assets=sym)
                            self.log.info("Active Equities: " + str(sym))
                            self.log.info("{}: {}\n".format(self.messages_counter,sym))
                            await self.client.sendMessage(data_encoded)
                    elif 'OPTS-ALL' in key or 'OPTS-60' in key:
                        self.log.info(security)
                        sym = security['SYMBOL']
                        if sym not in self.subscriptions['options']:
                            self.subscriptions['options'].append(sym)
                            data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,asset_type='OPTION',assets=self.subscriptions['options'])
                            self.log.info("Active Options: " + str(sym))
                            self.log.info("{}: {}\n".format(self.messages_counter,sym))
                            await self.client.sendMessage(data_encoded)
                    else:
                        self.log.error(key)
                        await asyncio.sleep(1)
                        continue
                self.msg_count += 1
                if self.msg_count % 5 == 0:
                        self.consumer.commit(asynchronous=False)
            except KeyboardInterrupt:
                break
            except:
                self.log.exception()
                raise Exception("Something went wrong.")

    async def activesTable(self):
        '''
        Wrapper function to check results of TD Ameritrade ACTIVES stream in Postgres databases.
        '''
        while True:
            self.log.info("Checking Actives")
            activeEquities = []
            activeOptions = []
            await self.db.checkActives(None)
            self.log.info(str(self.subscriptions))
            self.log.info(str(self.db.actives['ACTIVES_NYSE']))
            self.log.info(str(self.db.actives['ACTIVES_NASDAQ']))
            self.log.info(str(self.db.actives['ACTIVES_OPTIONS']))
            newActiveEquities = list(set(self.db.actives['ACTIVES_NYSE']) - set(self.subscriptions['stocks']))
            self.log.info('New Active Equities: \n' + str(newActiveEquities))
            activeEquities = activeEquities.append(newActiveEquities)
            '''for symbol in self.db.actives['ACTIVES_NYSE']:
                try:
                    self.log.info(symbol)
                    if symbol not in self.subscriptions['stocks']:
                        activeEquities = activeEquities.append(symbol)
                except:
                    raise Exception("Something happened.")'''
            newActiveEquities = list(set(self.db.actives['ACTIVES_NASDAQ']) - set(self.subscriptions['stocks']))
            activeEquities = activeEquities.append(newActiveEquities)
            self.log.info('Active Equities: \n' + str(activeEquities))
            '''for symbol in self.db.actives['ACTIVES_NASDAQ']:
                self.log.info(symbol)
                if symbol not in self.subscriptions['stocks']:
                    activeEquities = activeEquities.append(symbol)'''
            newActiveOptions = list(set(self.db.actives['ACTIVES_OPTIONS']) - set(self.subscriptions['options']))
            activeOptions = activeOptions.append(newActiveOptions)
            self.log.info('Active Options: \n' + str(activeOptions))
            '''for symbol in self.db.actives['ACTIVES_OPTIONS']:
                self.log.info(symbol)
                if symbol not in self.subscriptions['options']:
                    activeOptions = activeOptions.append(symbol)'''
            if len(activeEquities) == 0 and len(activeOptions) == 0:
                self.log.info("No actives to add.")
            else:
                if len(activeEquities) > 0:
                    data_encoded, self.messages_counter, self.messages_counter_thread_lock  = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'QUOTE',activeEquities)
                    self.log.info(str(activeEquities))
                    await self.client.sendMessage(data_encoded)
                    self.subscriptions['stocks'].append(activeEquities)
                    self.log.info("Added Equities to Feed: " + str(activeEquities))
                if len(activeOptions) > 0:
                    data_encoded, self.messages_counter, self.messages_counter_thread_lock  = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'OPTION',activeOptions)
                    self.log.info(str(activeOptions))
                    await self.client.sendMessage(data_encoded)
                    self.subscriptions['options'].append(activeOptions)
                    self.log.info("Added Options to Feed: " + str(activeOptions))
            await asyncio.sleep(1)

    async def stop(self):
        '''
        Log out of API. Stop receiving messages and close web socket connection.
        '''
        if self.client.connection != None:
            # we need to get the timestamp in order to make our next request, but it needs to be parsed
            tokenTimeStamp = self.userPrincipalsResponse['streamerInfo']['tokenTimestamp']
            tokenTimeStampAsMs = self.unix_time_millis(tokenTimeStamp)
            # we need to define our credentials that we will need to make our stream
            credentials = {"userid": self.userPrincipalsResponse['accounts'][0]['accountId'],
                        "token": self.userPrincipalsResponse['streamerInfo']['token'],
                        "company": self.userPrincipalsResponse['accounts'][0]['company'],
                        "segment": self.userPrincipalsResponse['accounts'][0]['segment'],
                        "cddomain": self.userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
                        "usergroup": self.userPrincipalsResponse['streamerInfo']['userGroup'],
                        "accesslevel":self.userPrincipalsResponse['streamerInfo']['accessLevel'],
                        "authorized": "Y",
                        "timestamp": int(tokenTimeStampAsMs),
                        "appid": self.userPrincipalsResponse['streamerInfo']['appId'],
                        "acl": self.userPrincipalsResponse['streamerInfo']['acl'] }
            logout_encoded, self.messages_counter, self.messages_counter_thread_lock = self.logoutMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock)
            
            try:
                await asyncio.wait_for(self.client.sendMessage(logout_encoded),timeout=10.0)
            except TimeoutError:
                self.log.info('Timed out trying to log out.')
                if self.client.canClose == True:
                    self.loop.stop()
                    self.loop.close()
                    #await self.client.connection.close()
                    #await self.client.connection.wait_closed()
                    self.log.info("Connection Closed")
                    self.db.exit()
                return
            if self.client.canClose == True:
                self.loop.stop()
                self.loop.close()
                #await self.client.connection.close()
                #await self.client.connection.wait_closed()
                self.log.info("Connection Closed")
                self.endTime = time.time()

                # Update RunHistory With EndTime
                inserted = int(len(self.subscriptions['stocks']) + len(self.subscriptions['options']) + len(self.subscriptions['futures']))
                self.db.cur.execute('''
                    UPDATE PUBLIC.financedb_RUNHISTORY
                    SET "Endtime"=%s,
                        "SymbolsInsert"=%s,
                    WHERE "Id"=%s
                ''' % (self.endTime,inserted,self.runId))

                self.db.exit()
        else:
            self.log.error('No existing connection to close.')
