from pprint import pprint
import urllib
import requests
import dateutil.parser
import datetime
from aioscheduler import TimedScheduler
import logging
#from TdAmeritradeStream import TDAuthentication
import configparser
import json
import time
import pytz
import threading
from splinter import Browser
from selenium.webdriver.chrome.options import Options
import asyncio
import nest_asyncio
from DataBroker.Sources.TDAmeritrade.Streams.database import databaseHandler
from DataBroker.Sources.TDAmeritrade.Streams.socketclient import WebSocketClient
import argparse
from DataBroker.Sources.TDAmeritrade.Streams.kafkaClient import kafkaClient

nest_asyncio.apply()
logger = logging.getLogger(__name__)

class streams:
    def __init__(self,header=None,stocks=None,options=None,futures=None,debug=False,kafkaAddress='10.6.47.45'):
        # we need to go to the User Principals endpoint to get the info we need to make a streaming request
        endpoint = "https://api.tdameritrade.com/v1/userprincipals"
        self.subscriptions = {'stocks': None,'options': None,'futures': None}
        self.kafkaLoc = kafkaAddress

        if logger != None:
            self.log = logger
        else:
            self.log = logging.getLogger(__name__)
        if debug:
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
        date = dateutil.parser.parse(tokenTimeStamp, ignoretz = True)
        tokenTimeStampAsMs = self.unix_time_millis(date)

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
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.run(self.main(credentials,self.userPrincipalsResponse),debug=True)

    async def main(self,credentials,userPrincipalsResponse,assets={'stocks': None,'options': None,'futures': None}):
        # Creating client object
        self.client = WebSocketClient(userPrincipalsResponse)

        self.messages_counter = 0
        self.messages_counter_thread_lock = threading.Lock()
        
        loop = asyncio.get_event_loop()
        login_encoded, self.messages_counter, self.messages_counter_thread_lock = self.loginMsg(userPrincipalsResponse,credentials,self.messages_counter,self.messages_counter_thread_lock)
        data_encoded, self.messages_counter, self.messages_counter_thread_lock  = self.initMsg(userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,'LEVELONE_FUTURES')
        # Start connection and get client connection protocol
        self.connection, hours = await self.client.connect()
        self.log.info("Connected")
        # Start listener and heartbeat 
        #tasks = [asyncio.ensure_future(client.receiveMessage(connection)),
        #         asyncio.ensure_future(client.sendMessage(login_encoded)),
        #         asyncio.ensure_future(client.receiveMessage(connection)),
        #         asyncio.ensure_future(client.sendMessage(data_encoded)),
        #         asyncio.ensure_future(client.receiveMessage(connection)),]
        scheduler = TimedScheduler()
        scheduler.start()

        self.messages_counter,self.messages_counter_thread_lock = self.scheduleHours(hours,scheduler,self.client,self.messages_counter,self.messages_counter_thread_lock)

        await asyncio.gather(
            self.client.sendMessage(login_encoded),
            #self.client.sendMessage(data_encoded),
            self.client.receiveMessage(self.connection),
            self.checkActives()
        )

    def initMsg(self,userPrincipalsResponse,counter,lock,asset_type='LEVELONE_FUTURES',assets={'stocks': None,'options': None,'futures': None}):
        # define a request for different data sources
        message_id = self.generate_message_id(counter,lock)
        #self.log.info(message_id)
        if asset_type == 'QUOTE':
            if self.subscriptions['stocks'] != None:
                asset = ",".join(self.subscriptions['stocks'])
                print(asset)
                data_request = {"requests": [{
                    "service": "QUOTE",
                    "requestid": message_id,
                    "command": "SUBS",
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "keys": asset,
                        "fields": "0,1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,23,24,25,26,28,29,30,31,32,33,34,37,40,41,42,43,44,45,46,47,48,49"
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
        elif asset_type == 'OPTION':
            if self.subscriptions['options'] != None:
                asset = ",".join(self.subscriptions['options'])
                print(asset)
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
                asset = ",".join(self.subscriptions['futures'])
                print(asset)
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
            self.log.info(self.subscriptions)
        # create it into a JSON string, as the API expects a JSON string.

        data_encoded = json.dumps(data_request)

        return data_encoded, message_id, lock

    def addMsg(self,userPrincipalsResponse,counter,lock,asset_type='LEVELONE_FUTURES',assets=[]):
        message_id = self.generate_message_id(counter,lock)
        #self.log.info(message_id)
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
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,18,19,23,24,25,26,28,29,30,31,32,33,34,37,40,41,42,43,44,45,46,47,48,49"
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
        est = pytz.timezone('US/Eastern')
        localTime = datetime.datetime.now(tz=est)
        #localTime = localTime.isoformat()
        for index, asset in table.iterrows():
            if asset['td_service_name'] != 'LEVELONE_FUTURES':
                self.log.info(index)
                start = asset['td_start']
                startDateTime = datetime.datetime(year=localTime.year,month=localTime.month,day=localTime.day,hour=start.hour,minute=start.minute,tzinfo=datetime.timezone(datetime.timedelta(hours=-4))).timestamp()
                startDateTime = datetime.datetime.utcfromtimestamp(startDateTime)

                end = asset['td_end']
                endDateTime = datetime.datetime(year=localTime.year,month=localTime.month,day=localTime.day,hour=end.hour,minute=end.minute,tzinfo=datetime.timezone(datetime.timedelta(hours=-4))).timestamp()
                endDateTime = datetime.datetime.utcfromtimestamp(endDateTime)

                if asset['td_service_name'] == 'QUOTE':
                    key = 'stocks'
                elif asset['td_service_name'] == 'LEVELONE_FUTURES':
                    key = 'futures'
                elif asset['td_service_name'] == 'OPTION':
                    key = 'options'
                data_encoded, counter, thread_lock = self.initMsg(self.userPrincipalsResponse,counter,thread_lock,asset['td_service_name'], self.subscriptions[key])
                self.log.info(endDateTime)
                self.log.info(localTime)
                if startDateTime > localTime:
                    aioSched.schedule(client.sendMessage(data_encoded),startDateTime)
                elif localTime < endDateTime:
                    localTime = datetime.datetime.utcnow()
                    aioSched.schedule(client.sendMessage(data_encoded),localTime + datetime.timedelta(seconds=5))
        return counter, thread_lock

    def get_access_token(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        browser = self.loginBrowser(config)

        # give it a second, then grab the url
        keepWaiting = True
        while keepWaiting:
            if 'code=' in browser.url:
                new_url = browser.url
                parse_url = urllib.parse.unquote(new_url.split('code=')[1])

                # close the browser
                browser.quit()
                
                self.log.info("Pulled Code, grabbing access token.")
                
                # THE AUTHENTICATION ENDPOINT

                # define the endpoint
                url = r"https://api.tdameritrade.com/v1/oauth2/token"

                # define the headers
                headers = {"Content-Type":"application/x-www-form-urlencoded"}

                # define the payload
                payload = {'grant_type': 'authorization_code', 
                        'access_type': 'offline', 
                        'code': parse_url, 
                        'client_id':config['TD']['client_id'], 
                        'redirect_uri':'http://localhost/test'}

                # post the data to get the token
                authReply = requests.post(r'https://api.tdameritrade.com/v1/oauth2/token', headers = headers, data=payload)

                # convert it to a dictionary
                decoded_content = authReply.json()    
                
                # grab the access_token
                access_token = decoded_content['access_token']
                headers = {'Authorization': "Bearer {}".format(access_token)}
                keepWaiting = False
                return headers
            else:
                time.sleep(5)

    def loginBrowser(self,config):
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
        self.log.info(myurl)

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
        
        # grab the starting point, so time '0'
        epoch = datetime.datetime.utcfromtimestamp(0)
        
        return (dt - epoch).total_seconds() * 1000.0

    def generate_message_id(self,counter,lock):
        self.log.info(lock)
        with lock:
            counter += 1
            return counter

    def loginMsg(self,userPrincipalsResponse,credentials,counter,lock):
        # define a request
        self.log.info("Generating")
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

    async def checkActives(self):
        self.consumer = kafkaClient(groupid='testing_checkActives1',kafkaAddress=self.kafkaLoc).consumer
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(30)
                    continue

                if msg.error():
                    self.log.info("AvroConsumer error: {}".format(msg.error()))
                    return

                val = msg.value()
                key = msg.key()

                if 'NASDAQ' in key or 'NYSE' in key:
                    sym = val['SYMBOL']
                    if sym not in self.subscriptions['stocks']:
                        data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,asset_type='QUOTE',assets=[sym])
                        self.log.info("{}: {}\n".format(self.messages_counter,sym))
                        await self.client.sendMessage(data_encoded)
                        self.subscriptions['stocks'].append(sym)
                elif 'OPT' in key:
                    sym = val['SYMBOL']
                    if sym not in self.subscriptions['options']:
                        data_encoded, self.messages_counter, self.messages_counter_thread_lock = self.addMsg(self.userPrincipalsResponse,self.messages_counter,self.messages_counter_thread_lock,asset_type='OPTION',assets=[sym])
                        self.log.info("{}: {}\n".format(self.messages_counter,sym))
                        await self.client.sendMessage(data_encoded)
                        self.subscriptions['options'].append(sym)

            except KeyboardInterrupt:
                break