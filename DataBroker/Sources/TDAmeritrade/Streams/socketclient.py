import websockets
import asyncio
import json
from os import environ
import pyodbc
import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import pandas.io.sql as sqlio
import logging
import sys
from StreamingFlaskDocker.constants import APP_NAME

from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, BOOTSTRAP_SERVER, INGRESS_TOPIC

class WebSocketClient():
    def __init__(self,userPrincipalsResponse={},debug=False):
        '''
        Class to handle the websocket connections to TD Ameritrade Streaming API.
        userPrincipalsResponse          -> (dict) Dict with credentials for \
                                            Stream API
        debug                           -> (boolean) whether to log debug\
                                            messages
        '''
        self.data_holder = []
        self.cnxn = None
        self.crsr = None
        self.canClose = False
        self.userPrincipalsResponse = userPrincipalsResponse
        logger = logging.getLogger(__name__)
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
        
    def database_connect(self):
        '''
        Create sqlAlchemy connection and get the table of Stream API hours from Postgres database. Creates Kafka Producer.
        '''
        alchemyStr = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_LOCATION}:{POSTGRES_PORT}/{POSTGRES_DB}?application_name={APP_NAME}_WebSocketClient_Alchemy"

        connAlch = create_engine(alchemyStr).connect()
        Session = sessionmaker(connAlch)
        hoursSql = "SELECT  * FROM \"DATASERVICEHOURS\";"
        hours = sqlio.read_sql_query(hoursSql,connAlch,index_col="asset_type")
        connAlch.close()
        self.log.info('Db Exit Status:')
        self.log.info('SqlAlchemy:')
        self.log.info(connAlch.closed)
        self.producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],
                                        api_version=(2, 5, 0),
                                        value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                        retries=3, linger_ms=30000, batch_size=10000000)
        return hours    
        
    def database_insert(self, query, topic):   
        '''
        Send messages to Kafka Cluster for stream processing.
        query       -> (Object) Stream messages to send
        topic       -> (str) Name of topic for intake of messages
        '''
        self.producer.send(topic, query)
        self.producer.flush()
        self.log.debug("Msg Sent \n" + '-'*20)

    async def connect(self):
        '''
        Connecting to webSocket server.
        websockets.connect returns a WebSocketClientProtocol, which is used to send and receive messages.
        '''
        # define the URI of the data stream, and connect to it.
        uri = "wss://" + self.userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws"
        self.hours = self.database_connect()
        self.log.info('Database Connection established. Client correctly connected')
        self.connection = await websockets.connect(uri,compression=None,max_size=3000000)
        
        # if all goes well, let the user know.
        if self.connection.open:
            self.log.info('TD Ameritrade Connection established. Client correctly connected')
            return self.connection           

    async def sendMessage(self, message):
        '''
        Send subcription message to webSocket server
        message     -> (dict) Subscription message for Stream API
        '''
        self.log.info("Sent: " + message)
        await self.connection.send(message) 

    async def receiveMessage(self, connection, runFutures):
        '''
        Receiving all server messages and handle them.
        connection      -> (websocket object) Websocket to listen to for \
                            messages.
        '''
        while True:
            try:
                
                # grab and decode the message
                data_tuple = ('service', 'timestamp', 'command')
                message = await asyncio.wait_for(connection.recv(), timeout=60)
                message = str(message).replace(' \"C\" ','C')
                message_decoded = json.loads(message)
                toDel = []
                if 'data' in message_decoded.keys():
                    self.log.info("Services Returned: " + str(len(message_decoded['data'])))
                    for i in range(len(message_decoded['data'])):
                        self.log.info("Inserting \n" + str(message_decoded['data'][i].get('service')))
                        if str(message_decoded['data'][i].get('service')) == 'LEVELONE_FUTURES' and not runFutures:
                            toDel.append(i)
                for d in toDel:
                    del message_decoded['data'][d]
                if 'response' in message_decoded.keys():    
                    if message_decoded['response'][0].get('command') == 'LOGOUT' and message_decoded['response'][0].get('content').get('code') != 0:
                        self.log.error("Logout Failed")
                        raise Exception("Logout Failed")
                    elif message_decoded['response'][0].get('command') == 'LOGOUT' and message_decoded['response'][0].get('content').get('code') == 0:
                        self.canClose = True
                        self.log.info("Logged Out of Td Ameritrade")
                        return
                
                # check if the response contains a key called data if so then it contains the info we want to insert.'
                self.database_insert(message_decoded, INGRESS_TOPIC)
            except websockets.exceptions.ConnectionClosed:            
                self.log.info('Connection with server closed')
                break
                
    async def heartbeat(self, connection):
        '''
        Sending heartbeat to server every 5 seconds.
        Ping - pong messages to verify connection is alive.
        connection      -> (websocket object) Websocket to listen to for \
                            messages.
        '''
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                self.log.info('Connection with server closed')
                break