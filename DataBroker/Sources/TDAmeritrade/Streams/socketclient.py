import websockets
import asyncio
import json
from os import environ
import pyodbc
import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import create_engine
import pandas as pd
import pandas.io.sql as sqlio
import logging
import sys

class WebSocketClient():
    def __init__(self,userPrincipalsResponse,debug=False):
        self.data_holder = []
        self.file = open('td_ameritrade_data.txt', 'a')
        self.cnxn = None
        self.crsr = None
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
        
        # define the server and the database, YOU WILL NEED TO CHANGE THIS TO YOUR OWN DATABASE AND SERVER
        server = environ.get('TIMESCALE_ADDRESS','10.6.47.45')
        port = '5432'
        database = environ.get('TIMESCALE_DB','postgres')  
        sql_driver = '{PostgreSQL Unicode}'
        me = environ.get('TIMESCALE_USER','postgres')
        pwd = environ.get('TIMESCALE_PWD','Mollitiam-0828')

        alchemyStr = f"postgresql+psycopg2://{me}:{pwd}@{server}/{database}"
        connAlch = create_engine(alchemyStr).connect()
        hoursSql = "SELECT  * FROM \"DATASERVICEHOURS\";"
        hours = sqlio.read_sql_query(hoursSql,connAlch,index_col="asset_type")
        self.producer = KafkaProducer(bootstrap_servers=['10.6.47.45:9092'],
                                        value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                        retries=3, linger_ms=30000, batch_size=10000000)
        return hours    

    def on_send_success(self,record_metadata):
        self.log.info(record_metadata.topic)
        self.log.info(record_metadata.partition)
        self.log.info(record_metadata.offset)

    def on_send_error(excp):
        log.error('I am an errback', exc_info=excp)
        # handle exception
        
    def database_insert(self, query, topic):   
        
        # execute the query, commit the changes, and close the connection
        '''
        self.crsr.execute(query, data_tuple)
        self.cnxn.commit()
        self.cnxn.close()

        print('Data has been successfully inserted into the database.')
        '''
        self.log.info("Inserting \n" + '-'*20)
        self.producer.send(topic, query)


    async def connect(self):
        '''
            Connecting to webSocket server
            websockets.connect returns a WebSocketClientProtocol, which is used to send and receive messages
        '''
        
        # define the URI of the data stream, and connect to it.
        uri = "wss://" + self.userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws"
        self.hours = self.database_connect()
        self.log.info('Database Connection established. Client correctly connected')
        self.connection = await websockets.connect(uri,compression=None)
        
        # if all goes well, let the user know.
        if self.connection.open:
            self.log.info('TD Ameritrade Connection established. Client correctly connected')
            return self.connection, self.hours

    async def sendMessage(self, message):
        '''
            Sending message to webSocket server
        '''
        #print("Sent: " + message)
        await self.connection.send(message)

    def getList(dict):
        return dict.keys()  

    async def receiveMessage(self, connection):
        '''
            Receiving all server messages and handle them
        '''
        while True:
            try:
                
                # grab and decode the message
                data_tuple = ('service', 'timestamp', 'command')
                message = await connection.recv()
                message = str(message).replace(' \"C\" ','C')
                self.log.debug(message)                
                message_decoded = json.loads(message)
                self.log.debug(message_decoded)
                # prepare data for insertion, connect to database
                futuresQuery = "INSERT INTO public.td_service_data (service, timestamp, command) VALUES (?,?,?);"
                nasdaqQuery = "INSERT INTO public.td_service_data (service, timestamp, command) VALUES (?,?,?);"
                
                # check if the response contains a key called data if so then it contains the info we want to insert.'
                self.database_insert(message_decoded, 'rawTDdata')
            except websockets.exceptions.ConnectionClosed:            
                self.log.info('Connection with server closed')
                break
                
    async def heartbeat(self, connection):
        '''
            Sending heartbeat to server every 5 seconds
            Ping - pong messages to verify connection is alive
        '''
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                self.log.info('Connection with server closed')
                break