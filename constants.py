from pathlib import Path
import ast
from os import environ
from datetime import datetime, timedelta
import logging
import json

def convert(s): 
    if s == "True" or s =="true" or s == "TRUE": 
        return True
    return False

logger = logging.getLogger(__name__)

def lastWeekday():
    '''
    Get yesterday's date if it is a weekday or else get last weekday before that.
    '''
    today = datetime.today()
    logger.info("Weekday: " + str(today.isoweekday()))
    if today.isoweekday() == 1:
        lastWeekDay = today - timedelta(3)
    elif today.isoweekday() > 5:
        lastWeekDay = today - timedelta(today.isoweekday()-5)
    else: 
        lastWeekDay = today - timedelta(1)
    return lastWeekDay


#debugBool = convert(environ.get('STREAM_DEBUG','False'))
DEBUG = json.loads(environ['DEBUG_BOOL'].lower())
BOOTSTRAP_SERVER = environ.get('BOOTSTRAP_SERVER','localhost:9092')
SCHEMA_REG_ADDRESS = environ.get('SCHEMA_REG_ADDRESS','localhost:9092')
POSTGRES_LOCATION = environ['POSTGRES_LOCATION']
POSTGRES_PORT = environ['POSTGRES_PORT']
POSTGRES_DB = environ['POSTGRES_DB']
POSTGRES_USER = environ['POSTGRES_USER']
POSTGRES_PASSWORD = environ['POSTGRES_PASSWORD']
INGRESS_TOPIC = environ.get('INGRESS_TOPIC','rawTDData')
APP_NAME = environ.get('APP_NAME','TdStream')
BOOKRUN = environ.get('BOOKRUN',False)
SECURITY_ANSWERS = ast.literal_eval(environ.get("SECURITY_ANSWERS",None))