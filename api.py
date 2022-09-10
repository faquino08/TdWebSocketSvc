import logging
import sys
import datetime
from os import path, environ
from urllib import request, parse

import configparser
import requests
import json
import time
from splinter import Browser
from selenium.webdriver.chrome.options import Options

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from flask import Flask, request, g, session
from flask_restful import Api
from flask_apscheduler import APScheduler

from DataBroker.tdstreams import tdStreams

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

dataHours = [
    "7",
    "7",
    "7",
    "7",
    "7",
    "None",
    "18"
]

def get_access_token(browser,logger,code=None):
    config = configparser.ConfigParser()
    config.read('config.ini')
    #browser = loginBrowser(config)

    # give it a second, then grab the url
    if code != None:
        smscode = browser.find_by_id("smscode0").first.fill(code)
        #trust = browser.find_by_css("input[name=rememberdevice]").first.click()
        submit = browser.find_by_id("accept").first.click()
        trust = browser.find_by_css("label[for=trustthisdevice0_0]").first.click()
        submitTrust = browser.find_by_id("accept").first.click()
        accept = browser.find_by_id("accept").first.click()
    keepWaiting = True
    while keepWaiting:
        if 'code=' in browser.url:
            new_url = browser.url
            parse_url = parse.unquote(new_url.split('code=')[1])

            # close the browser
            browser.quit()
            
            logger.info("Pulled Code, grabbing access token.")
            
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

def loginBrowser(config,logger):
    # define the location of the Chrome Driver - YOU MUST CHANGE THE PATH SO IT POINTS TO YOUR CHROMEDRIVER
    executable_path = '/usr/bin/chromium'
    option = Options()
    option.add_argument("--no-sandbox")
    option.binary_location = executable_path

    # Create a new instance of the browser, make sure we can see it (Headless = False)
    browser = Browser('chrome',options=option, headless=True)

    # define the components to build a URL
    method = 'GET'
    url = 'https://auth.tdameritrade.com/auth?'
    client_code = config['TD']['client_id'] + '@AMER.OAUTHAP'
    payload = {'response_type':'code', 'redirect_uri':'http://localhost/test', 'client_id':client_code}

    # build the URL and store it in a new variable
    p = requests.Request(method, url, params=payload).prepare()
    myurl = p.url
    logger.info(myurl)

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

def create_app(kafka_location,debug=False):
    cache = {}
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/TDStreamFlask_{datetime.date.today()}.txt')],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/TDStreamFlask_{datetime.date.today()}.txt') ],
        )
    cache['logger'] = logging.getLogger(__name__)
    app = Flask(__name__)
    app.config.from_object(Config())
    # initialize scheduler
    scheduler = APScheduler()
    scheduler.init_app(app)

    config = configparser.ConfigParser()
    config.read('config.ini')

    timeNow = datetime.datetime.now()
    hourNow = timeNow.hour()
    minuteNow = timeNow.minute()
    weekdayNow = timeNow.weekday()
    startTime = dataHours[weekdayNow]
    if(hourNow >= int(startTime)):
        cache['browser'] = loginBrowser(config,logger=cache['logger'])
    elif(minuteNow >= 55):
        cache['browser'] = loginBrowser(config,logger=cache['logger'])

    @scheduler.task('cron', id='stream', minute='0', hour='6', day_of_week='mon-fri', timezone='America/New_York')
    def login():
        config = configparser.ConfigParser()
        config.read('config.ini')
        cache['browser'] = loginBrowser(config,logger=cache['logger'])
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        })

    @app.route("/run", methods=['GET'])
    def loginFlow():
        config = configparser.ConfigParser()
        config.read('config.ini')
        cache['browser'] = loginBrowser(config,logger=cache['logger'])
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        })

    @app.route("/two_fa", methods=['POST'])
    def sms_received():
        msg = request.json['code']
        header = get_access_token(cache['browser'],cache['logger'],msg)
        tdStreams(headers=header,kafkaLocation=kafka_location,debug=debug)
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        })

    @app.route("/", methods=['GET'])
    def testApi():
        return json.dumps({
        'status':'success'
        })

    def runStreams():
        return tdStreams(kafkaLocation=kafka_location)
    
    return app

def convert(s): 
    if s == "True" or s =="true" or s == "TRUE": 
        return True
    return False

debugBool = convert(environ['STREAM_DEBUG'])
print(debugBool)
app = create_app(kafka_location='10.6.47.45',debug=debugBool)
#app.run(host='0.0.0.0',debug=False,port=8081)