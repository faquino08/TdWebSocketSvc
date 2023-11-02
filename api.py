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

from quart import Quart, request, g, session

from DataBroker.tdstreams import TdStreams

from constants import DEBUG, BOOTSTRAP_SERVER, SECURITY_ANSWERS

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

dataHours = [
    "7",
    "7",
    "7",
    "7",
    "7",
    None,
    "18"
]

headerCode = ''

def get_access_token(browser,logger,code=None):
    '''
    Grab access token from the browser login flow.
    logger      -> (object) logging object
    code        -> (str) Two FA code
    '''
    config = configparser.ConfigParser()
    config.read('config.ini')

    # give it a second, then grab the url
    if code != None:
        smscode = browser.find_by_id("smscode0").first.fill(code)
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

class loginBrowser():
    def __init__(self,config,logger,securityQuestions=False,streams=None,runFutures=False):
        '''
        Class to handle browser login at start of workflow.
        config      -> (dict) TD Ameritrade credentials
        logger      -> (object) logging object
        '''
        self.config = config
        self.logger = logger
        self.securityAnswers = SECURITY_ANSWERS
        self.securityQuestions = securityQuestions
        if self.securityQuestions:
            self.streams = streams
            self.runFutures = runFutures

    def run(self):
        '''
        Function to run first part of browser login flow up to two factor authentication.
        '''
        # define the location of the Chrome Driver - YOU MUST CHANGE THE PATH SO IT POINTS TO YOUR CHROMEDRIVER
        executable_path = '/usr/bin/chromium'
        option = Options()
        option.add_argument("--no-sandbox")
        option.binary_location = executable_path

        # Create a new instance of the browser, make sure we can see it (Headless = False)
        self.browser = Browser('chrome',options=option, headless=True)

        # define the components to build a URL
        method = 'GET'
        url = 'https://auth.tdameritrade.com/auth?'
        client_code = self.config['TD']['client_id'] + '@AMER.OAUTHAP'
        payload = {'response_type':'code', 'redirect_uri':'http://localhost/test', 'client_id':client_code}

        # build the URL and store it in a new variable
        p = requests.Request(method, url, params=payload).prepare()
        myurl = p.url
        self.logger.debug(myurl)

        # go to the URL
        self.browser.visit(myurl)

        # define items to fillout form
        payload = {'username': self.config['TD']['account_number'],
                'password': self.config['TD']['password']}

        time.sleep(1)
        # fill out each part of the form and click submit
        self.browser.find_by_id("username0").first.fill(payload['username'])
        self.browser.find_by_id("password1").first.fill(payload['password'])
        self.browser.find_by_id("accept").first.click()

        if not self.securityQuestions:
            # SMS Auth
            # Accept Terms
            self.browser.find_by_id("accept").first.click()
        else:
            self.browser.execute_script('document.getElementsByClassName("alternates")[0].click();')
            self.browser.execute_script('document.getElementsByClassName("alt_cta")[0].click();')
            time.sleep(2)

            # Question
            question = self.browser.find_by_css('.description > p:nth-of-type(2)').text
            self.logger.info(question)
            self.logger.info(self.securityAnswers)
            for index, value in self.securityAnswers.items():
                if index in question:
                    self.browser.find_by_id("secretquestion0").first.fill(value)
                    break
            self.browser.find_by_id("accept").first.click()

            time.sleep(2)
            self.browser.find_by_css("label[for=trustthisdevice0_0]").first.click()
            self.browser.find_by_id("accept").first.click()
            self.browser.find_by_id("accept").first.click()
            keepWaiting = True
            while keepWaiting:
                if 'code=' in self.browser.url:
                    new_url = self.browser.url
                    parse_url = parse.unquote(new_url.split('code=')[1])

                    # close the browser
                    self.browser.quit()
                    
                    self.logger.info("Pulled Code, grabbing access token.")
                    
                    # THE AUTHENTICATION ENDPOINT
                    # define the endpoint
                    url = r"https://api.tdameritrade.com/v1/oauth2/token"

                    # define the headers
                    headers = {"Content-Type":"application/x-www-form-urlencoded"}

                    # define the payload
                    payload = {'grant_type': 'authorization_code', 
                            'access_type': 'offline', 
                            'code': parse_url, 
                            'client_id':self.config['TD']['client_id'], 
                            'redirect_uri':'http://localhost/test'}

                    # post the data to get the token
                    authReply = requests.post(r'https://api.tdameritrade.com/v1/oauth2/token', headers = headers, data=payload)

                    # convert it to a dictionary
                    decoded_content = authReply.json()    
                    
                    # grab the access_token
                    access_token = decoded_content['access_token']
                    header = {'Authorization': "Bearer {}".format(access_token)}
                    keepWaiting = False

                    app.add_background_task(self.streams.run,header,self.runFutures)
                    return 
                else:
                    time.sleep(5)
        return 

def create_app(debug=False):
    '''
    Function that creates our Quart application.
    kafka_location  -> Connection string to the kafka cluster
    debug           -> Boolean of whether to log debug messages
    '''
    cache = {}
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/TDStreamFlask_{datetime.date.today()}.txt'), logging.StreamHandler()],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/TDStreamFlask_{datetime.date.today()}.txt'), logging.StreamHandler()],
        )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    app = Quart(__name__)
    app.config.from_object(Config())
    # initialize scheduler
    #scheduler = APScheduler()
    #scheduler.init_app(app)

    config = configparser.ConfigParser()
    config.read('config.ini')

    tzInfo  = datetime.timezone(datetime.timedelta(hours=-4))
    timeNow = datetime.datetime.now(tz=tzInfo)
    hourNow = timeNow.hour
    minuteNow = timeNow.minute
    weekdayNow = timeNow.weekday()
    startTime = dataHours[weekdayNow]
    cache['streams'] = TdStreams()
    if startTime is not None:
        if(hourNow == int(startTime)):
            cache['browser'] = loginBrowser(config,logger=logger,streams=cache['streams'],runFutures=False)
        elif(hourNow < int(startTime) and minuteNow >= 55):
            cache['browser'] = loginBrowser(config,logger=logger,streams=cache['streams'],runFutures=False)

    #@scheduler.task('cron', id='stream', minute='0', hour='6', day_of_week='mon-fri', timezone='America/New_York')
    def login():
        config = configparser.ConfigParser()
        config.read('config.ini')
        cache['browser'] = loginBrowser(config,logger=logger,streams=cache['streams'],runFutures=False)
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        }), cache['browser'].run()

    @app.route("/run", methods=['POST'])
    async def loginFlow():
        msg = await request.get_json()
        runFutures = str(msg['futuresRun']).lower() in ['true','1']
        if 'securityQuestions' in list(msg.keys()):
            securityQuestions = str(msg['securityQuestions']).lower() in ['true','1']
        else:
            securityQuestions = False
        config = configparser.ConfigParser()
        config.read('config.ini')
        cache['browser'] = loginBrowser(config,logger=logger,securityQuestions=securityQuestions,streams=cache['streams'],runFutures=runFutures)
        app.add_background_task(cache['browser'].run)
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        })

    @app.route("/stop", methods=['GET'])
    async def logoutFlow():
        if cache['streams'].streamHandler == None:
            return json.dumps({
                'status':'failed',
            })
        else:
            app.add_background_task(cache['streams'].streamHandler.stop)
            return json.dumps({
            'status':'success',
            #'text': reminder_text,
            })

    @app.route("/two_fa", methods=['POST'])
    async def sms_received():
        msg = await request.get_json()
        code = msg['code']
        runFutures = str(msg['futuresRun']).lower() in ['true','1']
        header = get_access_token(cache['browser'].browser,logger,code)
        app.add_background_task(cache['streams'].run,header,runFutures)
        return json.dumps({
        'status':'success',
        #'text': reminder_text,
        })

    @app.route("/", methods=['GET'])
    async def testApi():
        return json.dumps({
        'status':'success'
        })

    def runStreams():
        return TdStreams()

    #scheduler.start()
    return app

app = create_app(debug=DEBUG)