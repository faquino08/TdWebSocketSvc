from shutil import register_unpack_format
from numpy import percentile
import pandas as pd
import logging
import psycopg2
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
from dateutil import tz
from StreamingFlaskDocker.constants import APP_NAME, lastWeekday

logger = logging.getLogger(__name__)
class DatabaseHandler:
    def __init__(self,postgresParams={}):
        '''
        Database wrapper for common SQL queries and handling database connection.
        postgresParams  -> Dict with keys host, port, database, user, \
                            password for Postgres database
        '''
        self.log = logger
        alchemyStr = f"postgresql+psycopg2://{postgresParams['user']}:{postgresParams['password']}@{postgresParams['host']}:{postgresParams['port']}/{postgresParams['database']}?application_name={APP_NAME}_DatabaseHandler_Alchemy"
        self.conn = psycopg2.connect(**postgresParams)
        self.cur = self.conn.cursor()
        #self.connAlch = create_engine(alchemyStr).connect()
        #self.Session = sessionmaker(self.connAlch)
        self.actives = {
            "ACTIVES_OPTIONS":[],
            "ACTIVES_NYSE": [],
            "ACTIVES_NASDAQ": []
            }

    def exit(self):
        '''
        Exit class and close Postgres connection
        '''
        self.connAlch.close()
        self.log.info('Db Exit Status:')
        self.log.info('SqlAlchemy:')
        self.log.info(self.connAlch.closed)

    def checkActives(self,date):
        '''
        Function to check results of TD Ameritrade ACTIVES stream in Postgres databases.
        date    -> (str) Begin date of symbols in ACTIVES stream "YYYY-mm-dd"
        '''
        self.log.info('checkActives')

        if date == None:
            date = datetime.date.today().strftime("%Y-%m-%d")
        elif len(date) == 0:
            date = datetime.date.today().strftime("%Y-%m-%d")

        activesbytradesSql = "\
            WITH X AS ( \
	        SELECT DISTINCT \"SYMBOL\", \"SERVICE\", \
            DATE(to_timestamp(\"TIMESTAMP\"/1000) AT TIME ZONE 'est') FROM \
            PUBLIC.\"kafka_TdActivesByTrades\") SELECT * FROM X WHERE \
            DATE>='%s'" % date
        activesbysharesSql = "\
            WITH X AS ( \
	        SELECT DISTINCT \"SYMBOL\", \"SERVICE\", \
            DATE(to_timestamp(\"TIMESTAMP\"/1000) AT TIME ZONE 'est') FROM \
            PUBLIC.\"kafka_TdActivesByShares\") SELECT * FROM X WHERE \
            DATE>='%s'" % date
        try:
            self.cur.execute(activesbytradesSql)
            self.activesbytrades = pd.DataFrame(self.cur.fetchall(),columns=['SERVICE','TIMESTAMP','ACTIVETYPE','TICKER','ID','sampleDuration','StartTime','DisplayTime','NumGroups','GROUPNUM','GROUPSIZE','TOTVOL','SYMBOL','VOLUME','PERCENT'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''self.activesbytrades = sqlio.read_sql_query(activesbytradesSql, self.connAlch,index_col='SYMBOL')
        actives = []
        for df in self.activesbytrades:
            if type(df) == pd.DataFrame:
                actives.append(df)
        if len(actives) > 0:    
            self.activesbytrades = pd.concat(actives)'''
        try:
            self.activesbyshares = self.cur.execute(activesbysharesSql)
            self.activesbyshares = pd.DataFrame(self.cur.fetchall(),columns=['SERVICE','TIMESTAMP','ACTIVETYPE','TICKER','ID','sampleDuration','StartTime','DisplayTime','NumGroups','GROUPNUM','GROUPSIZE','TOTVOL','SYMBOL','VOLUME','PERCENT'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''self.activesbyshares = sqlio.read_sql_query(activesbysharesSql, self.connAlch,index_col='SYMBOL')
        actives = []
        for df in self.activesbyshares:
            if type(df) == pd.DataFrame:
                actives.append(df)
        if len(actives) > 0:
            self.activesbyshares = pd.concat(actives)'''

        #Actives By Trades
        newActiveEquitiesByTrades = list(set(self.activesbytrades[(self.activesbytrades['SERVICE']=='ACTIVES_NYSE') | (self.activesbytrades['SERVICE']=='ACTIVES_NASDAQ')].index.str[:]) - set(self.actives['ACTIVES_NYSE']) - set(self.actives['ACTIVES_NASDAQ']))
        self.log.info('New Active Equities By Trades: \n' + str(newActiveEquitiesByTrades))
        newActiveOptionsByTrades = list(set(self.activesbytrades[self.activesbytrades['SERVICE']=='ACTIVES_OPTIONS'].index.str[:]) - set(self.actives['ACTIVES_OPTIONS']))
        self.log.info('New Options By Trades: \n' + str(newActiveOptionsByTrades))

        #Actives By Shares
        newActiveEquitiesByShares = list(set(self.activesbyshares[(self.activesbyshares['SERVICE']=='ACTIVES_NYSE') or (self.activesbyshares['SERVICE']=='ACTIVES_NASDAQ')].index.str[:]) - set(self.actives['ACTIVES_NYSE']) - set(self.actives['ACTIVES_NASDAQ']))
        self.log.info('New Active Equities By Shares: \n' + str(newActiveEquitiesByShares))
        newActiveOptionsByShares = list(set(self.activesbyshares[self.activesbyshares['SERVICE']=='ACTIVES_OPTIONS'].index.str[:]) - set(self.actives['ACTIVES_OPTIONS']))
        self.log.info('New Options By Shares: \n' + str(newActiveOptionsByShares))

    def getSymbolsSql(self,command=''):
        if len(command) > 0:
            try:
                self.cur.execute(command)
                res = pd.DataFrame(self.cur.fetchall(),columns=['Symbol'])
                self.conn.commit()
                return res
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.conn.rollback()
            return None
        else:
            return None

    def getPennyOptionable(self,date=''):
        '''
        Function to get Stocks with penny incremented options from Postgres databse.
        date    -> (str) Begin date of symbols in ACTIVES stream "YYYY-mm-dd"
        '''
        if len(date) == 0:
            date = lastWeekday().strftime("%Y-%m-%d")
        pennyOptionsSql = "\
            SELECT \"Symbol\", \"Market Cap\", \"Updated\" \
            FROM PUBLIC.PENNYINCREMENTOPTIONS_TDSCAN \
            WHERE DATE(\"Updated\" AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern')>='%s'" % date
        try:
            self.cur.execute(pennyOptionsSql)
            self.pennyOptionable = pd.DataFrame(self.cur.fetchall(),columns=['Symbol','Market Cap','Updated'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''self.pennyOptionable = sqlio.read_sql_query(pennyOptionsSql, self.connAlch,index_col='Symbol')
        pennyOptionable = []
        self.log.info("getPennyOptionable")
        self.log.info(type(self.pennyOptionable))
        for df in self.pennyOptionable:
            if type(df) == pd.DataFrame:
                pennyOptionable.append(df)
        if len(pennyOptionable) > 0:
            self.pennyOptionable = pd.concat(pennyOptionable)
        with self.Session() as session:
            session.commit()'''
        return
        
    def getFutures(self,date=''):
        '''
        Function to get futures from Postgres databse.
        date    -> (str) Begin date of symbols in ACTIVES stream "YYYY-mm-dd"
        '''
        if len(date) == 0:
            date = lastWeekday().strftime("%Y-%m-%d")
        futuresSql = "\
            SELECT \"Symbol\", \"Market Cap\", \"Updated\" \
            FROM PUBLIC.FUTURES_TDSCAN \
            WHERE DATE(\"Updated\" AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern')>='%s'" % date
        try:
            self.cur.execute(futuresSql)
            futures = pd.DataFrame(self.cur.fetchall(),columns=['Symbol','Market Cap','Updated'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''futures = sqlio.read_sql_query(futuresSql, self.connAlch,index_col='Symbol',)
        futuresArr = []
        self.log.info("getFutures")
        self.log.info(type(futures))
        for df in futures:
            if type(df) == pd.DataFrame:
                futuresArr.append(df)
        if len(futuresArr) > 0:
            futures = pd.concat(futuresArr)
        with self.Session() as session:
            session.commit()'''
        self.futures = futures['Symbol'].str[:-5]
        return

    def getOptionsChains(self,date='',time=False):
        '''
        Function to get options chains from Postgres databse.
        date    -> (str) Begin date of symbols in ACTIVES stream "YYYY-mm-dd" \
                    or "YYYY-mm-dd HH:MM"
        time    -> (boolean) Whether the date variable includes a time
        '''
        if len(date) == 0:
            date = lastWeekday().strftime("%Y-%m-%d")
        optionsSql = "\
            SELECT \"symbol\", \"status\", \
                option ->> 'symbol' as ChainSymbol,\
                CAST((option ->> 'totalVolume') as bigint) as totalVolume,\
                CAST((option ->> 'tradeTimeInLong') as bigint) as tradeTimeInLong,\
                CAST((option ->> 'lastTradingDay') as bigint) as lastTradingDay,\
                CAST((option ->> 'openInterest') as bigint) as openInterest\
            FROM PUBLIC.TDOPTIONSDATA \
            WHERE DATE(DATEADDED AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern')>='%s'" % date
        if time:
            if len(date) == 0:
                date = lastWeekday().strftime("%Y-%m-%d")
            optionsSql = optionsSql.replace('DATE(DATEADDED)=','DATEADDED>=')
        try:
            self.cur.execute(optionsSql)
            self.options = pd.DataFrame(self.cur.fetchall(),columns=['symbol','status','ChainSymbol','totalVolume','tradeTimeInLong','lastTradingDay','openInterest'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''self.options = sqlio.read_sql_query(optionsSql, self.connAlch,index_col='chainsymbol')
        options = []
        for df in self.options:
            if type(df) == pd.DataFrame:
                options.append(df)
        if len(options) > 0:
            self.options = pd.concat(options)
        with self.Session() as session:
            session.commit()'''
        return
    
    def getAllOptions(self,minInterest=None):
        '''
        Function to get all options contracts. Can be filtered by open interest.
        Returns a dict with 'ratio' and 'result' keys.
        minInterest     -> (int) Minimum open interest of the options contracts.
        '''
        if type(minInterest) == int and minInterest >= 0:
            options = self.options[self.options['openinterest']>minInterest]
            PctOfContractsWithOpenInterest = len(options)/len(self.options) * 100
        else:
            options = self.options
            PctOfContractsWithOpenInterest = None
        res = {
            "ratio": PctOfContractsWithOpenInterest,
            "result": options
        }
        return res

    def getListed(self,date=''):
        '''
        Get all the symbols listed on other US exchanges based on NASDAQ data. Data stored in self.listed.
        '''
        if len(date) == 0:
            date = lastWeekday().strftime("%Y-%m-%d")
        listedSql = '''
            SELECT  
                "CQS Symbol"
            FROM "otherlisted" AS listed
            JOIN PUBLIC."tdequityfrequencytable" AS equity
            ON listed."CQS Symbol"=equity."Symbol"
            WHERE "Test Issue"='N' AND "Updated" >= CURRENT_DATE - INTERVAL '3 DAYS'
        '''
        try:
            self.cur.execute(listedSql)
            listed = pd.DataFrame(self.cur.fetchall(),columns=['Symbol'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''listed = sqlio.read_sql_query(listedSql, self.connAlch,index_col='Symbol')
        listedArr = []
        self.log.info("getListed")
        self.log.info(type(listed))
        for df in listed:
            if type(df) == pd.DataFrame:
                listedArr.append(df)
        if len(listedArr) > 0:
            listed = pd.concat(listedArr)
        with self.Session() as session:
            session.commit()'''
        self.listed = listed['Symbol'].str[:].array

    def getNasdaq(self,date=''):
        '''
        Get all the symbols listed on the NASDAQ. Data stored in self.nasdaq.
        '''
        nasdaqSql = '''
            SELECT  
                listed."Symbol"
            FROM "nasdaqlisted" AS listed
            JOIN PUBLIC."tdequityfrequencytable" AS equity
            ON listed."Symbol"=equity."Symbol"
            WHERE "Test Issue"='N' AND "Updated" >= CURRENT_DATE - INTERVAL '3 DAYS'
        '''
        #nasdaq = sqlio.read_sql_query(nasdaqSql, self.connAlch,index_col='Symbol')
        try:
            self.cur.execute(nasdaqSql)
            nasdaq = pd.DataFrame(self.cur.fetchall(),columns=['Symbol'])
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()
        '''nasdaqArr = []
        self.log.info("getNasdaq")
        self.log.info(type(nasdaq))
        for df in nasdaq:
            if type(df) == pd.DataFrame:
                nasdaqArr.append(df)
        if len(nasdaqArr) > 0:
            nasdaq = pd.concat(nasdaqArr)
        with self.Session() as session:
            session.commit()'''
        self.nasdaq = nasdaq['Symbol'].str[:].array

    def getTradedLastWk(self,dropNoInterest=False):
        '''
        Get all options contracts that were traded in the last week.
        dropNoInterest      -> (boolean) Don't include options with no open \
                                interest
        '''
        NYC = tz.gettz('America / NewYork')
        today = datetime.datetime.today()
        timedate = today - datetime.timedelta(weeks=1)
        timedate = timedate.timestamp() * 1000
        if dropNoInterest:
            options = self.options[self.options['openinterest']>0]
            PctOfContractsWithOpenInterest = len(options)/len(self.options) * 100
        else:
            options = self.options
            PctOfContractsWithOpenInterest = None
        tradedLastWk = options[options['tradetimeinlong'] >= timedate]
        tradedLastWk = tradedLastWk[~tradedLastWk.index.duplicated()]
        res = {
            "ratio": PctOfContractsWithOpenInterest,
            "result": tradedLastWk
        }
        return res

    def getQuantiles(self,contracts,units,col):
        '''
        Divide the options contracts into quantiles. Returns list of lists with length units. Each list in the list contains the options in that quantile.
        contracts       -> (Dataframe) pandas dataframe of options contracts
        units           -> (int) number of quantiles groups to create e.g 5 \
                            for deciles
        col             -> (str) column to arrange quantiles
        '''
        percentile = 1/units
        w = 0
        j = 1
        quantileClusters = []
        self.quantileValues = []
        res = []
        OptionsStr = ''
        #First Quintiles
        while j <= units:
            if j == 1:
                botQuantile = contracts[col].quantile(1 - (j * percentile))
                self.quantileValues.append(botQuantile)
                self.log.info('%s: %s' % (j,str(botQuantile)))
                filteredTable = contracts[botQuantile < contracts[col]] 
                j += 1
                quantileClusters.append(filteredTable)
            elif j > 1 and j < units:
                botQuantile = contracts[col].quantile(1 - (j * percentile))
                topQuantile = contracts[col].quantile(1 - ((j - 1) * percentile))
                self.quantileValues.append(botQuantile)
                self.log.info('%s: %s' % (j,str(botQuantile)))
                filteredTable = contracts[botQuantile < contracts[col]]
                filteredTable = filteredTable[filteredTable[col] <= topQuantile]
                j += 1
                quantileClusters.append(filteredTable)
            elif j > 1 and j == units:
                topQuantile = contracts[col].quantile(1 - ((j - 1) * percentile))
                self.log.info('j == %s: %s' % (j,str(topQuantile)))
                filteredTable = contracts[contracts[col] <= topQuantile] 
                j += 1
                quantileClusters.append(filteredTable)
        for quantileCluster in quantileClusters:
            for contract in quantileCluster:
                OptionsStr += '%s,' % contract
            OptionsStr = OptionsStr[:-1]
        return quantileClusters