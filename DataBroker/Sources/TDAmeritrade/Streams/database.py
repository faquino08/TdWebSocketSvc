from shutil import register_unpack_format
from numpy import percentile
import pandas as pd
import logging
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import datetime
from dateutil import tz

class databaseHandler:
    def __init__(self,postgresParams={},debug=False):
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
        alchemyStr = f"postgresql+psycopg2://{postgresParams['user']}:{postgresParams['password']}@{postgresParams['host']}/{postgresParams['database']}"
        self.connAlch = create_engine(alchemyStr).connect()

    def getPennyOptionable(self,date=''):
        if len(date) == 0:
            date = datetime.date.today().strftime("%Y-%m-%d")
        pennyOptionsSql = "\
            SELECT \"Symbol\", \"Market Cap\", \"Updated\" \
            FROM PUBLIC.PENNYINCREMENTOPTIONS_TDSCAN \
            WHERE DATE(\"Updated\" AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern')>='%s'" % date
        self.pennyOptionable = sqlio.read_sql_query(pennyOptionsSql, self.connAlch,index_col='Symbol')
        return
        
    def getFutures(self,date='',time=False):
        if len(date) == 0:
            date = datetime.date.today().strftime("%Y-%m-%d")
        futuresSql = "\
            SELECT \"Symbol\", \"Market Cap\", \"Updated\" \
            FROM PUBLIC.FUTURES_TDSCAN \
            WHERE DATE(\"Updated\" AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern')>='%s'" % date
        futures = sqlio.read_sql_query(futuresSql, self.connAlch,index_col='Symbol')
        self.futures = futures.index.str[:-5]
        return

    def getOptionsChains(self,date='',time=False):
        if len(date) == 0:
            date = datetime.date.today().strftime("%Y-%m-%d")
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
                date = datetime.date.today().strftime("%Y-%m-%d %H:%M:%S")
            optionsSql = optionsSql.replace('DATE(DATEADDED)=','DATEADDED>=')
        self.options = sqlio.read_sql_query(optionsSql, self.connAlch,index_col='chainsymbol')
        return
    
    def getAllOptions(self,minInterest=None):
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


    def getTradedLastWk(self,dropNoInterest=False):
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