
# coding: utf-8

#########################################################
# GENERAL FUNCTIONS
# Created by Damian von Schoenborn on November 14, 2015
# #######################################################


#########################################################
# setup
#########################################################

def importModules(modules=[]):
    #Not yet working, may not need
    import logging
    for module in modules:
        try: import module
        except: logging.critical('need module %s' % module)

def setSparkContext():
    '''Usage: sc = setSparkContext()'''
    from pyspark import SparkConf, SparkContext
    # configuration options can be found here: http://spark.apache.org/docs/latest/configuration.html
    conf = (SparkConf()
             .setMaster("local[7]")
             .setAppName("uyulala")
             .set("spark.executor.memory", "14g"))
    return SparkContext(conf = conf)

def setSparkSQL():
    '''Usage: sqlContext = setSparkSQL()'''
    from pyspark.sql import SQLContext
    return SQLContext(setSparkContext())


#########################################################
# Asset Data
#########################################################

def assetList(assets='Test'):
    #globalIndicies = ['^GSPC','^DJI','^IXIC','^NYA','^XAX','^BUK100P','^RUT','^VIX','^FTSE','^GDAXI','^FCHI','^STOXX50E','^N100','^BFX','IMOEX.ME','^N225','^HSI','000001.SS','^STI','^AXJO','^AORD','^BSESN','^JKSE','^KLSE','^NZ50','^KS11','^TWII','^GSPTSE','^BVSP','^MXX','^IPSA','^MERV','^TA125.TA','^CASE30','^JN0U.JO',^DJGSP]
    #Forex = ['BTCUSD=X','ETHUSD=X','EURUSD=X','JPY=X','GBPUSD=X','AUDUSD=X','NZDUSD=X','EURJPY=X','GBPJPY=X','EURGBP=X','EURCAD=X','EURSEK=X','EURCHF=X','EURHUF=X','EURJPY=X','CNY=X','HKD=X','SGD=X','INR=X','MXN=X','PHP=X','IDR=X','THB=X','MYR=X','ZAR=X','RUB=X']
    #Treasuries=['^IRX','^FVX','^TNX','^TYX']
    if assets == 'SchwabOneSource':
        return ['SCHB','SCHX','SCHV','SCHG','SCHA','SCHM','SCHD','SMD','SPYD','MDYV','SLYV','QUS','MDYG','SLYG','DEUS','ONEV','ONEY','SHE','RSP','XLG','ONEO','SPYX','FNDX','FNDB','SPHB','FNDA','SPLV','DGRW','RFV','RPV','RZG','RPG','RZV','RFG','DGRS','SDOG','EWSC','KRMA','JHML','QQQE','RWL','RWJ','RWK','JPSE','WMCR','DWAS','SYG','SYE','SYV','DEF','JHMM','RDIV','PDP','PKW','KNOW','JPUS','JPME','ESGL','SCHF','SCHC','SCHE','FNDF','ACIM','FEU','QCAN','LOWC','QEFA','QGBR','QEMM','QJPN','QDEU','CWI','IDLV','DBEF','HFXI','DEEF','FNDE','FNDC','WDIV','JPN','DDWM','DBAW','EELV','HDAW','DBEZ','DWX','HFXJ','HFXE','JHDG','DXGE','EDIV','GMF','PAF','IDOG','DEMG','PID','DXJS','IHDG','DNL','EUSC','GXC','EDOG','DGRE','EWX','DBEM','JHMD','CQQQ','JPIN','EWEM','EEB','PIN','PIZ','PIE','JPGE','JPEU','HGI','FRN','JPEH','JPIH','JPEM','ESGF','SCHZ','SCHP','SCHR','SCHO','TLO','ZROZ','FLRN','SHM','AGGY','CORP','AGZD','BSCQ','BSCJ','BSCH','BSCK','BSCL','BSCO','BSCN','BSCP','BSCM','BSCI','HYLB','RVNU','TFI','BWZ','PGHY','AGGE','CJNK','CWB','HYLV','BSJO','BSJJ','BSJM','BSJL','BSJK','BSJN','BSJI','BSJH','HYZD','AGGP','DSUM','BWX','PCY','PHB','HYMB','IBND','HYS','DWFI','BKLN','SRLN','SCHH','NANR','RTM','RYT','RHS','GNR','GII','RGI','EWRE','RYU','RYE','RYH','RCD','RYF','GHII','MLPX','MLPA','RWO','SGDM','RWX','PBS','CGW','ENFR','BOTZ','PSAU','CROP','GRES','JHMF','JHMT','JHMH','JHMC','JHMI','JHMA','JHME','JHMS','JHMU','ZMLP','GAL','FXY','FXS','FXF','FXE','FXC','FXB','FXA','PUTW','PSK','USDU','PGX','VRP','DYLS','INKM','RLY','WDTI','MNA','CVY','QMN','QAI','LALT','SIVR','SGOL','GLDW','PPLT','PALL','GLTR','USL','GCC','USCI','BNO','UGA','UNL','CPER']
    elif assets == 'AllStocks':
        import pandas
        nasdaq = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download').Symbol.tolist()
        amex = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download').Symbol.tolist()
        nyse = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download').Symbol.tolist()
        return list(set([item.split('.')[0].split('^')[0].strip() for item in nasdaq+amex+nyse]))
    elif assets == 'SchwabETFs':
        return ['SCHK','SCHB','SCHX','SCHD','SCHM','SCHA','SCHG','SCHV','SCHH','FNDB','FNDX','FNDA','SCHF','SCHC','SCHE','FNDF','FNDC','FNDE','SCHI','SCHJ','SCHZ','SCHO','SCHR','SCHQ','SCHP']
    else:
        return ['CHIX', 'QQQC', 'SDEM','ABCDEFG']



def priceHist2PandasDF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range in form YYYY-MM-DD
    Returns: pandas dataframe
    TODO: beginning setting isn't working
    '''
    import logging
    logging.info('running priceHist2DF function')
    try:
        import yfinance as yf
        import datetime
    except:
        logging.critical('need yfinance and datetime modules.')
        return


    if type(beginning) is str:
        beginningSplit = beginning.split('-')
        beginning = datetime.datetime(int(beginningSplit[0]),int(beginningSplit[1]),int(beginningSplit[2]))
    elif type(beginning) is datetime.datetime:
        pass
    else:
        beginning = datetime.datetime(1990,1,1)

    if type(ending) is str:
        endingSplit = ending.split('-')
        ending = datetime.datetime(int(endingSplit[0]),int(endingSplit[1]),int(endingSplit[2]))
    elif type(ending) is datetime.datetime:
        pass
    else:
        ending = datetime.datetime.now()

    try:
        tickerData = yf.Ticker(symbol)
        result = tickerData.history(period='1d', start=beginning, end=ending)
        logging.info('getting data from yahoo.')
        result = result.drop('Dividends',axis=1)
        result = result.drop('Stock Splits',axis=1)
        result['Symbol']=symbol
        result['DateCol']=result.index
        #result = result.reset_index(level=['Date'])
    except:
        logging.warning('unable to retrieve data. check symbol.')
        result = None

    return result

def priceHist2SparkDF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range in form YYYY-MM-DD
    Returns: spark dataframe
    Assumes: sqlContext is already defined
    '''
    return sqlContext.createDataFrame(priceHist2PandasDF(symbol=symbol,beginning=beginning,ending=ending))


###########################
# preprocess
###########################

def elongate(df=None):
    '''
    Expects a pandas dataframe with 'Open', 'Close', 'DateCol', and 'Symbol' fields. Unions OPEN with CLOSE
    '''
    import pandas
    import datetime

    opens = df[['DateCol','Symbol','Open']]
    opens.columns = ['DateCol','symbol','price']
    opens['datetime'] = opens.loc[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,9,30))
    opens.drop('DateCol', axis=1)

    closes = df[['DateCol','Symbol','Close']]
    closes.columns = ['DateCol','symbol','price']
    closes['datetime'] = closes.loc[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,16,0))
    closes.drop('DateCol', axis=1)

    df = pandas.concat([opens,closes])
    return df[['datetime','symbol','price']].sort_values(['symbol','datetime']).reset_index(drop=True)

def window(df=None,windowCol='price',windowSize=5,dropNulls=False):
    '''
    Expects pandas dataframe sorted appropriately. Returns a windowed df
    '''
    tempDF = df.copy()
    for i in range(windowSize):
        tempDF[windowCol+'_t-'+str(i)] = tempDF[windowCol].shift(i)
    #tempDF = tempDF.drop('price',axis=1)
    #if dropNulls:
    #    tempDF = tempDF.dropna()
    return tempDF

def rowNormalize(df=None):
    '''
    Normalizes by row across all numeric values with smallest value mapped to 1
    '''
    tempDF = df.copy()
    numericCols = tempDF.select_dtypes(include=['floating','float64']).columns
    tempDF['min'] = tempDF.min(axis=1,numeric_only=True)
    tempDF['max'] = tempDF.max(axis=1,numeric_only=True)
    for col in numericCols:
    #    tempDF[col] = (tempDF[col] - tempDF['min']) / (tempDF['max'] - tempDF['min'])
        tempDF[col] = tempDF[col] / tempDF['min']
    return tempDF.drop(['min','max'],axis=1)

def preprocess(symbol='',beginning='1990-01-01',ending=None,windowSize=10,horizon=2,setLabel=True,dropNulls=True):
    df = priceHist2PandasDF(symbol=symbol,beginning=beginning,ending=ending)
    df = elongate(df)
    norm = rowNormalize(df=window(df=df,windowSize=windowSize,dropNulls=True))
    if setLabel:
        df['label'] = (df.price.shift(-horizon) - df.price.shift(-1)) / df.price.shift(-1)
        df = df.drop('price',axis=1)
    if dropNulls:
        df = df.dropna()
    return df.merge(norm,on=['datetime','symbol'],how='inner')

def inf2null(df=None):
    import numpy
    tempDF = df.copy()
    tempDF = tempDF.replace([numpy.inf, -numpy.inf], numpy.nan)
    return tempDF

###########################
# Create Labels
###########################
'''
def highPoint(df=None,horizon=7):

    #Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'highest'
    #TODO: currently this includes the current day...need to limit to sarting the next day.

    import logging
    logging.info('running highPoint function')
    try:
        import pandas
    except:
        logging.critical('need pandas and datetime modules.')
        return
    tempDF = df.copy()
    tempDF['highest'] = tempDF['High'].shift(-1)[::-1].rolling(window=horizon,center=False).max()
    return tempDF
'''

'''
def bracket(df=None, horizon=7):
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    for h in range(1,horizon+1):
'''        

def percentChange(df=None, horizon=7, HighOrClose='High'):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'percentChange'
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    #tempDF = highPoint(tempDF, horizon=horizon)
    tempDF['highest'] = tempDF[HighOrClose].shift(-2)[::-1].rolling(window=horizon,center=False).max()

    fieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    tempDF[fieldName] = (tempDF['highest'] - tempDF['nextDayOpen']) / tempDF['nextDayOpen']
    return tempDF.drop(['highest','nextDayOpen'], 1)

def lowPercentChange(df=None, horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'percentChange'
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    #tempDF = highPoint(tempDF, horizon=horizon)
    tempDF['lowest'] = tempDF['Low'].shift(-2)[::-1].rolling(window=horizon,center=False).min()

    fieldName = 'lab_lowPercentChange_H' + str(horizon)
    tempDF[fieldName] = (tempDF['lowest'] - tempDF['nextDayOpen']) / tempDF['nextDayOpen']
    return tempDF.drop(['lowest','nextDayOpen'], 1)

def buy(df=None, horizon=7, HighOrClose='High', threshold=0.01):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'buy'
    '''
    tempDF = df.copy()
    tempDF = percentChange(tempDF, horizon=horizon, HighOrClose=HighOrClose)
    pctChangeFieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    fieldName = 'lab_buy_H' + str(horizon) + HighOrClose + '_' + str(threshold)
    tempDF[fieldName] = tempDF[pctChangeFieldName] >= threshold
    return tempDF.drop([pctChangeFieldName], 1)

def absolutePercentChange(df=None, horizon=7, HighOrClose='High'):
    tempDF = df.copy()
    tempDF = percentChange(tempDF, horizon=horizon, HighOrClose=HighOrClose)
    pctChangeFieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    fieldName = 'lab_absolutePercentChange_H' + str(horizon) + HighOrClose
    tempDF[fieldName] = tempDF[pctChangeFieldName].abs()
    return tempDF.drop([pctChangeFieldName], 1)


###########################
# Create Features
###########################


def DOW(df=None,dateCol=None):
    tempDF = df.copy()
    newColName = 'feat_DOW'
    tempDF[newColName] = tempDF[dateCol].dt.dayofweek+1
    return tempDF

def SMA(df=None,colToAvg=None,windowSize=10):
    '''
    Expects a pandas dataframe df sorted in ascending order. Returns df with additional SMA column.
    Good for average price (whether just Closed or elongated Open+Close) and average volume.
    '''
    import pandas
    tempDF = df.copy()
    #return pandas.rolling_mean(tempDF[colToAvg],window=windowSize)  # deprecated to the below
    return tempDF[colToAvg].rolling(window=windowSize,center=False).mean()

def SMARatio(df=None,colToAvg=None,windowSize1=10,windowSize2=20):
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSize1)+'/'+str(windowSize2)+'SMARatio'
    firstSMA = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize1)
    secondSMA = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize2)
    tempDF[newColName] = (firstSMA - secondSMA) / (firstSMA + secondSMA)
    #tempDF['feat_'+colToAvg+'DistFrom'+str(windowSize1)+'SMA'] = (tempDF[colToAvg] - firstSMA) / firstSMA
    return tempDF

def PctFromSMA(df=None,colToAvg=None,windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSize)+'PctFromSMA'
    SMAvg = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize)
    tempDF[newColName] = (tempDF[colToAvg] - SMAvg) / SMAvg
    return tempDF

def CommodityChannelIndex(df=None, windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'CCI'
    TypicalPrice = (tempDF['High'] + tempDF['Low'] + tempDF['Close']) / 3
    RollingMean = TypicalPrice.rolling(window=windowSize,center=False).mean()
    RollingStd = TypicalPrice.rolling(window=windowSize,center=False).std()
    tempDF[newColName] = (TypicalPrice - RollingMean) / (0.015 * RollingStd)
    return tempDF

def EaseOfMovement(df=None, windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'EMV'
    dm = ((tempDF['High'] + tempDF['Low'])/2) - ((tempDF['High'].shift(1) + tempDF['Low'].shift(1))/2)
    avgVol = tempDF['Volume'].rolling(window=windowSize,center=False).mean()
    br = (tempDF['Volume'] / avgVol) / ((tempDF['High'] - tempDF['Low'])) # typically have vol/100000000 but changed to account for differences of average volume
    tempDF[newColName] = (dm / br).rolling(window=windowSize,center=False).mean()
    return tempDF

def ForceIndex(df=None, windowSize=10):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'FI'
    priceChange = (tempDF['High'] - tempDF['Open'])/tempDF['Open']
    avgVol = tempDF['Volume'].rolling(window=windowSize,center=False).mean()
    #pandas.ewm(priceChange*(tempDF['Volume']/avgVol),span=windowSize).mean()
    s=priceChange*(tempDF['Volume']/avgVol)
    tempDF[newColName] = s.ewm(ignore_na=False,span=windowSize,min_periods=windowSize,adjust=True).mean()
    return tempDF

def BollingerBands(df=None, colToAvg=None, windowSize=10):
    tempDF = df.copy()
    MA = tempDF[colToAvg].rolling(window=windowSize,center=False).mean()
    SD = tempDF[colToAvg].rolling(window=windowSize,center=False).std()
    UpperBB = MA + (2 * SD)
    LowerBB = MA - (2 * SD)
    tempDF['feat_' + colToAvg+str(windowSize)+'PctFromUpperBB'] = (tempDF[colToAvg] - UpperBB) / UpperBB
    tempDF['feat_' + colToAvg+str(windowSize)+'PctFromLowerBB'] = (tempDF[colToAvg] - LowerBB) / LowerBB
    tempDF['feat_' + colToAvg+str(windowSize)+'BBBandwidth'] = ( (UpperBB - LowerBB) / MA) * 100
    tempDF['feat_' + colToAvg+str(windowSize)+'PctB'] = (tempDF[colToAvg] - LowerBB)/(UpperBB - LowerBB)
    return tempDF

def PROC(df=None, colToAvg=None,windowSize=10):
    '''
    takes date-sorted dataframe with Volume column. returns dataframe with VROC column
    '''
    tempDF = df.copy()
    tempDF['priorPrice'] = tempDF[colToAvg].shift(windowSize)
    tempDF['feat_PROC'+str(windowSize)] = (tempDF[colToAvg] - tempDF['priorPrice']) / tempDF['priorPrice']
    tempDF.drop('priorPrice',inplace=True, axis=1)
    return tempDF

def VROC(df=None,windowSize=10):
    '''
    takes date-sorted dataframe with Volume column. returns dataframe with VROC column
    '''
    tempDF = df.copy()
    tempDF['priorVolume'] = tempDF['Volume'].shift(windowSize)
    tempDF['feat_VROC'+str(windowSize)] = (tempDF['Volume'] - tempDF['priorVolume']) / tempDF['priorVolume']
    tempDF.drop('priorVolume',inplace=True, axis=1)
    return tempDF

def autocorrelation(df=None,windowSize=10,colToAvg=None,lag=1):
    tempDF = df.copy()
    tempDF['feat_autocorr'+str(windowSize)+colToAvg+str(lag)] = tempDF[colToAvg].rolling(window=windowSize).corr(other=tempDF[colToAvg].shift(lag))
    return tempDF

def RSI(df=None,priceCol='Close',windowSize=14):
    '''
    takes date-sorted dataframe with price column (Close or price)
    '''
    import pandas
    tempDF = df.copy()
    tempDF['rsiChange']=tempDF[priceCol] - tempDF[priceCol].shift(1)
    tempDF['rsiGain']=tempDF['rsiChange'].apply(lambda x: 1.00000000*x if x>0 else 0)
    tempDF['rsiLoss']=tempDF['rsiChange'].apply(lambda x: 1.00000000*x if x<0 else 0).abs()
    tempDF['rsiAvgGain']=tempDF['rsiGain'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['rsiAvgLoss']=tempDF['rsiLoss'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['feat_RSI'+str(windowSize)] = tempDF.apply(lambda x: 100.00000000 if (x.rsiAvgLoss<0.00000000001 or x.rsiAvgLoss>-0.00000000001) else 0.00000000 if (x.rsiAvgGain<0.00000000001 or x.rsiAvgGain>-0.00000000001) else 100.00000000-(100.00000000/(1+(x.rsiAvgGain/x.rsiAvgLoss))),axis=1)
    tempDF.drop(['rsiChange','rsiGain','rsiLoss','rsiAvgGain','rsiAvgLoss'],inplace=True,axis=1)
    return tempDF


def RSIgranular(df=None,windowSize=14):
    '''
    uses elongated data
    '''
    import pandas
    window = 2 * windowSize
    tempDF = df.copy()
    tempDF = RSI(elongate(tempDF),priceCol='price',windowSize=window)
    tempDF.rename(columns={'feat_RSI'+str(window): 'feat_RSIg'+str(windowSize),'symbol':'Symbol'}, inplace=True)
    tempDF = tempDF[tempDF['datetime'].dt.hour == 16]
    tempDF['DateCol'] = tempDF['datetime'].dt.normalize()
    tempDF.drop(['datetime','price'],inplace=True,axis=1)
    tempDF2 = df.copy()
    return tempDF2.merge(tempDF,on=['Symbol','DateCol'], how='left')

def DMI(df=None,windowSize=14):
    '''
    Assumes df has High, Low, and Close columns
    '''
    import pandas
    tempDF = df.copy()
    tempDF['upMove'] = tempDF['High'] - tempDF['High'].shift(1)
    tempDF['downMove'] = tempDF['Low'].shift(1) - tempDF['Low']

    tempDF['posDM'] = tempDF.apply(lambda x: x['upMove'] if x['upMove'] > x['downMove'] and x['upMove']>0 else 0, axis=1)
    tempDF['negDM'] = tempDF.apply(lambda x: x['downMove'] if x['downMove'] > x['upMove'] and x['downMove']>0 else 0, axis=1)

    tempDF['high-low'] = tempDF['High'] - tempDF['Low']
    tempDF['high-close'] = abs(tempDF['High'] - tempDF['Close'].shift(1))
    tempDF['low-close'] = abs(tempDF['Low'] - tempDF['Close'].shift(1))
    tempDF['TR'] = tempDF[['high-low','high-close','low-close']].max(axis=1)

    tempDF['posDI'] = (tempDF['posDM'] / tempDF['TR']).ewm(span=windowSize).mean()
    tempDF['negDI'] = (tempDF['negDM'] / tempDF['TR']).ewm(span=windowSize).mean()


    tempDF['feat_DIRatio'+ str(windowSize)] = (100 * (tempDF['posDI'] - tempDF['negDI']) / (tempDF['posDI'] + tempDF['negDI'])).ewm(span=windowSize).mean()
    tempDF['feat_ADX'+ str(windowSize)] = (100 * abs(tempDF['posDI'] - tempDF['negDI']) / (tempDF['posDI'] + tempDF['negDI'])).ewm(span=windowSize).mean()
    tempDF['feat_DMI'+ str(windowSize)] = (tempDF['feat_ADX'+ str(windowSize)] * tempDF['feat_DIRatio'+ str(windowSize)]) / 100
    tempDF['feat_AdjATR'+ str(windowSize)] = (tempDF['TR']/tempDF['High']).rolling(window=windowSize,center=False).mean()

    tempDF = tempDF.drop(['upMove','downMove','posDM','negDM','high-low','high-close','low-close','TR','posDI','negDI'],axis=1)
    return tempDF


def MACD(df=None,colToAvg=None,windowSizes=[9,12,26]):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSizes[0])+str(windowSizes[1])+str(windowSizes[2])+'MACD'
    secondEMA = df[colToAvg].ewm(span=windowSizes[1]).mean()
    thirdEMA = df[colToAvg].ewm(span=windowSizes[2]).mean()
    MACD = (secondEMA - thirdEMA)
    signal = MACD.ewm(span=windowSizes[0]).mean()
    tempDF[newColName] = (MACD - signal) / (MACD + signal)
    return tempDF

def MACDgranular(df=None,windowSizes=[9,12,26]):
    '''
    uses elongated data
    '''
    import pandas
    frst = 2 * windowSizes[0]
    scnd = 2 * windowSizes[1]
    thrd = 2 * windowSizes[2]
    tempDF = MACD(elongate(df),colToAvg='price',windowSizes=[frst,scnd,thrd])

    tempDF.rename(columns={'feat_price' +str(frst)+str(scnd)+str(thrd)+'MACD': 'feat_' +str(windowSizes[0])+str(windowSizes[1])+str(windowSizes[2])+'MACDg','symbol':'Symbol'}, inplace=True)

    tempDF = tempDF[tempDF['datetime'].dt.hour == 16]
    tempDF['DateCol'] = tempDF['datetime'].dt.normalize()
    tempDF.drop(['datetime','price'],inplace=True,axis=1)
    tempDF2 = df.copy()
    return tempDF2.merge(tempDF,on=['Symbol','DateCol'], how='left')


def StochasticOscillator(df=None,windowSize=14):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'StocOsc'
    lowestLow = tempDF['Low'].rolling(window=windowSize,center=False).min()
    highestHigh = tempDF['High'].rolling(window=windowSize,center=False).max()
    K = (tempDF['Close'] - lowestLow) / (highestHigh - lowestLow) * 100
    D = K.rolling(window=3,center=False).mean()
    tempDF[newColName] = K
    tempDF[newColName+'Ratio'] = (K-D) / D
    return tempDF


def PriceChannels(df=None,windowSize=14):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'PriceChannelDist'
    lowestLow = tempDF['Low'].rolling(window=windowSize,center=False).min()
    highestHigh = tempDF['High'].rolling(window=windowSize,center=False).max()
    centerLine = (lowestLow + highestHigh) / 2
    tempDF[newColName] = (tempDF['Close'] - centerLine) / centerLine
    return tempDF


def PSAR(df=None, iaf = 0.02, maxaf = 0.2):
    import pandas
    barsdata = df.copy()
    length = len(barsdata)
    dates = list(barsdata['DateCol'])
    high = list(barsdata['High'])
    low = list(barsdata['Low'])
    close = list(barsdata['Close'])
    psar = close[0:len(close)]
    psarbull = [None] * length
    psarbear = [None] * length
    bull = True
    af = iaf
    ep = low[0]
    hp = high[0]
    lp = low[0]
    for i in range(2,length):
        if bull:
            psar[i] = psar[i - 1] + af * (hp - psar[i - 1])
        else:
            psar[i] = psar[i - 1] + af * (lp - psar[i - 1])
        reverse = False
        if bull:
            if low[i] < psar[i]:
                bull = False
                reverse = True
                psar[i] = hp
                lp = low[i]
                af = iaf
        else:
            if high[i] > psar[i]:
                bull = True
                reverse = True
                psar[i] = lp
                hp = high[i]
                af = iaf
        if not reverse:
            if bull:
                if high[i] > hp:
                    hp = high[i]
                    af = min(af + iaf, maxaf)
                if low[i - 1] < psar[i]:
                    psar[i] = low[i - 1]
                if low[i - 2] < psar[i]:
                    psar[i] = low[i - 2]
            else:
                if low[i] < lp:
                    lp = low[i]
                    af = min(af + iaf, maxaf)
                if high[i - 1] > psar[i]:
                    psar[i] = high[i - 1]
                if high[i - 2] > psar[i]:
                    psar[i] = high[i - 2]
        if bull:
            psarbull[i] = psar[i]
        else:
            psarbear[i] = psar[i]
    dictForDF = {'close':close,"psar":psar, "psarbear":psarbear, "psarbull":psarbull}
    newDF = pandas.DataFrame(dictForDF)
    newDF['dist'] = (newDF['close'] - newDF['psar']) / newDF['psar']
    tempDF = df.copy()
    tempDF['feat_PSAR'] = newDF['dist']
    return tempDF

def AccumulationDistributionLine(df=None,windowSize=10):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'ADL'
    MoneyFlowMultiplier = ((tempDF['Close']  -  tempDF['Low']) - (tempDF['High'] - tempDF['Close'])) /(tempDF['High'] - tempDF['Low'])
    RelativeVolume = tempDF['Volume'] / tempDF['Volume'].rolling(window=windowSize,center=False).sum()
    AdjMoneyFlowVolume = MoneyFlowMultiplier * RelativeVolume
    MoneyFlowVolume = MoneyFlowMultiplier * tempDF['Volume']
    tempDF[newColName] = AdjMoneyFlowVolume.rolling(window=windowSize,center=False).sum()
    tempDF['feat_' + str(windowSize)+'CMF'] = MoneyFlowVolume.rolling(window=windowSize,center=False).sum() / tempDF['Volume'].rolling(window=windowSize,center=False).sum()
    tempDF['feat_' + str(windowSize)+'ChaikinOscillator'] = tempDF[newColName].ewm(span=max(1,int(windowSize/3))).mean() - tempDF[newColName].ewm(span=windowSize).mean()
    return tempDF

def Aroon(df=None,windowSize=10):
    import numpy
    tempDF = df.copy()
    rmlagmax = lambda xs: numpy.argmax(xs[::-1])
    DaysSinceHigh = 1.00000000000*tempDF['High'].rolling(center=False,min_periods=windowSize,window=windowSize).apply(func=rmlagmax)
    rmlagmin = lambda xs: numpy.argmin(xs[::-1])
    DaysSinceLow = 1.00000000000*tempDF['Low'].rolling(center=False,min_periods=windowSize,window=windowSize).apply(func=rmlagmin)
    tempDF['feat_' + str(windowSize)+'AroonUp'] = ((windowSize - DaysSinceHigh*1.000000000)/windowSize) * 100.00000000000
    tempDF['feat_' + str(windowSize)+'AroonDown'] = ((windowSize - DaysSinceLow*1.000000000)/windowSize) * 100.00000000000
    tempDF['feat_' + str(windowSize)+'AroonOscillator'] = tempDF['feat_' + str(windowSize)+'AroonUp'] - tempDF['feat_' + str(windowSize)+'AroonDown']
    return tempDF

###########################
# Create Models
###########################



def OLD_run_model(df, model, WhichLabel, Resultant_Col_Name, sample_rate, incl_preds=False):
    '''
    Takes pandas dataframe, model, etc.
    TODO: decide if this should even be used
    TODO: investigate 'truncate_after' usage
    TODO: clean up reference to PricesPanel, etc.
    '''
    if incl_preds==True:
        PredCols = df.filter(regex="Pred_").columns.values
    else:
        PredCols = []
    LabelCols = df.filter(regex="Label_").columns.values
    xbase = df.copy().dropna().drop(LabelCols,axis=1).drop(PredCols,axis=1).truncate(after=truncate_after)
    sample_size = int(sample_rate*len(xbase.index))
    xtrain = xbase.ix[sorted(np.random.choice(xbase.index,sample_size))]
    ytrain = df.copy()[WhichLabel].ix[sorted(xtrain.index)]
    model.fit(xtrain,ytrain)
    to_predict = df.copy().drop(LabelCols,axis=1).dropna().drop(PredCols,axis=1)
    rslt = model.predict(to_predict)
    result = pd.DataFrame(rslt,index=to_predict.index, columns=[Resultant_Col_Name])
    dummy = PricesPanel[asset].join(result,how='left')
    PricesPanel[asset][Resultant_Col_Name] = dummy[Resultant_Col_Name]









###################################
