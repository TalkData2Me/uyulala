
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

def priceHist2PandasDF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range in form YYYY-MM-DD
    Returns: pandas dataframe
    TODO: beginning setting isn't working
    '''
    import logging
    logging.info('running priceHist2DF function')
    try:
        import pandas
        import datetime
    except:
        logging.critical('need pandas and datetime modules.')
        return
    #if int(pandas.__version__.split('.')[1]) < 17:
    #    import pandas.io.data as data
    #else:
    #    import pandas_datareader as data

    try:
        import pandas.io.data as data
    except:
        import pandas_datareader as data

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
        result = data.DataReader(symbol,'yahoo',start=beginning,end=ending)
        logging.info('getting data from yahoo.')
        result = result.drop('Adj Close',axis=1)
        result['Symbol']=symbol
        result['DateCol']=result.index
        #result = result.reset_index(level=['Date'])
    except:
        try:
            result = data.DataReader(symbol,'google',start=beginning,end=ending)
            logging.info('getting data from google.')
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
    opens['datetime'] = opens.ix[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,9,30))
    opens.drop('DateCol', axis=1)

    closes = df[['DateCol','Symbol','Close']]
    closes.columns = ['DateCol','symbol','price']
    closes['datetime'] = closes.ix[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,16,0))
    closes.drop('DateCol', axis=1)

    df = pandas.concat([opens,closes])
    return df[['datetime','symbol','price']].sort(['symbol','datetime']).reset_index(drop=True)

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

def highPoint(df=None,horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'highest'
    TODO: currently this includes the current day...need to limit to sarting the next day.
    '''
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


def percentChange(df=None, horizon=7):
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
    tempDF = highPoint(tempDF, horizon=horizon)
    tempDF['percentChange'] = (tempDF['highest'] - tempDF['nextDayOpen']) / tempDF['nextDayOpen']
    return tempDF.drop(['highest','nextDayOpen'], 1)

def buy(df=None, horizon=3, threshold=0.01):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'buy'
    '''
    tempDF = df.copy()
    tempDF = percentChange(tempDF, horizon=horizon)
    tempDF['buy'] = tempDF.percentChange >= threshold
    return tempDF.drop(['percentChange'], 1)


###########################
# Create Features
###########################

'''  TODO: Address the following
- Directional Movement Index
- MACD
- On Balance Volume
- Parabolic SAR
- Price Channel
X Relative Strength Index (RSI)
X Volume Rate of Change (VROC)
- Stochastic Oscillator
- Bollinger Bands
- Distance above/below SMA
- SMA crosses (10&20 20&50, 50&200)
- day of week, month, etc.
- something about dividends (need to find right data source)
- sector/industry
- stock v. etf v. mutual fund
'''

def SMA(df=None,colToAvg=None,windowSize=10):
    '''
    Expects a pandas dataframe df sorted in ascending order. Returns df with additional SMA column.
    Good for average price (whether just Closed or elongated Open+Close) and average volume.
    '''
    import pandas
    tempDF = df.copy()
    newColName = colToAvg+str(windowSize)+'SMA'
    tempDF[newColName] = pandas.rolling_mean(tempDF[colToAvg],window=windowSize)
    return tempDF

def VROC(df=None,windowSize=10):
    '''
    takes date-sorted dataframe with Volume column. returns dataframe with VROC column
    '''
    tempDF = df.copy()
    tempDF['priorVolume'] = tempDF['Volume'].shift(windowSize)
    tempDF['VROC'+str(windowSize)] = (tempDF['Volume'] - tempDF['priorVolume']) / tempDF['priorVolume']
    tempDF.drop('priorVolume',inplace=True, axis=1)
    return tempDF

def RSI(df=None,priceCol='Close',windowSize=14):
    '''
    takes date-sorted dataframe with price column (Close or price)
    '''
    import pandas
    tempDF = df.copy()
    tempDF['rsiChange']=tempDF[priceCol] - tempDF[priceCol].shift(1)
    tempDF['rsiGain']=tempDF['rsiChange'].apply(lambda x: x if x>0 else 0)
    tempDF['rsiLoss']=tempDF['rsiChange'].apply(lambda x: x if x<0 else 0).abs()
    tempDF['rsiAvgGain']=tempDF['rsiGain'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['rsiAvgLoss']=tempDF['rsiLoss'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['RSI'+str(windowSize)] = tempDF.apply(lambda x: 100 if x.rsiAvgLoss==0 else 100-(100/(1+(x.rsiAvgGain/x.rsiAvgLoss))),axis=1)
    tempDF.drop(['rsiChange','rsiGain','rsiLoss','rsiAvgGain','rsiAvgLoss'],inplace=True,axis=1)
    return tempDF


def RSIgranular(df=None,windowSize=14):
    '''
    uses elongated data
    '''
    import pandas
    window = 2 * windowSize
    tempDF = RSI(elongate(df),priceCol='price',windowSize=window)
    tempDF.rename(columns={'RSI'+str(window): 'RSIg'+str(windowSize),'symbol':'Symbol'}, inplace=True)
    tempDF = tempDF[tempDF['datetime'].dt.hour == 16]
    tempDF['DateCol'] = tempDF['datetime'].apply(pandas.datetools.normalize_date)
    tempDF.drop(['datetime','price'],inplace=True,axis=1)
    tempDF2 = df.copy()
    return tempDF2.merge(tempDF,on=['Symbol','DateCol'], how='left')



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
