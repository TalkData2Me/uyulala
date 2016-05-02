
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
    if int(pandas.__version__.split('.')[1]) < 17:
        import pandas.io.data as data
    else:
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

def window(df=None,windowSize=5,dropNulls=True):
    '''
    Expects pandas dataframe with 'price' sorted appropriately. Returns a windowed df
    '''
    tempDF = df.copy()
    for i in range(windowSize):
        tempDF['t-'+str(i)] = tempDF.price.shift(i)
    tempDF = tempDF.drop('price',axis=1)
    if dropNulls:
        tempDF = tempDF.dropna()
    return tempDF

def rowNormalize(df=None):
    '''
    Normalizes to 0-1 by row across all numeric values
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


###########################
# Create Labels
###########################

def highPoint(df=None,horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'highest'
    '''
    import logging
    logging.info('running highPoint function')
    try:
        import pandas
        import datetime
    except:
        logging.critical('need pandas and datetime modules.')
        return
    tempDF = df.copy()
    tempDF['startDate'] = tempDF.index.to_datetime()
    tempDF['endDate'] = tempDF.index.to_datetime() + datetime.timedelta(days=horizon)
    tempDF['highest'] = tempDF.apply(lambda row: max(tempDF.ix[row['startDate']:row['endDate'],'High']), axis=1)
    return tempDF.drop(['startDate','endDate'], 1)


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
# Create Attributes
###########################

def SMAs(df=None):
    '''
    Expects a pandas dataframe in standard OHLCV format.
    '''
    import pandas
    tempDF = df.copy()
    tempDF['tenDaySMA'] = pandas.rolling_mean(tempDF['Close'],window=10)
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
