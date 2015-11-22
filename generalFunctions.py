
# coding: utf-8

#########################################################
# GENERAL FUNCTIONS
# Created by Damian von Schoenborn on November 14, 2015
# #######################################################


#########################################################
# Asset Data
#########################################################


def priceHist2DF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range
    Returns: pandas dataframe
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

    if type(beginning)=='str':
        beginningSplit = beginning.split('-')
        beginning = datetime.datetime(int(beginningSplit[0]),int(beginningSplit[1]),int(beginningSplit[2]))
    elif type(beginning)=='datetime.datetime':
        pass
    else:
        beginning = datetime.datetime(1990,1,1)

    if type(ending)=='str':
        bendingSplit = beginning.split('-')
        ending = datetime.datetime(int(endingSplit[0]),int(endingSplit[1]),int(endingSplit[2]))
    elif type(ending)=='datetime.datetime':
        pass
    else:
        ending = datetime.datetime.now()

    try:
        result = data.DataReader(symbol,'yahoo',start=beginning,end=ending)
        logging.info('getting data from yahoo.')
    except:
        try:
            result = data.DataReader(symbol,'google',start=beginning,end=ending)
            logging.info('getting data from google.')
        except:
            logging.warning('unable to retrieve data. check symbol.')
            result = None

    return result

###########################
# Create Labels
###########################

def highPoint(df=None,horizon=7):
    '''
    Expects dataframe in standard OHLCV format. Returns dataframe with new column 'highest'
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
    Creates new column in given DF. Assumes DF is standard OHLC format.
    '''
    df['percentChange'] = df




if __name__==__main__:
