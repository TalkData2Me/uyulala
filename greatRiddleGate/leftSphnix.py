#!/usr/bin/python

##################################################################################
#########################       Configure       ##################################
##################################################################################


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days

pullPriceHistFor = ['CHIX', 'QQQC', 'SDEM', 'URA', 'YAO', 'FXI', 'DVYE', 'BKF', 'EEM', 'EPOL', 'EWS', 'IPO', 'SCHE', 'BIK', 'EDIV', 'VWO', 'DEM']

historyStart = '2005-01-01'
historyEnd = None

#-------------------------------------------------------------------------------#
horizon = horizonDays*2
window = windowDays*2
datasetName = 'Hrzn'+str(horizon)+'Wndw'+str(window)



##################################################################################
###########################       Imports       ##################################
##################################################################################



import uyulala
#reload(uyulala)



import pandas


##################################################################################
###########################       Execute       ##################################
##################################################################################



df = uyulala.preprocess(symbol=pullPriceHistFor[0],beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon)

for asset in pullPriceHistFor[1:]:
    df = pandas.concat([df,uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon)])



with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
        df = pandas.concat([existing,df]).drop_duplicates(subset=['datetime','symbol'])
    except KeyError:
        print 'No existing dataset with this horizon and window'



with pandas.HDFStore('data.h5') as store:
    store[datasetName] = df.reset_index(drop=True)
