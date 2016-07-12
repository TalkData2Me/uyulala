#!/usr/bin/python

##################################################################################
#########################       Configure       ##################################
##################################################################################


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days

import pandas
#df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=33&y=6&min_LatestClosePrice=11.00&fooldomain=caps.fool.com&max_LatestClosePrice=50.00&MarketCap=-1&')
#evaluate = df.Symbol.tolist()
evaluate = ['GOOG', 'AAPL']

historyStart = '2016-01-01'
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


import numpy
from sklearn import neighbors
from sklearn.linear_model import LinearRegression


##################################################################################
###########################       Execute       ##################################
##################################################################################


df = pandas.DataFrame()
for asset in evaluate:
    try:
        newData = uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)
        newData = uyulala.inf2null(df=newData).dropna()
        df = pandas.concat([df,newData])
    except:
        print 'unable to get data for '+asset
        pass

df = df.reset_index(drop=True)


with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
    except ValueError:
        print 'No existing dataset with this horizon and window'


evaluated = pandas.DataFrame()
for asset in evaluate:
    tempDF = df[df.symbol==asset].copy()
    for i in range(1,20,2):
        knn = neighbors.KNeighborsRegressor(n_neighbors=i, weights='distance', n_jobs=-1)
        knn.fit(existing.dropna().filter(regex=("t-.*")),existing.dropna().label)
        tempDF['pred-'+str(i)] = knn.predict(tempDF.filter(regex=("t-.*")))
    lm = LinearRegression(n_jobs=-1)
    lm.fit(tempDF.dropna().filter(regex=("pred-.*")),tempDF.dropna().label)
    tempDF['pred'] = lm.predict(tempDF.filter(regex=("pred-.*")))
    evaluated = evaluated.append(tempDF[['datetime','symbol','pred']].tail(1))


print evaluated.sort(columns='pred',ascending=False).head(10)
