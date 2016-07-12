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

from multiprocessing import Pool
import pandas
import uyulala
#reload(uyulala)


import numpy
from sklearn import neighbors
from sklearn.linear_model import LinearRegression


##################################################################################
###########################       Execute       ##################################
##################################################################################


with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
    except ValueError:
        print 'No existing dataset with this horizon and window'


knnDict={}
for i in range(2,5,2):
    knnDict[i] = neighbors.KNeighborsRegressor(n_neighbors=i, weights='distance', n_jobs=-1).fit(existing.dropna().filter(regex=("t-.*")),existing.dropna().label)


def pullData(asset=''):
    try:
        tempDF = uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)
        tempDF = uyulala.inf2null(df=tempDF).dropna()
        for i in range(1,5,2):
            tempDF['pred-'+str(i)] = knnDict[i].predict(tempDF.filter(regex=("t-.*")))
        lm = LinearRegression(n_jobs=-1)
        lm.fit(tempDF.dropna().filter(regex=("pred-.*")),tempDF.dropna().label)
        tempDF['pred'] = lm.predict(tempDF.filter(regex=("pred-.*")))
        return tempDF[['datetime','symbol','pred']].tail(1)
    except:
        print 'unable to get data for '+asset
        pass

pool = Pool(24)

results = pool.map(pullData, evaluate)
#close the pool and wait for the work to finish
pool.close()
pool.join()


df = pandas.concat(results).reset_index(drop=True)
print df.sort(columns='pred',ascending=False).head(10)
