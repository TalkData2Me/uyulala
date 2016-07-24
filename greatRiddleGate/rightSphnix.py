#!/usr/bin/python

##################################################################################
#########################       Configure       ##################################
##################################################################################

daysOfHistoryForModelling = 180


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days



##################################################################################
###########################       Imports       ##################################
##################################################################################

from multiprocessing import Pool
import pandas
import uyulala
#reload(uyulala)

import datetime
import numpy
from sklearn import neighbors
from sklearn.linear_model import LinearRegression


##################################################################################
###########################       Setup Calcs       ##################################
##################################################################################

df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=33&y=6&min_LatestClosePrice=11.00&fooldomain=caps.fool.com&max_LatestClosePrice=50.00&MarketCap=-1&')
evaluate = df.Symbol.tolist()
#evaluate = ['GOOG', 'AAPL']


d = datetime.date.today() - datetime.timedelta(days=daysOfHistoryForModelling)
#historyStart = '2016-01-01'
historyStart = str(d)
historyEnd = None

horizon = horizonDays*2
window = windowDays*2
datasetName = 'Hrzn'+str(horizon)+'Wndw'+str(window)



##################################################################################
###########################       Execute       ##################################
##################################################################################


with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
    except ValueError:
        print 'No existing dataset with this horizon and window'
existing = existing[existing['datetime'].dt.hour == 16] #only look at close data
existing = existing[(existing.datetime < pandas.Timestamp(historyStart)) | (~existing.symbol.isin(evaluate))] #remove data from training set that will be used for testing
existing = existing[existing['label'] < 0.5]

knnDict={}
for i in [10,50,200,1000]:
    knnDict[i] = neighbors.KNeighborsRegressor(n_neighbors=i, weights='distance', n_jobs=-1).fit(existing.dropna().filter(regex=("t-.*")),existing.dropna().label)


def pullData(asset=''):
    print 'running data for '+asset
    try:
        tempDF = uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)
        tempDF = tempDF[tempDF['datetime'].dt.hour == 16] #only look at close data
        tempDF = tempDF[(tempDF['label'] < 0.5) | (tempDF['label'].isnull())]
        tempDF = uyulala.inf2null(df=tempDF)
        for i in [10,50,200,1000]:
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
df.sort(columns='pred',ascending=False).to_excel('uyulala_GRG_results.xlsx')
