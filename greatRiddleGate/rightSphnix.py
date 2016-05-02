#!/usr/bin/python

##################################################################################
#########################       Configure       ##################################
##################################################################################


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days

evaluate = ['CHIX', 'QQQC', 'SDEM', 'URA', 'YAO', 'FXI', 'DVYE', 'BKF', 'EEM', 'EPOL', 'EWS', 'IPO', 'SCHE', 'BIK', 'EDIV', 'VWO', 'DEM']

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


import pandas
import numpy
from sklearn import neighbors
from sklearn.linear_model import LinearRegression


##################################################################################
###########################       Execute       ##################################
##################################################################################

df = uyulala.preprocess(symbol=evaluate[0],beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)

for asset in evaluate[1:]:
    df = pandas.concat([df,uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)])

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
        knn = neighbors.KNeighborsRegressor(n_neighbors=i, weights='distance')
        knn.fit(existing.dropna().filter(regex=("t-.*")),existing.dropna().label)
        tempDF['pred-'+str(i)] = knn.predict(tempDF.filter(regex=("t-.*")))
    lm = LinearRegression(n_jobs=-1)
    lm.fit(tempDF.dropna().filter(regex=("pred-.*")),tempDF.dropna().label)
    tempDF['pred'] = lm.predict(tempDF.filter(regex=("pred-.*")))
    evaluated = evaluated.append(tempDF[['datetime','symbol','pred']].tail(1))


print evaluated.sort(columns='pred',ascending=False)
