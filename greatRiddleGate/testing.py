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


def pullData(asset=''):
    try:
        newData = uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon,setLabel=True,dropNulls=False)
        newData = uyulala.inf2null(df=newData).dropna()
        return newData
    except:
        print 'unable to get data for '+asset
        pass

pool = Pool(24)

results = pool.map(pullData, evaluate)
#close the pool and wait for the work to finish
pool.close()
pool.join()


df = pandas.concat(results)
df = df.reset_index(drop=True)
print df.head()
