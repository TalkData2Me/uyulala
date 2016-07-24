#!/usr/bin/python

##################################################################################
#########################       Configure       ##################################
##################################################################################


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days

import pandas
df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=80&y=10&fooldomain=caps.fool.com&MarketCap=-1&')
pullPriceHistFor = df.Symbol.tolist()




#pullPriceHistFor = ['CHIX', 'QQQC', 'SDEM', 'URA']

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

from multiprocessing import Pool

import pandas


##################################################################################
###########################       Execute       ##################################
##################################################################################




def pullData(asset=''):
    try:
        return uyulala.preprocess(symbol=asset,beginning=historyStart,ending=historyEnd,windowSize=window,horizon=horizon)
    except:
        pass

pool = Pool(24)

results = pool.map(pullData, pullPriceHistFor)
#close the pool and wait for the work to finish
pool.close()
pool.join()


df = pandas.concat(results).reset_index(drop=True)



with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
        df = pandas.concat([existing,df]).drop_duplicates(subset=['datetime','symbol'])
    except KeyError:
        print 'No existing dataset with this horizon and window'



with pandas.HDFStore('data.h5') as store:
    store[datasetName] = uyulala.inf2null(df=df).dropna().reset_index(drop=True)
