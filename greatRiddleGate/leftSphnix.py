#!/usr/bin/python


'''
leftSphnix:
* pulls asset data from the web (yahoo, google, etc.) and stores raw data to disk
* transforms data and stores results to disk

TODO: need to be able to pass in parameter (asset list)
'''




##################################################################################
#########################       Configure       ##################################
##################################################################################


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days

import pandas

df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=80&y=10&fooldomain=caps.fool.com&MarketCap=-1&')
pullPriceHistFor = df.Symbol.tolist()

pullPriceHistFor = ['CHIX', 'QQQC', 'SDEM', 'URA']




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
import os


##################################################################################
###########################       Execute       ##################################
##################################################################################


def PullData(asset=''):
    try:
        rawData = uyulala.priceHist2PandasDF(symbol=asset,beginning=historyStart,ending=historyEnd)
        rawData.to_csv(os.path.join(uyulala.dataDir,'raw',asset+'.csv'),index=False)
        return asset
    except:
        print 'Unable to pull data for ' + asset
        pass


def AddFeatures(asset=''):
    try:
        rawData = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',asset+'.csv'),parse_dates=['DateCol'])
        features = uyulala.VROC(df=rawData,windowSize=11)
        features = uyulala.VROC(df=features,windowSize=7)
        features = uyulala.VROC(df=features,windowSize=5)
        features = uyulala.VROC(df=features,windowSize=3)
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=17)
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=13)
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=11)
        features = uyulala.RSIgranular(df=features,windowSize=7)
        features = uyulala.RSIgranular(df=features,windowSize=5)
        features = uyulala.RSIgranular(df=features,windowSize=3)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=11,windowSize2=19)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=5,windowSize2=11)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=3,windowSize2=7)
        features = uyulala.DMI(df=features,windowSize=17)
        features = uyulala.DMI(df=features,windowSize=11)
        features = uyulala.DMI(df=features,windowSize=7)
        features = uyulala.DMI(df=features,windowSize=3)
        features = uyulala.MACD(df=features,colToAvg='Close',windowSizes=[9,12,26])
        features = uyulala.MACD(df=features,colToAvg='Close',windowSizes=[7,11,23])
        features = uyulala.MACDgranular(df=features,windowSizes=[2,3,5])
        features = uyulala.MACDgranular(df=features,windowSizes=[3,5,7])
        features = uyulala.MACDgranular(df=features,windowSizes=[5,7,11])
        features = uyulala.StochasticOscillator(df=features,windowSize=11)
        features = uyulala.StochasticOscillator(df=features,windowSize=7)
        features = uyulala.StochasticOscillator(df=features,windowSize=5)
        features = uyulala.StochasticOscillator(df=features,windowSize=3)
        features = uyulala.PriceChannels(df=features,windowSize=11)
        features = uyulala.PriceChannels(df=features,windowSize=7)
        features = uyulala.PriceChannels(df=features,windowSize=5)
        features = uyulala.PriceChannels(df=features,windowSize=3)
        features = uyulala.PSAR(df=features)

        features.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        features.to_csv(os.path.join(uyulala.dataDir,'transformed',asset+'.csv'),index=False)
        return asset
    except:
        print 'Unable to transform ' + asset
        pass




pool = Pool(uyulala.availableCores)

downloadedAssets = pullPriceHistFor
#downloadedAssets = pool.map(PullData, pullPriceHistFor)

transformedAssets = pool.map(AddFeatures, downloadedAssets)

pool.close()  #close the pool and wait for the work to finish
pool.join()

'''
df = pandas.concat(results).reset_index(drop=True)



with pandas.HDFStore('data.h5') as store:
    try:
        existing = store[datasetName]
        df = pandas.concat([existing,df]).drop_duplicates(subset=['datetime','symbol'])
    except KeyError:
        print 'No existing dataset with this horizon and window'



with pandas.HDFStore('data.h5') as store:
    store[datasetName] = uyulala.inf2null(df=df).dropna().reset_index(drop=True)


'''
