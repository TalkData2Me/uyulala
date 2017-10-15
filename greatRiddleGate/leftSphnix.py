#!/usr/bin/python

### Run this with     python leftSphnix.py --assets='SchwabOneSource' --horizon=3 --start='2017-01-01' --end=None

'''
leftSphnix:
* pulls asset data from the web (yahoo, google, etc.) and stores raw data to disk
* transforms data and stores results to disk

'''

##################################################################################
###########################       Imports       ##################################
##################################################################################



import uyulala
#reload(uyulala)

from multiprocessing import Pool

import pandas
import os
import sys
import getopt




##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'Test'
pullPriceHistFor = ['CHIX', 'QQQC', 'SDEM']
horizon = 3
start = '2017-01-01'
end = None

try:
    options, remainder = getopt.getopt(sys.argv[1:], 'ahse', ['assets=',
                                                             'horizon=',
                                                             'start=',
                                                             'end=',
                                                             ])
    for opt, arg in options:
        if opt in ('-a', '--assets'):
            assets = arg
            pullPriceHistFor = uyulala.assetList(assets=assets)
        elif opt in ('-h', '--horizon'):
            horizon = arg
        elif opt in ('-s', '--start'):
            start = arg
        elif opt in ('-e', '--end'):
            end = arg
except:
    print 'Error in parsing input parameters'

print 'Getting and transforming data'
print 'Assets   :', assets
print 'Horizon   :', horizon
print 'Start Date   :', start
print 'End Date   :', end




##################################################################################
###########################       Execute       ##################################
##################################################################################

folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)

try:
    [ os.remove(os.path.join(uyulala.dataDir,'raw',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'raw',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'raw',folderName))

try:
    [ os.remove(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'transformed',folderName))




def PullData(asset=''):
    try:
        rawData = uyulala.priceHist2PandasDF(symbol=asset,beginning=start,ending=end)
        try:
            firstIndex = pandas.isnull(rawData).any(1).nonzero()[0].max()+1
        except:
            firstIndex = 0
        rawData = rawData.iloc[firstIndex:,:].reset_index()  # get last row with a null and only include data after it
        rawData.to_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),index=False)
        return asset
    except:
        print 'Unable to pull data for ' + asset
        pass


def AddFeatures(asset=''):
    try:
        rawData = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),parse_dates=['DateCol'])
        features = uyulala.VROC(df=rawData,windowSize=11)
        features = uyulala.VROC(df=features,windowSize=7)
        features = uyulala.VROC(df=features,windowSize=5)
        features = uyulala.VROC(df=features,windowSize=3)
        features = uyulala.PROC(df=features, colToAvg='High',windowSize=11)
        features = uyulala.PROC(df=features, colToAvg='High',windowSize=7)
        features = uyulala.PROC(df=features, colToAvg='High',windowSize=2)
        features = uyulala.PROC(df=features, colToAvg='High',windowSize=3)
        features = uyulala.DOW(df=features,dateCol='DateCol')
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=17)
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=13)
        features = uyulala.RSI(df=features,priceCol='Close',windowSize=11)
        features = uyulala.RSIgranular(df=features,windowSize=7)
        features = uyulala.RSIgranular(df=features,windowSize=5)
        features = uyulala.RSIgranular(df=features,windowSize=3)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=11,windowSize2=19)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=5,windowSize2=11)
        features = uyulala.SMARatio(df=features,colToAvg='Close',windowSize1=3,windowSize2=7)
        features = uyulala.PctFromSMA(df=features,colToAvg='Close',windowSize=2)
        features = uyulala.PctFromSMA(df=features,colToAvg='Close',windowSize=4)
        features = uyulala.PctFromSMA(df=features,colToAvg='Close',windowSize=8)
        features = uyulala.CommodityChannelIndex(df=features, windowSize=10)
        features = uyulala.CommodityChannelIndex(df=features, windowSize=6)
        features = uyulala.CommodityChannelIndex(df=features, windowSize=3)
        features = uyulala.ForceIndex(df=features, windowSize=10)
        features = uyulala.ForceIndex(df=features, windowSize=6)
        features = uyulala.ForceIndex(df=features, windowSize=3)
        features = uyulala.EaseOfMovement(df=features, windowSize=10)
        features = uyulala.EaseOfMovement(df=features, windowSize=6)
        features = uyulala.EaseOfMovement(df=features, windowSize=3)
        features = uyulala.BollingerBands(df=features, colToAvg='Close', windowSize=10)
        features = uyulala.BollingerBands(df=features, colToAvg='Close', windowSize=6)
        features = uyulala.BollingerBands(df=features, colToAvg='Close', windowSize=3)
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
        features = uyulala.AccumulationDistributionLine(df=features,windowSize=10)
        features = uyulala.AccumulationDistributionLine(df=features,windowSize=5)
        features = uyulala.Aroon(df=features,windowSize=10)
        features = uyulala.Aroon(df=features,windowSize=5)
        features = uyulala.autocorrelation(df=features,windowSize=10,colToAvg='High',lag=1)
        features = uyulala.autocorrelation(df=features,windowSize=5,colToAvg='High',lag=1)
        features = uyulala.autocorrelation(df=features,windowSize=5,colToAvg='High',lag=2)
        features = uyulala.autocorrelation(df=features,windowSize=3,colToAvg='High',lag=1)
        features = uyulala.autocorrelation(df=features,windowSize=3,colToAvg='High',lag=2)
        features = uyulala.autocorrelation(df=features,windowSize=3,colToAvg='High',lag=3)
        features.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        features.to_csv(os.path.join(uyulala.dataDir,'transformed',folderName,asset+'.csv'),index=False)
        return asset
    except:
        print 'Unable to transform ' + asset
        pass




pool = Pool(uyulala.availableCores)

downloadedAssets = [ f.replace('.csv','') for f in os.listdir(os.path.join(uyulala.dataDir,'raw',folderName)) if f.endswith(".csv") ]
downloadedAssets = pool.map(PullData, pullPriceHistFor)

transformedAssets = pool.map(AddFeatures, downloadedAssets)

pool.close()  #close the pool and wait for the work to finish
pool.join()
