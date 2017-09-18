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

### All stocks
#df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=80&y=10&fooldomain=caps.fool.com&MarketCap=-1&')
#pullPriceHistFor = df.Symbol.tolist()

### Schwab OneSource ETFs
pullPriceHistFor = ['SCHB','SCHX','SCHV','SCHG','SCHA','SCHM','SCHD','SMD','SPYD','MDYV','SLYV','QUS','MDYG','SLYG','DEUS','ONEV','ONEY','SHE','RSP','XLG','ONEO','SPYX','FNDX','FNDB','SPHB','FNDA','SPLV','DGRW','RFV','RPV','RZG','RPG','RZV','RFG','DGRS','SDOG','EWSC','KRMA','JHML','QQQE','RWL','RWJ','RWK','JPSE','WMCR','DWAS','SYG','SYE','SYV','DEF','JHMM','RDIV','PDP','PKW','KNOW','JPUS','JPME','ESGL','SCHF','SCHC','SCHE','FNDF','ACIM','FEU','QCAN','LOWC','QEFA','QGBR','QEMM','QJPN','QDEU','CWI','IDLV','DBEF','HFXI','DEEF','FNDE','FNDC','WDIV','JPN','DDWM','DBAW','EELV','HDAW','DBEZ','DWX','HFXJ','HFXE','JHDG','DXGE','EDIV','GMF','PAF','IDOG','DEMG','PID','DXJS','IHDG','DNL','EUSC','GXC','EDOG','DGRE','EWX','DBEM','JHMD','CQQQ','JPIN','EWEM','EEB','PIN','PIZ','PIE','JPGE','JPEU','HGI','FRN','JPEH','JPIH','JPEM','ESGF','SCHZ','SCHP','SCHR','SCHO','TLO','ZROZ','FLRN','SHM','AGGY','CORP','AGZD','BSCQ','BSCJ','BSCH','BSCK','BSCL','BSCO','BSCN','BSCP','BSCM','BSCI','HYLB','RVNU','TFI','BWZ','PGHY','AGGE','CJNK','CWB','HYLV','BSJO','BSJJ','BSJM','BSJL','BSJK','BSJN','BSJI','BSJH','HYZD','AGGP','DSUM','BWX','PCY','PHB','HYMB','IBND','HYS','DWFI','BKLN','SRLN','SCHH','NANR','RTM','RYT','RHS','GNR','GII','RGI','EWRE','RYU','RYE','RYH','RCD','RYF','GHII','MLPX','MLPA','RWO','SGDM','RWX','PBS','CGW','ENFR','BOTZ','PSAU','CROP','GRES','JHMF','JHMT','JHMH','JHMC','JHMI','JHMA','JHME','JHMS','JHMU','ZMLP','GAL','FXY','FXS','FXF','FXE','FXC','FXB','FXA','PUTW','PSK','USDU','PGX','VRP','DYLS','INKM','RLY','WDTI','MNA','CVY','QMN','QAI','LALT','SIVR','SGOL','GLDW','PPLT','PALL','GLTR','USL','GCC','USCI','BNO','UGA','UNL','CPER']



#pullPriceHistFor = ['CHIX', 'QQQC', 'SDEM', 'URA']




historyStart = '2005-01-01'
historyEnd = '2017-03-05' # typically set as None except for backtesting

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
        try:
            firstIndex = pandas.isnull(rawData).any(1).nonzero()[0].max()+1
        except:
            firstIndex = 0
        rawData = rawData.iloc[firstIndex:,:].reset_index()  # get last row with a null and only include data after it
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
        features.to_csv(os.path.join(uyulala.dataDir,'transformed',asset+'.csv'),index=False)
        return asset
    except:
        print 'Unable to transform ' + asset
        pass




pool = Pool(uyulala.availableCores)

downloadedAssets = pullPriceHistFor
downloadedAssets = pool.map(PullData, pullPriceHistFor)

transformedAssets = pool.map(AddFeatures, downloadedAssets)

pool.close()  #close the pool and wait for the work to finish
pool.join()
