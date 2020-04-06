#!/usr/bin/python

### Run this with     python leftSphnix.py --assets='AllStocks' --horizon=3 --start='2004-01-01' --end=None

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
import numpy
import time
import math
import random
from psutil import virtual_memory
import h2o
from h2o.estimators.pca import H2OPrincipalComponentAnalysisEstimator
from glob import glob
import shutil



##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'Test'
horizon = 3
start = '2019-01-01'
end = None


try:
    argumentList=sys.argv[1:]
    unixOptions = "a:h:s:e:"
    gnuOptions = ["assets=", "horizon=", "start=", "end="]
    arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
except getopt.error as err:
    # output error, and return with an error code
    print (str(err))
    sys.exit(2)
for currentArgument, currentValue in arguments:
    if currentArgument in ("-a", "--assets"):
        assets = currentValue
    elif currentArgument in ("-h", "--horizon"):
        horizon = currentValue
    elif currentArgument in ("-s", "--start"):
        start = currentValue
    elif currentArgument in ("-e", "--end"):
        end = currentValue

pullPriceHistFor = uyulala.assetList(assets=assets)

print('Getting and transforming data')
print('Assets   :', assets)
print('Horizon   :', horizon)
print('Start Date   :', start)
print('End Date   :', end)

if assets!="Test":
    import warnings
    warnings.filterwarnings("ignore")

folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)

##################################################################################
###########################       Execute       ##################################
##################################################################################
print('Removing existing data to be replaced')

try:
    [ os.remove(os.path.join(uyulala.dataDir,'raw',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'raw',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'raw',folderName))

try:
    [ os.remove(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'transformed',folderName))

try:
    [ os.remove(os.path.join(uyulala.dataDir,'transformed_pca',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed_pca',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'transformed_pca',folderName))




def PullData(asset='',max_retries = 5):
    minRows = 5
    retries = 0
    success = False
    while retries < max_retries and not success:
        rawData = uyulala.priceHist2PandasDF(symbol=asset,beginning=start,ending=end)
        #rint(rawData.size)
        rawData = rawData.replace(0,numpy.nan)
        try:
            firstIndex = pandas.isnull(rawData).any(1).to_numpy().nonzero()[0].max()+1
        except:
            firstIndex = 0
        rawData = rawData.iloc[firstIndex:,:].reset_index()  # get last row with a null and only include data after it
        if rawData.shape[0] >= minRows:
            rawData.to_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),index=False)
            return asset
            success = True
        else:
            time.sleep(5)
            retries += 1
    if not success:
        print('Unable to pull data for ' + asset)



def AddFeatures(asset=''):
    try:
        rawData = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),parse_dates=['DateCol'])
        features = rawData.drop_duplicates(subset=['Date'], keep='last')
        features = uyulala.VROC(df=features,windowSize=11)
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
        features = features.dropna()
        features.to_csv(os.path.join(uyulala.dataDir,'transformed',folderName,asset+'.csv'),index=False)
        features = None
        return asset
    except:
        print('Unable to transform ' + asset)
        import traceback
        print(('%s: %s' % (asset, traceback.format_exc())))




def PullAndTransformData(asset):
    try:
        PullData(asset)
        AddFeatures(asset)
    except:
        import traceback
        print(('%s: %s' % (asset, traceback.format_exc())))


print('Downloading and transforming data for %s' % (pullPriceHistFor))
for i in range(0,len(pullPriceHistFor),400):
    l = pullPriceHistFor[i:i+400]
    pool = Pool(uyulala.availableCores,maxtasksperchild=1)
    pool.map(PullAndTransformData, l)
    pool.close()
    pool.join()
print('Done pulling and transforming data')
time.sleep(30)

'''
#################################################
###        PCA for Transformed data           ###
#################################################


transformedAssets = [f.split('.')[0] for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName))]
totMem = virtual_memory().total
availMem = virtual_memory().available

try:
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))
except:
    time.sleep(20)
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))

dataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)))
ratio = (availMem / (6.0000000000000)) / (dataSize)  # H2O recommends to have a cluster 2-4X the size of the data RAM>=dataSize*4 (ratio=1) --> RAM/4 = ratio*dataSize --> (RAM/4)/dataSize = ratio

if ratio < 0.98:
    k=math.ceil(len(transformedAssets)*ratio)
else:
    k=len(transformedAssets)

print('Selecting %s random assets' % k)
sampledAssets=random.choices(transformedAssets, k=min(k,len(transformedAssets)))

transSample = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s)\.csv" % ('|'.join(sampledAssets),),col_types={'DateCol':'enum','Date':'enum'}).na_omit()

colList=[col for col in list(transSample.columns) if col not in ['Date','DateCol','Symbol']]

pca = H2OPrincipalComponentAnalysisEstimator(k = math.ceil(len(colList)*.9), transform = "STANDARDIZE", pca_method="GramSVD")
pca.train(x=colList, training_frame=transSample)
numCompsToUse = next(x for x, val in enumerate(pca.varimp()[2][1:]) if val >= 0.98)+1
print('Will reduce from %s columns to %s components' % (len(colList),numCompsToUse,))

h2o.save_model(model=pca, path=os.path.join(uyulala.modelsDir,folderName), force=True)
model_id = pca.model_id

i=1
pred = pca.predict(transSample)
h2o.export_file(transSample[:, ['Date', 'Symbol', 'DateCol']].cbind(pred[:,list(range(numCompsToUse))]), path=os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run%s' % i), force = True, parts=-1)

[shutil.move(f, "{}{}run{}-{}.csv".format(os.path.join(uyulala.dataDir,'transformed_pca',folderName),os.path.sep,i,f.split(os.path.sep)[-1])) for f in glob(os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run%s' % i,'part*'))]
transformedAssets = [ele for ele in transformedAssets if ele not in list(transSample.columns)]
h2o.remove_all()

while len(transformedAssets) > 0:
    i=i+1
    pca=h2o.load_model(os.path.join(uyulala.modelsDir,folderName,model_id))
    sampledAssets = random.choices(transformedAssets, k=min(k,len(transformedAssets)))
    transSample = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s)\.csv" % ('|'.join(sampledAssets),),col_types={'DateCol':'enum','Date':'enum'}).na_omit()
    pred = pca.predict(transSample)
    h2o.export_file(transSample[:, ['Date', 'Symbol', 'DateCol']].cbind(pred[:,list(range(numCompsToUse))]), path=os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run%s' % i), force = True, parts=-1)
    [shutil.move(f, "{}{}run{}-{}.csv".format(os.path.join(uyulala.dataDir,'transformed_pca',folderName),os.path.sep,i,f.split(os.path.sep)[-1])) for f in glob(os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run%s' % i,'part*'))]
    transformedAssets = [ele for ele in transformedAssets if ele not in list(transSample.columns)]
    h2o.remove_all()

[shutil.rmtree(sub_folder) for sub_folder in glob(os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run*'))]
print('done with pca')

with open(os.path.join(uyulala.modelsDir,folderName,"pca_model_id.txt"), "w") as output:
    output.write(str(model_id))
'''
