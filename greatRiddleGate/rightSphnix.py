#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''

##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'SchwabETFs'   # Typically AllStocks, SchwabOneSource, SchwabETFs, or Test
horizon = 2       # prediction horizon in days

totalBuildTimeAllowed_seconds = 173000


startDate = '2000-01-01'


##################################################################################
###########################       Imports       ##################################
##################################################################################

from multiprocessing import Pool
import pandas
import os
import uyulala
#reload(uyulala)

import datetime
import numpy
import random
import string
import subprocess
import time
from psutil import virtual_memory
import shutil
from pathlib import Path
import math
import glob

totMem = virtual_memory().total
availMem = virtual_memory().available

folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)

##################################################################################
#################              Clear directories          ########################
##################################################################################
try:
    [ os.remove(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'labeled',folderName))

try:
    [ os.remove(os.path.join(uyulala.modelsDir,folderName,f)) for f in os.listdir(os.path.join(uyulala.modelsDir,folderName)) if f!='pca_model_id.txt'  ]
except:
    os.makedirs(os.path.join(uyulala.modelsDir,folderName))


##################################################################################
################# Get and transform data (run leftSphnix) ########################
##################################################################################

if assets!="Test":
    import warnings
    warnings.filterwarnings("ignore")


filePath = os.path.join(uyulala.uyulalaDir,'greatRiddleGate','leftSphnix.py')
print('making call: '+'''python %s --assets=%s --horizon=%i --start=%s''' % (filePath,assets,horizon,startDate))
subprocess.call('''python %s --assets=%s --horizon=%i --start=%s''' % (filePath,assets,horizon,startDate), shell=True)


##################################################################################
###########################       Execute       ##################################
##################################################################################


evaluate = [ f.replace('.csv','') for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if f.endswith(".csv") ]





def createLabels(asset=''):
    try:
        labeled = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),parse_dates=['DateCol']).set_index('DateCol',drop=False)
        labeled = labeled.drop_duplicates(subset=['Date'], keep='last')
        ### Supplemental labels
        #labeled = uyulala.percentChange(df=labeled,horizon=int(horizon/2),HighOrClose='High')
        #labeled = uyulala.percentChange(df=labeled,horizon=2*horizon,HighOrClose='High')
        ##labeled = uyulala.lowPercentChange(df=labeled,horizon=int(horizon/2))
        labeled = uyulala.lowPercentChange(df=labeled,horizon=horizon)
        ##labeled = uyulala.absolutePercentChange(df=labeled, horizon=2*horizon, HighOrClose='High')
        ##labeled = uyulala.absolutePercentChange(df=labeled, horizon=horizon, HighOrClose='High')
        #labeled = uyulala.buy(df=labeled,horizon=horizon,HighOrClose='High',threshold=0.00001)
        #########################################################################
        # THE BELOW MUST REMAIN IN CORRECT ORDER SINCE CALLED BELOW BY POSITION #
        #########################################################################
        # Key Classification Field (is it a good buy?)
        labeled = uyulala.buy(df=labeled,horizon=horizon,HighOrClose='High',threshold=0.01)
        # Key Regression Field (what's the predicted return?)
        labeled = uyulala.percentChange(df=labeled,horizon=horizon,HighOrClose='High')
        # Clean-up
        labeled.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        labeled.to_csv(os.path.join(uyulala.dataDir,'labeled',folderName,asset+'.csv'),index=False)
        return asset
    except:
        print('unable to create label for '+asset)
        pass


print('labelling data')
for i in range(0,len(evaluate),500):
    l = evaluate[i:i+500]
    pool = Pool(uyulala.availableCores,maxtasksperchild=1)
    pool.map(createLabels, l)
    pool.close()
    pool.join()

print('Done labelling data')



import h2o
from h2o.automl import H2OAutoML
try:
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1500000000),min_mem_size="%sG" % int(availMem/1500000000))
except:
    time.sleep(20)
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1500000000),min_mem_size="%sG" % int(availMem/1500000000))




print('importing data')

dataSize = sum(f.stat().st_size for f in Path(os.path.join(uyulala.dataDir,'transformed',folderName)).glob('**/*') if f.is_file() ) + sum(os.path.getsize(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)))
ratio = ((availMem/2000000000) / (5.0000000000000)) / (dataSize/1000000000)
print('full data size: {}gb'.format(dataSize/1000000000.00))

transformed_pca_files = [file for file in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if file not in ['.DS_Store']]
#transformed_pca_files = [file for file in os.listdir(os.path.join(uyulala.dataDir,'transformed_pca',folderName)) if file not in ['.DS_Store']]

if ratio < 0.98:
    print('reducing file size by {}%'.format(100*(1-ratio)))
    k=math.ceil(len(transformed_pca_files)*ratio)
    sampledFiles=random.choices(transformed_pca_files, k=min(k,len(transformed_pca_files)))
else:
    sampledFiles=transformed_pca_files

print('Files to use: {}'.format(sampledFiles))

fullDF = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s)" % ('|'.join(sampledFiles),),col_types={'DateCol':'enum','Date':'enum'}).na_omit().merge(h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName),pattern = ".*\.csv",col_types={'DateCol':'enum','Date':'enum'}).na_omit()).na_omit()
#fullDF = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed_pca',folderName),pattern = "(%s)" % ('|'.join(sampledFiles),),col_types={'DateCol':'enum','Date':'enum'}).na_omit().merge(h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName),pattern = ".*\.csv",col_types={'DateCol':'enum','Date':'enum'}).na_omit()).na_omit()

print('Final data size is %s' % (fullDF.shape,))
df,blending = fullDF.split_frame(ratios=[.9])
print('Training data size: %s' % (df.shape,))
print('Blending data size: %s' % (blending.shape,))
print(df.head(2))
print(blending.head(2))
features = [s for s in fullDF.columns if "feat_" in s]
#features = [s for s in fullDF.columns if "PC" in s]
labels = [s for s in fullDF.columns if "lab_" in s]





print('building models')
timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)+2*len(labels)+2*5))
print('Time per run: ' + str(timePerRun) + ' seconds')

print('running the first layer of models')
L0Results = fullDF[['Symbol','DateCol']]
executionOrder = []
for label in labels:
    print('first run of '+label)
    if df.types[label] in ('enum','str','bool'):
        aml = H2OAutoML(project_name=label+'0',
        #                stopping_tolerance=0.1,
                        max_runtime_secs = timePerRun,sort_metric = "logloss",stopping_metric="logloss")

    else:
        aml = H2OAutoML(project_name=label+'0',
        #                stopping_tolerance=0.1,
                        max_runtime_secs = timePerRun,sort_metric = "RMSLE",stopping_metric="RMSLE")
    aml.train(x=features,y=label,training_frame=fullDF)
    print(aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0])
    print(aml.leaderboard[0,:])
    executionOrder = executionOrder + [aml._leader_id]

    print('variable importance:')
    print(aml.leader.varimp(use_pandas=True))

    preds = aml.leader.predict(fullDF)
    print('preds shape is {}'.format(preds.shape))
    preds.head()
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L0Results = L0Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

print('running the second layer of models')
L1Results = fullDF[['Symbol','DateCol']]
for label in labels:
    print('second run of '+label)
    if df.types[label] in ('enum','str','bool'):
        aml = H2OAutoML(project_name=label+'1',
        #                stopping_tolerance=0.1,
                        max_runtime_secs = 2*timePerRun,sort_metric = "mean_per_class_error",stopping_metric="mean_per_class_error")

    else:
        aml = H2OAutoML(project_name=label+'1',
        #                stopping_tolerance=0.1,
                        max_runtime_secs = 2*timePerRun,sort_metric = "MSE",stopping_metric="MSE")
    aml.train(x=features,y=label,training_frame=df.merge(L0Results))
    print(aml.leaderboard.as_data_frame())
    print(aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0])
    print(aml.leaderboard[0,:])
    executionOrder = executionOrder + [aml._leader_id]

    print('variable importance:')
    print(aml.leader.varimp(use_pandas=True))

    preds = aml.leader.predict(fullDF.merge(L0Results))
    print('preds shape is {}'.format(preds.shape))
    preds.head()
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L1Results = L1Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

print('running the final Buy Signal model')
BuyResults = fullDF[['Symbol','DateCol']]
for label in [labels[-2]]:
    print('final run of '+label)
    aml = H2OAutoML(project_name=label+'_final',
    #                stopping_tolerance=0.001,
                    max_runtime_secs = 5*timePerRun,sort_metric = "AUCPR",stopping_metric="AUCPR")
    aml.train(x=features+[x for x in L1Results.columns if (x != 'Symbol') & (x != 'DateCol')],
              y=label,
              training_frame=df.merge(L1Results),
              leaderboard_frame=blending.merge(L1Results),
              blending_frame=blending.merge(L1Results))
    print(aml.leaderboard.as_data_frame())
    print(aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0])
    print(aml.leaderboard[0,:])
    executionOrder = executionOrder + [aml._leader_id]

    print('variable importance:')
    print(aml.leader.varimp(use_pandas=True))

    preds = aml.leader.predict(fullDF.merge(L1Results))
    print('preds shape is {}'.format(preds.shape))
    preds.head()
    preds = preds.drop(['predict','False']).set_names([aml._leader_id + '_True'])
    BuyResults = BuyResults.cbind(preds)
    #BuyResults = BuyResults[BuyResults[aml._leader_id + '_True']>0.7]

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml


fullDF = fullDF.merge(BuyResults)


PredictedPerformance = fullDF[['Symbol','DateCol']]
for label in [labels[-1]]:
    print('final run of '+label)
    aml = H2OAutoML(project_name=label+'_final',
    #                stopping_tolerance=0.001,
                    max_runtime_secs = 5*timePerRun,sort_metric = "MAE",stopping_metric="MAE")
    aml.train(x=features+[x for x in L1Results.columns if (x != 'Symbol') & (x != 'DateCol')],
              y=label,
              training_frame=df.merge(L1Results),
              leaderboard_frame=blending.merge(L1Results),
              blending_frame=blending.merge(L1Results))
    print(aml.leaderboard.as_data_frame())
    print(aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0])
    print(aml.leaderboard[0,:])
    executionOrder = executionOrder + [aml._leader_id]

    print('variable importance:')
    print(aml.leader.varimp(use_pandas=True))

    preds = aml.leader.predict(fullDF.merge(L1Results))
    print('preds shape is {}'.format(preds.shape))
    preds.head()
    PredictedPerformance = PredictedPerformance.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

with open(os.path.join(uyulala.modelsDir,folderName,"executionOrder.txt"), "w") as output:
    output.write(str(executionOrder))
fullDF = fullDF.merge(PredictedPerformance)
print(fullDF.head(2))
h2o.export_file(fullDF, path=os.path.join(uyulala.dataDir,'model_data',folderName), force = True, parts=-1)

##################################################################################
#########################      Backtesting      ##################################
##################################################################################

#import matplotlib.pyplot as plt

raw = h2o.import_file(path=os.path.join(uyulala.dataDir,'raw',folderName),pattern = ".*\.csv",col_types={'DateCol':'enum','Date':'enum'})
labelCol = labels[-1]
pandasDF = fullDF.merge(raw)[[labelCol,'predict','Open','Date']].as_data_frame()
pandasDF['returnIfWrong'] = (pandasDF.Open.shift(-4) - pandasDF.Open.shift(-1)) / pandasDF.Open.shift(-1)
maxEV = 0
thresholdToUse = 0
for threshold in numpy.arange(0.005,0.051,0.001):
    pandasDF['invested'] = pandasDF.apply(lambda row: 1 if row.predict > threshold else 0,axis=1)
    pandasDF['return'] = pandasDF.apply(lambda row: threshold if ((row.predict >= threshold) & (row[labelCol]>threshold)) else (row.returnIfWrong if ((row.predict >= threshold) & (row[labelCol]<=threshold)) else 0),axis=1)
    pandasDF = pandasDF.dropna()
    dailyDF = pandas.DataFrame(pandasDF.groupby(['Date'])[['return','invested']].sum())
    dailyDF['avgReturn'] = (dailyDF['return']/dailyDF['invested']).fillna(value=0)
    threshEV = dailyDF['avgReturn'].mean()
    if threshEV > maxEV:
        maxEV = threshEV
        thresholdToUse = threshold
        x=dailyDF['avgReturn'].values
print('Using a threshold of %f has an expected return of %f' %(thresholdToUse,maxEV))
#n, bins, patches = plt.hist(x, bins=100, facecolor='g', alpha=0.75, range=(-0.05,0.05))

with open(os.path.join(uyulala.modelsDir,folderName,"threshold.txt"), "w") as output:
    output.write(str(thresholdToUse))

h2o.cluster().shutdown()
