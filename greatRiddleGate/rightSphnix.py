#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''

##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'AllStocks'   # Typically AllStocks, SchwabOneSource, or Test
horizon = 3       # prediction horizon in days

totalBuildTimeAllowed_seconds = 1800


startDate = '2014-01-01'


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



##################################################################################
################# Get and transform data (run leftSphnix) ########################
##################################################################################

if assets!="Test":
    import warnings
    warnings.filterwarnings("ignore")


filePath = os.path.join(uyulala.uyulalaDir,'greatRiddleGate','leftSphnix.py')
subprocess.call('''python %s --assets=%s --horizon=%i --start=%s''' % (filePath,assets,horizon,startDate), shell=True)



##################################################################################
###########################       Execute       ##################################
##################################################################################


folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)

try:
    [ os.remove(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'labeled',folderName))

try:
    [ os.remove(os.path.join(uyulala.modelsDir,folderName,f)) for f in os.listdir(os.path.join(uyulala.modelsDir,folderName)) ]
except:
    os.makedirs(os.path.join(uyulala.modelsDir,folderName))


evaluate = [ f.replace('.csv','') for f in os.listdir(os.path.join(uyulala.dataDir,'raw',folderName)) if f.endswith(".csv") ]





def createLabels(asset=''):
    try:
        labeled = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),parse_dates=['DateCol']).set_index('DateCol',drop=False)
        labeled = labeled.drop_duplicates(subset=['Date'], keep='last')
        # Supplemental labels
        #labeled = uyulala.buy(df=labeled,horizon=1,HighOrClose='High',threshold=0.01)
        labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='High')
        labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='Close')
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.01)
        labeled = uyulala.percentChange(df=labeled,horizon=2,HighOrClose='High')
        labeled = uyulala.percentChange(df=labeled,horizon=2,HighOrClose='Close')
        labeled = uyulala.lowPercentChange(df=labeled,horizon=2)
        labeled = uyulala.lowPercentChange(df=labeled,horizon=3)
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.02)
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.03)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.02)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.03)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.04)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.05)
        labeled = uyulala.absolutePercentChange(df=labeled, horizon=3, HighOrClose='High')
        labeled = uyulala.absolutePercentChange(df=labeled, horizon=2, HighOrClose='High')
        # THE BELOW MUST REMAIN IN CORRECT ORDER SINCE CALLED BELOW BY POSITION
        # Key Classification Field (is it a good buy?)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.01)
        # Key Regression Field (what's the predicted return?)
        labeled = uyulala.percentChange(df=labeled,horizon=3,HighOrClose='High')
        # Clean-up
        labeled.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        labeled.to_csv(os.path.join(uyulala.dataDir,'labeled',folderName,asset+'.csv'),index=False)
        return asset
    except:
        print 'unable to create label for '+asset
        pass


print 'labelling data'
for i in range(0,len(evaluate),500):
    l = evaluate[i:i+500]
    pool = Pool(uyulala.availableCores,maxtasksperchild=1)
    pool.map(createLabels, l)
    pool.close()
    pool.join()

print 'Done labelling data'







import h2o
from h2o.automl import H2OAutoML
try:
    h2o.init(max_mem_size="16G",min_mem_size="14G")
except:
    time.sleep(20)
    h2o.init(max_mem_size="16G",min_mem_size="14G")

print 'importing data'
transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName))
labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName))
df = labeled.merge(transformed)
df = df.na_omit()
print 'Data size is %s' % (df.shape,)

features = [s for s in transformed.columns if "feat_" in s]
labels = [s for s in labeled.columns if "lab_" in s]


transformed = None
labeled = None

dataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName))) + sum(os.path.getsize(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)))

ratio = (14 * 1000000000 / 8.0000000000000) / dataSize  # H2O recommends to have a cluster 4X the size of the data

if ratio < 0.98:
    df,drop = df.split_frame(ratios=[ratio])
    drop=None
    print 'Decreased data size to %s for better performance' % (df.shape,)





timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)+2*len(labels)+2*5))
print 'Time per run: ' + str(timePerRun) + ' seconds'

print 'running the first layer of models'
L0Results = df[['Symbol','DateCol']]
executionOrder = []
for label in labels:
    print 'first run of '+label
    # project_name=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(99))
    aml = H2OAutoML(project_name=label+'0',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = timePerRun)
    aml.train(x=features,y=label,training_frame=df)
    print aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0]
    print aml.leaderboard[0,:]
    executionOrder = executionOrder + [aml._leader_id]

    preds = aml.leader.predict(df)
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L0Results = L0Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

print 'running the second layer of models'
L1Results = df[['Symbol','DateCol']]
for label in labels:
    print 'second run of '+label
    aml = H2OAutoML(project_name=label+'1',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = 2*timePerRun)
    aml.train(x=features+[x for x in L0Results.columns if (x != 'Symbol') & (x != 'DateCol')],
              y=label,
              training_frame=df.merge(L0Results))
    print aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0]
    print aml.leaderboard[0,:]
    executionOrder = executionOrder + [aml._leader_id]

    preds = aml.leader.predict(df.merge(L0Results))
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L1Results = L1Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

print 'running the final Buy Signal model'
BuyResults = df[['Symbol','DateCol']]
for label in [labels[-2]]:
    print 'final run of '+label
    aml = H2OAutoML(project_name=label+'_final',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = 5*timePerRun)
    aml.train(x=features+[x for x in L1Results.columns if (x != 'Symbol') & (x != 'DateCol')],
              y=label,
              training_frame=df.merge(L1Results))
    print aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0]
    print aml.leaderboard[0,:]
    executionOrder = executionOrder + [aml._leader_id]

    preds = aml.leader.predict(df.merge(L1Results))
    preds = preds.drop(['predict','False']).set_names([aml._leader_id + '_True'])
    BuyResults = BuyResults.cbind(preds)
    BuyResults = BuyResults[BuyResults[aml._leader_id + '_True']>0.7]

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml


df = BuyResults.merge(df)


PredictedPerformance = df[['Symbol','DateCol']]
for label in [labels[-1]]:
    print 'final run of '+label
    aml = H2OAutoML(project_name=label+'_final',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = 5*timePerRun)
    aml.train(x=features+[x for x in L1Results.columns if (x != 'Symbol') & (x != 'DateCol')],
              y=label,
              training_frame=df.merge(L1Results))
    print aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0]
    print aml.leaderboard[0,:]
    executionOrder = executionOrder + [aml._leader_id]

    preds = aml.leader.predict(df.merge(L1Results))
    PredictedPerformance = PredictedPerformance.cbind(preds)

    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    aml = None
    del aml

df = PredictedPerformance.merge(df)

with open(os.path.join(uyulala.modelsDir,folderName,"executionOrder.txt"), "w") as output:
    output.write(str(executionOrder))

##################################################################################
#########################      Backtesting      ##################################
##################################################################################

#import matplotlib.pyplot as plt

raw = h2o.import_file(path=os.path.join(uyulala.dataDir,'raw',folderName))
labelCol = labels[-1]
pandasDF = df.merge(raw)[[labelCol,'predict','Open','Date']].as_data_frame()
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
print 'Using a threshold of %f has an expected return of %f' %(thresholdToUse,maxEV)
#n, bins, patches = plt.hist(x, bins=100, facecolor='g', alpha=0.75, range=(-0.05,0.05))

with open(os.path.join(uyulala.modelsDir,folderName,"threshold.txt"), "w") as output:
    output.write(str(thresholdToUse))

h2o.cluster().shutdown()
