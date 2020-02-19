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

totalBuildTimeAllowed_seconds = 3600


startDate = '2004-01-01'


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
        labeled['returnIfWrong'] = (labeled.Open.shift(-4) - labeled.Open.shift(-1)) / labeled.Open.shift(-1)
        # Supplemental labels
        #labeled = uyulala.buy(df=labeled,horizon=1,HighOrClose='High',threshold=0.01)
        #labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='High')
        #labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='Close')
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.01)
        #labeled = uyulala.percentChange(df=labeled,horizon=2,HighOrClose='High')
        #labeled = uyulala.percentChange(df=labeled,horizon=2,HighOrClose='Close')
        #labeled = uyulala.lowPercentChange(df=labeled,horizon=2)
        #labeled = uyulala.lowPercentChange(df=labeled,horizon=3)
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.02)
        #labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.03)
        #labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.02)
        #labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.03)
        #labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.04)
        #labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.05)
        #labeled = uyulala.absolutePercentChange(df=labeled, horizon=3, HighOrClose='High')
        #labeled = uyulala.absolutePercentChange(df=labeled, horizon=2, HighOrClose='High')
        # THE BELOW MUST REMAIN IN CORRECT ORDER SINCE CALLED BELOW BY POSITION
        # Key Classification Field (is it a good buy?)
        labeled = uyulala.buy(df=labeled,horizon=3,HighOrClose='High',threshold=0.01)
        # Key Regression Field (what's the predicted return?)
        labeled = uyulala.percentChange(df=labeled,horizon=3,HighOrClose='High')
        # Clean-up
        labeled.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        labeled = labeled.dropna()
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
    h2o.init(max_mem_size=uyulala.systemMemGBstr,min_mem_size=uyulala.minH2OmemGBstr)
except:
    time.sleep(20)
    h2o.init(max_mem_size=uyulala.systemMemGBstr,min_mem_size=uyulala.minH2OmemGBstr)

print('importing data')
transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName))
transformed = transformed.na_omit()
features = [s for s in transformed.columns if "feat_" in s]


transformedDataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)))
ratio = ((uyulala.minH2OmemGB * 1000000000 / 10.0000000000000) / transformedDataSize)
from h2o.transforms.decomposition import H2OPCA
pca = H2OPCA(transform = "STANDARDIZE", pca_method="Randomized",k=min(50,transformed.shape[1]),compute_metrics = False,max_iterations=100)
if ratio < .98:
    miniTransformed,drop = transformed.split_frame(ratios=[ratio])
    h2o.remove("drop")
    drop=None
    pca.train(x=features,training_frame=miniTransformed)
else:
    pca.train(x=features,training_frame=transformed)



h2o.save_model(model=pca, path=os.path.join(uyulala.modelsDir,folderName), force=True)
executionOrder = [pca.model_id]
transPCA = pca.predict(transformed[features])
transformed = transformed[['Date','Symbol','DateCol']]
transformed = transformed.cbind(transPCA)
features = [s for s in transformed.columns if "PC" in s]
print(pca.varimp(use_pandas=False))

labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName))
labeled = labeled.na_omit()
labels = [s for s in labeled.columns if "lab_" in s]

df = labeled.merge(transformed)
df = df.na_omit()


h2o.remove("transformed")
h2o.remove("labeled")
transformed = None
labeled = None



from h2o.estimators.aggregator import H2OAggregatorEstimator
agg = H2OAggregatorEstimator(target_num_exemplars=min(600000,df.shape[0]),transform="STANDARDIZE")
agg.train(training_frame=df,ignored_columns=['Date','Symbol','DateCol'])
df = agg.aggregated_frame
h2o.save_model(model=agg, path=os.path.join(uyulala.modelsDir,folderName), force=True)
#executionOrder = executionOrder + [agg.model_id]
print('Reduced data size is %s' % (df.shape,))




dataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName))) + sum(os.path.getsize(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)))

#ratio = min(((uyulala.minH2OmemGB * 1000000000 / 8.0000000000000) / dataSize), 0.85)  # H2O recommends to have a cluster 4X the size of the data
ratio = 0.85

train,test = df.split_frame(ratios=[ratio])
h2o.remove("df")
df=None

print('Training data size is %s' % (train.shape,))
print('Testing data size is %s' % (test.shape,))



timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)))
print('Time per run: ' + str(timePerRun) + ' seconds')

print('running the first layer of models')
for label in labels:
    print('first run of '+label)
    # project_name=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(99))
    aml = H2OAutoML(project_name=label+'0',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = timePerRun)
    aml.train(x=features,y=label,training_frame=train,leaderboard_frame=test,weights_column='counts')
    print(aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0])
    print(aml.leaderboard[0,:])
    executionOrder = executionOrder + [aml._leader_id]
    preds = aml.leader.predict(test)
    preds = preds.set_names([label + '_' + s for s in preds.columns])
    test = test.cbind(preds)
    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName), force=True)
    h2o.remove("aml")
    aml = None
    del aml


with open(os.path.join(uyulala.modelsDir,folderName,"executionOrder.txt"), "w") as output:
    output.write(str(executionOrder))

##################################################################################
#########################      Backtesting      ##################################
##################################################################################

#import matplotlib.pyplot as plt
labelCol = labels[-1]
predict = labels[-1]+'_predict'
buy = labels[-2]+'_predict'
test = test[['Date','returnIfWrong',labelCol,predict,buy]]
test = test.as_data_frame()
print(test.head(3))

maxEV = 0
thresholdToUse = 0
for threshold in numpy.arange(0.005,0.051,0.001):
    test['invested'] = test.apply(lambda row: 1 if ((row[predict] >= threshold) & (row[buy]==True)) else 0, axis=1)
    test['return'] = test.apply(lambda row: threshold if ((row.invested == 1) & (row[labelCol]>=threshold)) else (row.returnIfWrong if row.invested == 1 else 0),axis=1)
    test = test.dropna()
    dailyDF = pandas.DataFrame(test.groupby(['Date'])[['return','invested']].sum())
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
