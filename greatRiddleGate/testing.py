#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''

##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'Test'   # Typically AllStocks, SchwabOneSource, SchwabETFs, or Test
horizon = 2       # prediction horizon in days

totalBuildTimeAllowed_seconds = 28800


startDate = '2001-01-01'


##################################################################################
###########################       Imports       ##################################
##################################################################################
print('importing packages')
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
print('clearing directories')
try:
    [ os.remove(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'labeled',folderName))

try:
    [ os.remove(os.path.join(uyulala.modelsDir,folderName,f)) for f in os.listdir(os.path.join(uyulala.modelsDir,folderName)) if f!='pca_model_id.txt'  ]
except:
    os.makedirs(os.path.join(uyulala.modelsDir,folderName))

'''
##################################################################################
################# Get and transform data (run leftSphnix) ########################
##################################################################################
print('getting and transforming data')
if assets!="Test":
    import warnings
    warnings.filterwarnings("ignore")


filePath = os.path.join(uyulala.uyulalaDir,'greatRiddleGate','leftSphnix.py')
print('making call: '+'python %s --assets=%s --horizon=%i --start=%s' % (filePath,assets,horizon,startDate))
subprocess.call('python %s --assets=%s --horizon=%i --start=%s' % (filePath,assets,horizon,startDate), shell=True)
'''

##################################################################################
########################       Create        ###############################
##################################################################################
print('creating ')

evaluate = [ f.replace('.csv','') for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if f.endswith(".csv") ]


def createLabels(asset=''):
    try:
        labeled = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',folderName,asset+'.csv'),parse_dates=['DateCol']).set_index('DateCol',drop=False)
        labeled = labeled.drop_duplicates(subset=['Date'], keep='last')
        #########################################################################
        # THE BELOW MUST REMAIN IN CORRECT ORDER SINCE CALLED BELOW BY POSITION #
        #########################################################################
        # Key Regression Field (what's the biggest loss?)
        print('label for biggest loss')
        labeled = uyulala.lowPercentChange(df=labeled,horizon=horizon)
        # Key Regression Field (what's the predicted return?)
        print('label for highest gain')
        labeled = uyulala.percentChange(df=labeled,horizon=horizon,HighOrClose='High')
        # Key Classification Field (is it a good buy?)
        print('label for whether higest gain comes before biggest loss')
        labeled = uyulala.expectedReturnPct(df=labeled,horizon=horizon)
        #add weights
        print('add weights column')
        labeled = uyulala.weights(df=labeled, horizon=horizon,weightForIncrease=1,weightForDecrease=2)
        # Clean-up
        labeled = labeled.drop(['Open','High','Low','Close','Volume'],axis=1)
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


##################################################################################
##########################       Load Data       #################################
##################################################################################


import h2o
from h2o.automl import H2OAutoML
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.grid.grid_search import H2OGridSearch
from h2o.frame import H2OFrame

try:
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1500000000/1.5),min_mem_size="%sG" % int(availMem/1500000000/1.5))
except:
    time.sleep(20)
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1500000000/1.5),min_mem_size="%sG" % int(availMem/1500000000/1.5))


print('importing data')

dataSize = sum(f.stat().st_size for f in Path(os.path.join(uyulala.dataDir,'transformed',folderName)).glob('**/*') if f.is_file() ) + sum(os.path.getsize(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)))
ratio = ((availMem/2000000000) / (20.0000000000000)) / (dataSize/1000000000)
print('full data size: {}gb'.format(dataSize/1000000000.00))

transformed_files = [file for file in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)) if file not in ['.DS_Store']]
#transformed_pca_files = [file for file in os.listdir(os.path.join(uyulala.dataDir,'transformed_pca',folderName)) if file not in ['.DS_Store']]

def sampleAndCleanDataAsNeeded(transformed_files=transformed_files):
    if ratio < 0.98:
        print('reducing file size by {}%'.format(100*(1-ratio)))
        k=math.ceil(len(transformed_files)*ratio)
        sampledFiles=random.choices(transformed_files, k=max(1,min(k,len(transformed_files))))
    else:
        sampledFiles=transformed_files

    print('Files to use: {}'.format(sampledFiles))

    fullDF = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s)" % ('|'.join(sampledFiles),),col_types={'DateCol':'enum','Date':'enum'}).na_omit().merge(h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName),pattern = ".*\.csv",col_types={'DateCol':'enum','Date':'enum'}).na_omit()).na_omit()
    #fullDF = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed_pca',folderName),pattern = "(%s)" % ('|'.join(sampledFiles),),col_types={'DateCol':'enum','Date':'enum'}).na_omit().merge(h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName),pattern = ".*\.csv",col_types={'DateCol':'enum','Date':'enum'}).na_omit()).na_omit()

    ##################################################################################
    #####################       Clean and Split Data       ############################
    ##################################################################################

    uniqueMonths = list(set([x[0:7] for x in fullDF['Date'].unique().as_data_frame()['C1'].tolist()]))
    holdoutMonths = random.choices(uniqueMonths, k=int(len(uniqueMonths)*.15))
    fullDF = fullDF.cbind(H2OFrame(fullDF.as_data_frame()['Date'].apply(lambda x:x[0:7]).to_frame(name='mnth'),column_types=['enum']))
    oot = fullDF[fullDF['mnth'].isin(holdoutMonths),:]
    fullDF = fullDF.drop('mnth')
    oot = oot.drop('mnth')

    print('Final data size is %s' % (fullDF.shape,))
    train,test = fullDF.split_frame(ratios=[.85])
    test = test.rbind(oot)
    print('Training data size: %s' % (train.shape,))
    print('Validation data size: %s' % (test.shape,))
    print(train.head(2))
    print(test.head(2))
    features = [s for s in fullDF.columns if "feat_" in s]
    #features = [s for s in fullDF.columns if "PC" in s]
    labels = [s for s in fullDF.columns if "lab_" in s]

    train = train[(train[labels[0]]>=train[labels[0]].mean()[0] - (2*train[labels[0]].sd()[0])) &
    (train[labels[0]]<=train[labels[0]].mean()[0] + (2*train[labels[0]].sd()[0])) &
    (train[labels[1]]>=train[labels[1]].mean()[0] - (2*train[labels[1]].sd()[0])) &
    (train[labels[1]]<=train[labels[1]].mean()[0] + (2*train[labels[1]].sd()[0]))]

    h2o.remove('oot')
    h2o.remove('fullDF')
    return train,test,labels,features

##################################################################################
#######################       Building Models       ##############################
##################################################################################


print('building models')
#timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)*1.0000000000))
timePerRun = 999999
print('Time per run: ' + str(timePerRun) + ' seconds')

#executionOrder = []


def createQuantileModel(label,quantile_alpha=.05,perf_metric='mse'):
    print('building model for {}'.format(label))
    # GBM hyperparameters
    hyper_params = {'learn_rate': [i * 0.01 for i in range(1,21,2)],
                    'max_depth': list(range(3, 22, 3)),
                    'sample_rate': [i * 0.1 for i in range(5, 10)],
                    'col_sample_rate': [i * 0.1 for i in range(3, 10)],
                    'col_sample_rate_per_tree': [i * 0.1 for i in range(5, 8)],
                    'min_rows': [50, 100, 500],
                    'min_split_improvement': [1e-3, 1e-5]}
    # Search criteria
    search_criteria = {'strategy':'RandomDiscrete', 'max_models':500, 'max_runtime_secs':timePerRun,
                        'stopping_metric':perf_metric, 'stopping_rounds':5}
    gbm = H2OGradientBoostingEstimator(distribution="quantile", quantile_alpha = quantile_alpha, nfolds = 6, ntrees=10000, learn_rate_annealing=0.99)
    # Train and validate a random grid of GBMs
    gbm_grid = H2OGridSearch(model=gbm,
                              grid_id='gbm_grid_{}'.format(label),
                              hyper_params=hyper_params,
                              search_criteria=search_criteria,parallelism=0)
    gbm_grid.train(x=features,y=label,training_frame=train,weights_column='weights')
    # Grab the top GBM model, chosen by validation metric
    print('choosing top model...')
    for i in range(max(10,len(gbm_grid.get_grid(sort_by=perf_metric, decreasing=False).models))):
        ithPerf = eval('''gbm_grid.get_grid(sort_by=perf_metric, decreasing=False).models[{}].model_performance(test).{}()'''.format(i,perf_metric))
        print('model {0} {1}: {2}'.format(i,perf_metric,ithPerf))
        if i==0:
            leadingPerf = ithPerf
            leadingModel = gbm_grid.get_grid(sort_by=perf_metric, decreasing=False).models[i]
            print('''leading test performance: {}'''.format(leadingPerf))
        else:
            if ithPerf < leadingPerf:
                leadingPerf = ithPerf
                leadingModel = gbm_grid.get_grid(sort_by=perf_metric, decreasing=False).models[i]
                print('''leading test performance: {}'''.format(leadingPerf))
    print('variable importance:')
    print(leadingModel.varimp(use_pandas=True))
    return leadingModel

r=1
print('''model building round {}'''.format(r))
while len(transformed_files)>0:
    train,test,labels,features = sampleAndCleanDataAsNeeded(transformed_files=transformed_files)
    low_quantile_model = createQuantileModel(label=labels[0], quantile_alpha=.05, perf_metric='mae')
    #executionOrder.append(low_quantile_model.model_id)
    h2o.save_model(model=low_quantile_model, path=os.path.join(uyulala.modelsDir,folderName,labels[0]), force=True)
    high_quantile_model = createQuantileModel(label=labels[1], quantile_alpha=.05, perf_metric='mae')
    #executionOrder.append(high_quantile_model.model_id)
    h2o.save_model(model=high_quantile_model, path=os.path.join(uyulala.modelsDir,folderName,labels[1]), force=True)

    print('building model for {}'.format(labels[2]))
    aml = H2OAutoML(project_name=labels[2],
                    stopping_rounds=5,max_models=500,
                    max_runtime_secs = timePerRun)
    aml.train(x=features,y=labels[2],training_frame=train,leaderboard_frame=test,weights_column='weights')
    #executionOrder = executionOrder + [aml._leader_id]
    h2o.save_model(model=aml.leader, path=os.path.join(uyulala.modelsDir,folderName,labels[2]), force=True)
    print('variable importance:')
    print(aml.leader.varimp(use_pandas=True))
    transformed_files = [x for x in transformed_files if x not in sampledFiles]
    r=r+1
print('done building models')





##################################################################################
######################       Check Predictions       #############################
##################################################################################


train['dataset'] = 'train'
test['dataset'] = 'test'
fullDF = train.rbind(test)
h2o.remove('train')
h2o.remove('test')

preds = low_quantile_model.predict(fullDF)
preds.set_names([x+'_low' for x in preds.names])
fullDF = fullDF.cbind(preds)

preds = high_quantile_model.predict(fullDF)
preds.set_names([x+'_high' for x in preds.names])
fullDF = fullDF.cbind(preds)

preds = aml.leader.predict(fullDF)
preds.set_names([x+'_expectedReturnPct' for x in preds.names])
fullDF = fullDF.cbind(preds)


with open(os.path.join(uyulala.modelsDir,folderName,"executionOrder.txt"), "w") as output:
    output.write(str(executionOrder))
print(fullDF.head(2))
h2o.export_file(fullDF, path=os.path.join(uyulala.dataDir,'model_data',folderName), force = True, parts=-1)


print('done')
