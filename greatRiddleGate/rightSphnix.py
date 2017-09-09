#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''



##################################################################################
#########################       Configure       ##################################
##################################################################################

totalBuildTimeAllowed_seconds = 600

daysOfHistoryForModelling = 180


horizonDays = 2   # prediction horizon in days
windowDays = 5    # sliding window size in days



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
from sklearn import neighbors
from sklearn.linear_model import LinearRegression
import random
import string


##################################################################################
###########################       Setup Calcs       ##################################
##################################################################################

df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=33&y=6&min_LatestClosePrice=11.00&fooldomain=caps.fool.com&max_LatestClosePrice=50.00&MarketCap=-1&')
evaluate = df.Symbol.tolist()


evaluate = ['CHIX', 'QQQC', 'SDEM', 'URA']   # TODO: comment this out to run against everything


d = datetime.date.today() - datetime.timedelta(days=daysOfHistoryForModelling)
#historyStart = '2016-01-01'
historyStart = str(d)
historyEnd = None

horizon = horizonDays*2
window = windowDays*2
datasetName = 'Hrzn'+str(horizon)+'Wndw'+str(window)



##################################################################################
###########################       Execute       ##################################
##################################################################################




def createLabels(asset=''):
    try:
        rawData = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',asset+'.csv'),parse_dates=['DateCol']).set_index('DateCol',drop=False)
        # Classification
        labeled = uyulala.buy(df=rawData,horizon=1,HighOrClose='Close',threshold=0.01)
        # Regression
        labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='Close')

        # Clean-up
        labeled.drop(['Open','High','Low','Close','Volume'],inplace=True,axis=1)
        labeled.to_csv(os.path.join(uyulala.dataDir,'labeled',asset+'.csv'),index=False)
        return asset
    except:
        print 'unable to create label for '+asset
        pass

pool = Pool(24)

results = pool.map(createLabels, evaluate)

pool.close()  #close the pool and wait for the work to finish
pool.join()



import h2o
from h2o.automl import H2OAutoML
h2o.init()


transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed'))
labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled'))
df = labeled.merge(transformed)


features = [s for s in transformed.columns if "feat_" in s]
labels = [s for s in labeled.columns if "lab_" in s]


timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)+2*len(labels)+2*5))
print 'Time per run: ' + str(timePerRun) + ' seconds'


L0Results = df[['Symbol','DateCol']]
executionOrder = []
for label in labels:
    print label
    # project_name=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(99))
    aml = H2OAutoML(project_name=label+'0',
                    stopping_tolerance=0.000001,
                    max_runtime_secs = timePerRun)
    aml.train(x=features,y=label,training_frame=df)
    print aml.leaderboard.as_data_frame()['model_id'].tolist()[0:1][0]
    print aml.leaderboard[0,:]
    executionOrder = executionOrder + [aml._leader_id]

    preds = aml.leader.predict(df)
    L0Results = L0Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml


L1Results = df[['Symbol','DateCol']]
for label in labels:
    print label
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
    L1Results = L1Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml


BuyResults = df[['Symbol','DateCol']]
for label in ['lab_buy_H1Close_0.01']:
    print label
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
    BuyResults = BuyResults.cbind(preds)

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml


BuyResults = BuyResults.drop(['predict','False']).set_names(['Symbol','DateCol','BuyConfidence'])
BuyResults = BuyResults[BuyResults['BuyConfidence']>0.7]
df = BuyResults.merge(df)


PredictedPerformance = df[['Symbol','DateCol']]
for label in ['lab_percentChange_H1Close']:
    print label
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

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml


with open(os.path.join(uyulala.modelsDir,"executionOrder.txt"), "w") as output:
    output.write(str(executionOrder))
