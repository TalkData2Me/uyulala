#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''



##################################################################################
#########################       Configure       ##################################
##################################################################################

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


##################################################################################
###########################       Setup Calcs       ##################################
##################################################################################

df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=33&y=6&min_LatestClosePrice=11.00&fooldomain=caps.fool.com&max_LatestClosePrice=50.00&MarketCap=-1&')
evaluate = df.Symbol.tolist()


evaluate = ['CHIX', 'QQQC', 'SDEM', 'URA']


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
        labeled = uyulala.percentChange(df=rawData,horizon=7)
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


'''
import h2o
h2o.init()

transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed'))
labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled'))
df = labeled.merge(transformed)
transformedCols = transformed.columns
transformedCols.remove('Symbol').remove('DateCol')

from h2o.estimators.glm import H2OGeneralizedLinearEstimator
glm = H2OGeneralizedLinearEstimator(family='gaussian', nfolds=10)
glm.train(x=transformedCols,y="percentChange",training_frame=df.na_omit())

from h2o.estimators.gbm import H2OGradientBoostingEstimator
gbm = H2OGradientBoostingEstimator(distribution="gaussian",ntrees=10,max_depth=3,min_row=2,learn_rate="0.2")
gbm.train(x=transformedCols,y="percentChange",training_frame=df.na_omit())
'''
