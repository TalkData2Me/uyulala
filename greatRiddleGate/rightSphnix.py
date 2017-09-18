#!/usr/bin/python


'''
rightSphnix:
* create Labels and store to disk
* build models
'''



##################################################################################
#########################       Configure       ##################################
##################################################################################

totalBuildTimeAllowed_seconds = 36000

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

### All stocks
#df = pandas.read_csv('http://www.motleyfool.idmanagedsolutions.com/stocks/screener_alt_results.idms?csv=1&SHOW_RESULT=1&BLOCKSIZE=ALL&SORT=&ORDER=&themetype=caps&param=1&x=33&y=6&min_LatestClosePrice=11.00&fooldomain=caps.fool.com&max_LatestClosePrice=50.00&MarketCap=-1&')
#evaluate = df.Symbol.tolist()

### Schwab OneSource ETFs
evaluate = ['SCHB','SCHX','SCHV','SCHG','SCHA','SCHM','SCHD','SMD','SPYD','MDYV','SLYV','QUS','MDYG','SLYG','DEUS','ONEV','ONEY','SHE','RSP','XLG','ONEO','SPYX','FNDX','FNDB','SPHB','FNDA','SPLV','DGRW','RFV','RPV','RZG','RPG','RZV','RFG','DGRS','SDOG','EWSC','KRMA','JHML','QQQE','RWL','RWJ','RWK','JPSE','WMCR','DWAS','SYG','SYE','SYV','DEF','JHMM','RDIV','PDP','PKW','KNOW','JPUS','JPME','ESGL','SCHF','SCHC','SCHE','FNDF','ACIM','FEU','QCAN','LOWC','QEFA','QGBR','QEMM','QJPN','QDEU','CWI','IDLV','DBEF','HFXI','DEEF','FNDE','FNDC','WDIV','JPN','DDWM','DBAW','EELV','HDAW','DBEZ','DWX','HFXJ','HFXE','JHDG','DXGE','EDIV','GMF','PAF','IDOG','DEMG','PID','DXJS','IHDG','DNL','EUSC','GXC','EDOG','DGRE','EWX','DBEM','JHMD','CQQQ','JPIN','EWEM','EEB','PIN','PIZ','PIE','JPGE','JPEU','HGI','FRN','JPEH','JPIH','JPEM','ESGF','SCHZ','SCHP','SCHR','SCHO','TLO','ZROZ','FLRN','SHM','AGGY','CORP','AGZD','BSCQ','BSCJ','BSCH','BSCK','BSCL','BSCO','BSCN','BSCP','BSCM','BSCI','HYLB','RVNU','TFI','BWZ','PGHY','AGGE','CJNK','CWB','HYLV','BSJO','BSJJ','BSJM','BSJL','BSJK','BSJN','BSJI','BSJH','HYZD','AGGP','DSUM','BWX','PCY','PHB','HYMB','IBND','HYS','DWFI','BKLN','SRLN','SCHH','NANR','RTM','RYT','RHS','GNR','GII','RGI','EWRE','RYU','RYE','RYH','RCD','RYF','GHII','MLPX','MLPA','RWO','SGDM','RWX','PBS','CGW','ENFR','BOTZ','PSAU','CROP','GRES','JHMF','JHMT','JHMH','JHMC','JHMI','JHMA','JHME','JHMS','JHMU','ZMLP','GAL','FXY','FXS','FXF','FXE','FXC','FXB','FXA','PUTW','PSK','USDU','PGX','VRP','DYLS','INKM','RLY','WDTI','MNA','CVY','QMN','QAI','LALT','SIVR','SGOL','GLDW','PPLT','PALL','GLTR','USL','GCC','USCI','BNO','UGA','UNL','CPER']


# evaluate = ['CHIX', 'QQQC', 'SDEM', 'URA']   # TODO: comment this out to run against everything


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
        labeled = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',asset+'.csv'),parse_dates=['DateCol']).set_index('DateCol',drop=False)
        # Supplemental labels
        labeled = uyulala.buy(df=labeled,horizon=1,HighOrClose='High',threshold=0.01)
        labeled = uyulala.percentChange(df=labeled,horizon=1,HighOrClose='High')
        labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.01)
        labeled = uyulala.percentChange(df=labeled,horizon=2,HighOrClose='High')
        labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.02)
        labeled = uyulala.buy(df=labeled,horizon=2,HighOrClose='High',threshold=0.03)
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
        labeled.to_csv(os.path.join(uyulala.dataDir,'labeled',asset+'.csv'),index=False)
        return asset
    except:
        print 'unable to create label for '+asset
        pass

print 'labelling data'
pool = Pool(24)

results = pool.map(createLabels, evaluate)

pool.close()  #close the pool and wait for the work to finish
pool.join()

results = None


import h2o
from h2o.automl import H2OAutoML
try: h2o.init(max_mem_size="16G",min_mem_size="6G")
except: h2o.init(max_mem_size="16G",min_mem_size="6G")

print 'importing data'
transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed'))
labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled'))
df = labeled.merge(transformed)


features = [s for s in transformed.columns if "feat_" in s]
labels = [s for s in labeled.columns if "lab_" in s]


timePerRun = int(totalBuildTimeAllowed_seconds / (len(labels)+2*len(labels)+2*5))
print 'Time per run: ' + str(timePerRun) + ' seconds'

print 'running the first layer of models'
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
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L0Results = L0Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml

print 'running the second layer of models'
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
    if preds.shape[1]>1:
        preds = preds['True']
    preds = preds.set_names([aml._leader_id + '_' + s for s in preds.columns])
    L1Results = L1Results.cbind(preds)

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml

print 'running the final Buy Signal model'
BuyResults = df[['Symbol','DateCol']]
for label in [labels[-2]]:
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
    preds = preds.drop(['predict','False']).set_names([aml._leader_id + '_True'])
    BuyResults = BuyResults.cbind(preds)
    BuyResults = BuyResults[BuyResults[aml._leader_id + '_True']>0.6]

    h2o.save_model(model=aml.leader, path=uyulala.modelsDir, force=True)
    aml = None
    del aml


df = BuyResults.merge(df)


PredictedPerformance = df[['Symbol','DateCol']]
for label in [labels[-1]]:
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

h2o.cluster().shutdown()
