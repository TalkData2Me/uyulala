

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




##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'AllStocks'
horizon = 2
start = '2014-01-01'
end = None


pullPriceHistFor = uyulala.assetList(assets=assets)

folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)

#################################################
###        PCA for Transformed data           ###
#################################################

import math
import random
from psutil import virtual_memory
import h2o
from h2o.estimators.pca import H2OPrincipalComponentAnalysisEstimator

transformedAssets = [f.split('.')[0] for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName))]
totMem = virtual_memory().total
availMem = virtual_memory().available

try:
    [ os.remove(os.path.join(uyulala.dataDir,'transformed_pca',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed_pca',folderName)) if f.endswith(".csv") ]
except:
    os.makedirs(os.path.join(uyulala.dataDir,'transformed_pca',folderName))

try:
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))
except:
    time.sleep(20)
    h2o.init(nthreads = -1,max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))

dataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName)))
ratio = (availMem / (10.0000000000000)) / (dataSize)  # H2O recommends to have a cluster 2-4X the size of the data RAM>=dataSize*4 (ratio=1) --> RAM/4 = ratio*dataSize --> (RAM/4)/dataSize = ratio

k=math.ceil(len(transformedAssets)*ratio)
print('Selecting %s random assets' % k)
sampledAssets=random.choices(transformedAssets, k=min(k,len(transformedAssets)))

transSample = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s).*\.csv" % ('|'.join(sampledAssets),),col_types={'DateCol':'enum','Date':'enum'}).na_omit()

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
transformedAssets = [ele for ele in transformedAssets if ele not in list(transSample.columns)]
h2o.remove_all()
while len(transformedAssets) > 0:
    i=i+1
    pca=h2o.load_model(os.path.join(uyulala.modelsDir,folderName,model_id))
    sampledAssets = random.choices(transformedAssets, k=min(k,len(transformedAssets)))
    transSample = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),pattern = "(%s).*\.csv" % ('|'.join(sampledAssets),),col_types={'DateCol':'enum','Date':'enum'}).na_omit()
    pred = pca.predict(transSample)
    h2o.export_file(transSample[:, ['Date', 'Symbol', 'DateCol']].cbind(pred[:,list(range(numCompsToUse))]), path=os.path.join(uyulala.dataDir,'transformed_pca',folderName,'Run%s' % i), force = True, parts=-1)
    transformedAssets = [ele for ele in transformedAssets if ele not in list(transSample.columns)]
    h2o.remove_all()

print('done with pca')
