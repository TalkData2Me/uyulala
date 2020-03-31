
##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'AllStocks'   # Typically AllStocks, SchwabOneSource, or Test
horizon = 2       # prediction horizon in days

totalBuildTimeAllowed_seconds = 3600


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
from psutil import virtual_memory

totMem = virtual_memory().total
availMem = virtual_memory().available

folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)





import h2o
from h2o.automl import H2OAutoML
try:
    h2o.init(max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))
except:
    time.sleep(20)
    h2o.init(max_mem_size="%sG" % int(totMem/1000000000),min_mem_size="%sG" % int(availMem/1000000000))




print('importing data')

dataSize = sum(os.path.getsize(os.path.join(uyulala.dataDir,'transformed',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'transformed',folderName))) + sum(os.path.getsize(os.path.join(uyulala.dataDir,'labeled',folderName,f)) for f in os.listdir(os.path.join(uyulala.dataDir,'labeled',folderName)))


ratio = (availMem / 4.0000000000000) / (dataSize*.75)  # H2O recommends to have a cluster 2-4X the size of the data RAM>=dataSize*4 (ratio=1) --> RAM/4 = ratio*dataSize --> (RAM/4)/dataSize = ratio Already taking 25% off for blending frame (only leaving 75% of original data size)
if ratio < 0.98:                  #same ratio used in for loop
    print('Reducing data size by %s percent due to RAM per data size' % ((1-ratio)*100,))


labeled = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled',folderName),pattern = ".*\.csv")
