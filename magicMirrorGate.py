
'''
Read transformed data from leftSphnix and apply models built in rightSphnix
'''

import os
import glob
import pandas
import uyulala

import h2o
h2o.init()

transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed'),col_types={'DateCol':'enum'})

#predictionDF = h2o.as_list(transformed[['Symbol','DateCol']], use_pandas=True)

for modelFile in glob.glob(os.path.join(uyulala.modelsDir,'model|*')):
    fileName = modelFile.split('/')[-1]
    junk,label,modelType = fileName.split('|')
    model = h2o.load_model(path=modelFile)
    prediction = model.predict(transformed)
    #predictionDF = predictionDF.merge(h2o.as_list(prediction, use_pandas=True),left_index=True,right_index=True).rename(columns={'predict':label+'|'+modelType})
    transformed = transformed.cbind(prediction)


#predictionDF.to_csv(path=os.path.join(uyulala.dataDir,'predictions.csv'),index=False)

h2o.export_file(transformed, path=os.path.join(uyulala.dataDir,'predictions'), force=True, parts=-1)


#labels = h2o.import_file(path=os.path.join(uyulala.dataDir,'labeled'),col_types={'DateCol':'enum'})
