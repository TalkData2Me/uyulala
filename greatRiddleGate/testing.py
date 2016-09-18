

import os

import h2o
h2o.init()

transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed'))

glm = h2o.h2o.load_model(path=os.path.join(uyulala.modelsDir,'model|' + toPredict + '|glm'))
prediction = glm.predict(transformed)
