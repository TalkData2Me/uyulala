import pandas
import uyulala
import os

def createLabels(asset=''):
    try:
        rawData = pandas.read_csv(os.path.join(uyulala.dataDir,'raw',asset+'.csv'),parse_dates=['DateCol'])
        print rawData.columns
        labeled = uyulala.highPoint(df=rawData,horizon=7)
        print labeled.columns
        labeled.to_csv(os.path.join(uyulala.dataDir,'labeled',asset+'.csv'),index=False)
        return asset
    except:
        print 'unable to create label for '+asset
        pass

createLabels(asset = 'CHIX')
