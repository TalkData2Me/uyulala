
import os
uyulalaDir = os.path.dirname(os.path.realpath(__file__))
dataDir = os.path.join(uyulalaDir,'data')
modelsDir = os.path.join(uyulalaDir,'models')

execfile(os.path.join(uyulalaDir,'zweisiedler.py'))
