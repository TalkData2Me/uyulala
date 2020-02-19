
import os
uyulalaDir = os.path.dirname(os.path.realpath(__file__))
dataDir = os.path.join(uyulalaDir,'data')
modelsDir = os.path.join(uyulalaDir,'models')

exec(compile(open(os.path.join(uyulalaDir,'config.py'), "rb").read(), os.path.join(uyulalaDir,'config.py'), 'exec'))

exec(compile(open(os.path.join(uyulalaDir,'zweisiedler.py'), "rb").read(), os.path.join(uyulalaDir,'zweisiedler.py'), 'exec'))
