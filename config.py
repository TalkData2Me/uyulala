
import multiprocessing
availableCores = multiprocessing.cpu_count()

import psutil
systemMemGB = float(psutil.virtual_memory().total >> 30)
systemMemGBstr = str(int(systemMemGB)) + 'G'

minH2OmemGB = systemMemGB - 2
minH2OmemGBstr = str(int(minH2OmemGB)) + 'G'
