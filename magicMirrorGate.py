
'''
Read transformed data from leftSphnix and apply models built in rightSphnix
'''

##################################################################################
#########################       Configure       ##################################
##################################################################################

assets = 'SchwabOneSource'   # Typically AllStocks, SchwabOneSource, or Test
horizon = 3       # prediction horizon in days


startDate = '2017-06-01'


folderName = 'Assets-'+assets+'--Hrzn-'+str(horizon)
##################################################################################
###########################       Imports       ##################################
##################################################################################

import os
import pandas
import uyulala
import ast
import numpy as np
import pandas_datareader.data as web
import matplotlib.pyplot as plt
import sys
import subprocess
import time



##################################################################################
################# Get and transform data (run leftSphnix) ########################
##################################################################################
if assets!="Test":
    import warnings
    warnings.filterwarnings("ignore")


print('downloading data')

filePath = os.path.join(uyulala.uyulalaDir,'greatRiddleGate','leftSphnix.py')
subprocess.call('''python %s --assets=%s --horizon=%i --start=%s''' % (filePath,assets,horizon,startDate), shell=True)
time.sleep(10)

##################################################################################
###########################       Execute       ##################################
##################################################################################
print('Loading transformed data')

import h2o
try:
    h2o.init(nthreads = -1,max_mem_size="16G",min_mem_size="6G")
except:
    time.sleep(20)
    h2o.init(nthreads = -1,max_mem_size="16G",min_mem_size="6G")

transformed = h2o.import_file(path=os.path.join(uyulala.dataDir,'transformed',folderName),col_types={'DateCol':'enum'})

maxDate = transformed[int(transformed['DateCol'].max()),'DateCol']
transformed = transformed[transformed['DateCol']==maxDate]
print('The max date available is %s' % (maxDate))

print('Running predictions from all models in executionOrder.txt against all data files in transformed data directory')

filePath = os.path.join(uyulala.modelsDir,folderName,'executionOrder.txt')
with open(filePath,'r') as f:
    executionOrder = f.read()
    executionOrder = ast.literal_eval(executionOrder)

for modelName in executionOrder:
    model = h2o.load_model(path=os.path.join(uyulala.modelsDir,folderName,modelName))
    preds = model.predict(transformed)
    preds = preds.set_names([modelName + '_' + s for s in preds.columns])
    transformed = transformed.cbind(preds)

print('Filtering to assets with highest liklihood of return')

thresholdFile = os.path.join(uyulala.modelsDir,folderName,'threshold.txt')
with open(thresholdFile,'r') as f:
    threshold = f.read()
    threshold = ast.literal_eval(threshold)

BuySignalColumn = executionOrder[-2] + '_True'
finalPredictionColumn = executionOrder[-1] + '_predict'

'''
transformed[transformed[finalPredictionColumn]>0.05,finalPredictionColumn] = 0.05  # cap predicted returns to 5%
#transformed['WeightedExpectedReturn'] = transformed[BuySignalColumn] * transformed[finalPredictionColumn]
transformed['WeightedExpectedReturn'] = (transformed[finalPredictionColumn] > 0).ifelse( transformed[BuySignalColumn] * transformed[finalPredictionColumn], transformed[finalPredictionColumn]) # if predicted return is >0, replace with that times the liklihood of being a good buy (lowers value); else, leave as is
forConsideration = transformed[transformed[BuySignalColumn]>0.7]    #for testing, can set this to 0.0 for prod, use 0.7
forConsideration = forConsideration[forConsideration['WeightedExpectedReturn']>0.01]
forConsideration = forConsideration[['Symbol','WeightedExpectedReturn']]
forConsideration = forConsideration.set_names(['Symbol','predict'])
'''

forConsideration = transformed[transformed[finalPredictionColumn]>=threshold]
forConsideration = forConsideration[['Symbol',finalPredictionColumn]]
forConsideration = forConsideration.set_names(['Symbol','predict'])


# Check for early termination
stocks = forConsideration['Symbol'].as_data_frame().iloc[:,0].tolist()
if len(stocks)==0:
    print('No assets meet the threshold expectation. Do not invest in any today.')
    sys.exit()
elif len(stocks)==1:
    print('Only one asset meets the threshold expectation -- invest in %s today.' % stocks)
    sys.exit()
else:
    print('Symbols being considered: ' + str(stocks))
    print('Setting up portfolio optimization')



#download daily price data for each of the stocks in the portfolio
data = web.DataReader(stocks,data_source='yahoo',start='01/01/2005')['Adj Close'].sort_index(ascending=True)

#convert daily stock prices into daily returns
returns = data.pct_change()

#calculate mean daily return and covariance of daily returns
mean_daily_returns = forConsideration.as_data_frame().set_index(['Symbol']).iloc[:,0]
mean_daily_returns = mean_daily_returns.fillna(-0.02)
cov_matrix = returns.cov()

#set number of runs of random portfolio weights
num_portfolios = 10000*len(stocks)

#set up array to hold results
#We have increased the size of the array to hold the weight values for each stock
results = np.zeros((3+len(stocks),num_portfolios))


print('Creating sample portfolios')

for i in range(num_portfolios):
    #select random weights for portfolio holdings
    weightsSize = len(stocks)
    weights = np.array(np.random.random(weightsSize))
    mask = np.random.randint(0,int((weightsSize+1)/5),size=weightsSize).astype(np.bool)
    r = np.zeros(weightsSize)
    weights[mask] = r[mask]
    #rebalance weights to sum to 1
    weights /= np.sum(weights)
    #calculate portfolio return and volatility
    portfolio_return = np.sum(mean_daily_returns * weights) * 252
    portfolio_std_dev = np.sqrt(np.dot(weights.T,np.dot(cov_matrix, weights))) * np.sqrt(252)
    #store results in results array
    results[0,i] = portfolio_return
    results[1,i] = portfolio_std_dev
    #store Sharpe Ratio (return / volatility) - risk free rate element excluded for simplicity
    results[2,i] = results[0,i] / results[1,i]
    #iterate through the weight vector and add data to results array
    for j in range(len(weights)):
        results[j+3,i] = weights[j]

#convert results array to Pandas DataFrame
results_frame = pandas.DataFrame(results.T,columns=['ret','stdev','sharpe']+stocks)

print('Determining optimal portfolios')

#locate position of portfolio with highest Sharpe Ratio
max_sharpe_port = results_frame.iloc[results_frame['sharpe'].idxmax()]
print ('Max Sharpe Ratio Portfolio:')
print(max_sharpe_port)

#locate positon of portfolio with minimum standard deviation
min_vol_port = results_frame.iloc[results_frame['stdev'].idxmin()]
print ('Min Volatility Portfolio:')
print(min_vol_port)

print('Plot efficient frontier')

#create scatter plot coloured by Sharpe Ratio
plt.scatter(results_frame.stdev,results_frame.ret,c=results_frame.sharpe,cmap='RdYlBu',s=0.5)
plt.xlabel('Volatility')
plt.ylabel('Returns')
plt.colorbar()
#plot red star to highlight position of portfolio with highest Sharpe Ratio
plt.scatter(max_sharpe_port[1],max_sharpe_port[0],marker=(5,1,0),color='r',s=500)
#plot green star to highlight position of minimum variance portfolio
plt.scatter(min_vol_port[1],min_vol_port[0],marker=(5,1,0),color='g',s=500)


h2o.cluster().shutdown()

plt.show()
