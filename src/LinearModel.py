
# coding: utf-8

# ### Linear Regression Model

# In[203]:

# import required modules for prediction tasks
import numpy as np
import pandas as pd
import math
import random
import requests
import zipfile
import StringIO
import re
import json
import os

# sklearn functions used for the linear regression model
from sklearn.preprocessing import OneHotEncoder
from scipy import sparse
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import mean_absolute_error

modelDict = {}

# In[204]:

# first step is to load the actual data and exclude rows that are unnecessary
print('loading data...')
# 2005-2014
df = pd.read_csv('../cache/Big10FlightTable.csv', nrows=None)#uncomment for test purposes 2000)
# 2010-2014
#df = pd.read_csv('../cache/Big5FlightTable.csv', nrows=None)#uncomment for test purposes 2000)
# only 2014
#df = pd.read_csv('../cache/BigFlightTable.csv', nrows=None)#uncomment for test purposes 2000)


# In[205]:

print 'columns found: '
print df.columns


# In[206]:

print 'generating additional features'
df['HOUR_OF_ARR'] = df['ARR_TIME'].astype(int) / 10
df['HOUR_OF_DEP'] = df['DEP_TIME'].astype(int) / 10


# In[207]:

# split data into numerical and categorical features
print 'splitting into numerical/categorical features'
numericalFeat = df[['DISTANCE', 'AIRCRAFT_AGE']].copy().astype('float') # Numerical features
num_numFeatures = 2
categoricalFeat = df[['MONTH', 'DAY_OF_MONTH', 'ORIGIN', 
                    'DEST', 'HOUR_OF_ARR', 'HOUR_OF_DEP', 
                    'UNIQUE_CARRIER', 'DAY_OF_WEEK', 'AIRCRAFT_MFR']].copy() # Categorical features


# In[208]:

# for the next step, all features need to be encoded as integers --> create lookup Tables!
def transformToID(df, col):
    vals = np.sort(df[col].unique())
    LookupTable = dict(zip(vals, np.arange(len(vals))))
    for key in LookupTable.keys():
        df.loc[df[col] == key, col] = LookupTable[key]
    return LookupTable


# In[209]:

print 'indexing UNIQUE_CARRIER'
carrierTable = transformToID(categoricalFeat, 'UNIQUE_CARRIER')
with open('../cache/carrierTable.json', 'wb') as outfile:
    json.dump(carrierTable, outfile)
print 'indexing AIRCRAFT_MFR'
mfrTable = transformToID(categoricalFeat, 'AIRCRAFT_MFR')
with open('../cache/manufacturerTable.json', 'wb') as outfile:
    json.dump(mfrTable, outfile)
    

print 'indexing DEST'
destTable = transformToID(categoricalFeat, 'DEST')
with open('../cache/destTable.json', 'wb') as outfile:
    json.dump(destTable, outfile)
print 'indexing ORIGIN'
originTable = transformToID(categoricalFeat, 'ORIGIN')
with open('../cache/originTable.json', 'wb') as outfile:
    json.dump(originTable, outfile)

modelDict['CARRIER'] = carrierTable
modelDict['MANUFACTURER'] = mfrTable
modelDict['DEST'] = destTable
modelDict['ORIGIN'] = originTable
with open('../cache/model.json', 'wb') as f:
    json.dump(modelDict, f)
# In[210]:

# Encode categorical variables as binary ones
print 'encoding categorical variables'
#'MONTH'] = month
#'DAY_OF_MONTH'] = day_of_month
#'ORIGIN'] = origin
#'DEST'] = dest
#'HOUR_OF_ARR'] = hour_of_arr
#'HOUR_OF_DEP'] = hour_of_dep
#'UNIQUE_CARRIER'] = carrier
#'DAY_OF_WEEK'] = day_of_week
#'AIRCRAFT_MFR'] = mfr
n_values = [13, 32, len(originTable.keys()) + 1, \
len(destTable.keys()) + 1, int(2459 / 10) + 1, int(2459 / 10) + 1, \
 len(carrierTable.keys()) + 1, 7, len(mfrTable.keys()) + 1]
encoder = OneHotEncoder(n_values=n_values) 
categoricals_encoded = encoder.fit_transform(categoricalFeat)

# save encoder
encoderDict = {}
encoderDict['values'] = list(encoder.n_values_)
encoderDict['indices'] = list(encoder.feature_indices_)
modelDict['encoder'] = encoderDict

# convert numerical features to sparse matrix
numericals_sparse = sparse.csr_matrix(numericalFeat)

# get data matrix & response variable
X_all = sparse.hstack((numericals_sparse, categoricals_encoded))
y_all = df['ARR_DELAY'].values

# construct test/train set (15%)
print 'splitting test/train set'
X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, test_size = 0.15, random_state = 42)

# before starting the regression, numerical features need to be standardized!
X_train_numericals = X_train[:, 0:num_numFeatures+1].toarray()
X_test_numericals = X_test[:, 0:num_numFeatures+1].toarray()

# use sklearn tools...
print 'normalizing numerical features'
scaler = StandardScaler() 
scaler.fit(X_train_numericals) # get std/mean from train set

# save scaler to cache (for later prediction)
modelDict['scaler_mean'] = list(scaler.mean_)
modelDict['scaler_std'] = list(scaler.std_)
with open('../cache/model.json', 'wb') as f:
    json.dump(modelDict, f)

X_train_numericals = sparse.csr_matrix(scaler.transform(X_train_numericals)) 
X_test_numericals = sparse.csr_matrix(scaler.transform(X_test_numericals))

# update sets
X_train[:, 0:num_numFeatures+1] = X_train_numericals
X_test[:, 0:num_numFeatures+1] = X_test_numericals


# In[211]:

# stochastic gradient based ridge regression
#SGD_params = {'alpha': 10.0 ** -np.arange(1,8)}
SGD_params = {'alpha': 10.0 ** -np.arange(5,8)}
SGD_model = GridSearchCV(SGDRegressor(random_state = 42, verbose=1, n_iter=10),                          SGD_params, scoring = 'mean_absolute_error', cv = 4) # cross validate 4 times


# In[213]:

# train the model, this might take some time...
SGD_model.fit(X_train, y_train)

modelDict['params'] = SGD_model.best_estimator_.get_params()
modelDict['coeff'] = list(SGD_model.best_estimator_.coef_)
modelDict['intercept'] = list(SGD_model.best_estimator_.intercept_)

# save model to file
print 'saving model'
with open('../cache/model.json', 'wb') as f:
    json.dump(modelDict, f)

print SGD_model.best_estimator_.coef_


# In[214]:

def rmse(y, y_pred):
    return np.sqrt(((y - y_pred)**2).mean())

print 'computing statistics:'
y_pred = SGD_model.predict(X_test)
RMSE = rmse(y_test, y_pred)
MAS = mean_absolute_error(y_test, y_pred)
print 'RMSE:' + str(RMSE)
print 'MAS:' + str(MAS)

modelDict['RMSE'] = RMSE
modelDict['MAS'] = MAS
with open('../cache/model10.json', 'wb') as f:
    json.dump(modelDict, f)
