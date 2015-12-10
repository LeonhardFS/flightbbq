
# coding: utf-8

# ### Linear Regression Model

# In[136]:

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


# In[137]:

# first step is to load the actual data and exclude rows that are unnecessary
print('loading data...')
df = pd.read_csv('../cache/BigFlightTable.csv', nrows=None)#2000) # uncomment this for test purposes!


# In[138]:

print 'columns found: '
print df.columns


# In[139]:

print 'generating additional features'
df['HOUR_OF_ARR'] = df['ARR_TIME'].astype(int) / 10
df['HOUR_OF_DEP'] = df['DEP_TIME'].astype(int) / 10


# In[140]:

# split data into numerical and categorical features
print 'splitting into numerical/categorical features'
numericalFeat = df[['DISTANCE', 'AIRCRAFT_AGE']].copy().astype('float') # Numerical features
num_numFeatures = 2
categoricalFeat = df[['MONTH', 'DAY_OF_MONTH', 'ORIGIN', 
                    'DEST', 'HOUR_OF_ARR', 'HOUR_OF_DEP', 
                    'UNIQUE_CARRIER', 'DAY_OF_WEEK', 'AIRCRAFT_MFR']].copy() # Categorical features


# In[141]:

# for the next step, all features need to be encoded as integers --> create lookup Tables!
def transformToID(df, col):
    vals = df[col].unique()
    LookupTable = dict(zip(vals, np.arange(len(vals))))
    for key in LookupTable.keys():
        df.loc[df[col] == key, col] = LookupTable[key]
    return LookupTable


# In[142]:

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


# In[143]:

# Encode categorical variables as binary ones
print 'encoding categorical variables'
encoder = OneHotEncoder() 
categoricals_encoded = encoder.fit_transform(categoricalFeat)

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
with open('../cache/scalerValues.csv', 'wb') as f:
    f.write('mean: ' + str(list(scaler.mean_)) + '\n')
    f.write('std : ' + str(list(scaler.std_)) + '\n')

X_train_numericals = sparse.csr_matrix(scaler.transform(X_train_numericals)) 
X_test_numericals = sparse.csr_matrix(scaler.transform(X_test_numericals))

# update sets
X_train[:, 0:num_numFeatures+1] = X_train_numericals
X_test[:, 0:num_numFeatures+1] = X_test_numericals


# In[144]:

# stochastic gradient based ridge regression
SGD_params = {'alpha': 10.0 ** -np.arange(1,8)}
SGD_model = GridSearchCV(SGDRegressor(random_state = 42, verbose=1),                          SGD_params, scoring = 'mean_absolute_error', cv = 4) # cross validate 4 times


# In[145]:

# train the model, this might take some time...
SGD_model.fit(X_train, y_train)


# In[146]:

def rmse(y, y_pred):
    return np.sqrt(((y - y_pred)**2).mean())

print 'computing statistics:'
y_pred = SGD_model.predict(X_test)
print 'RMSE:' + str(rmse(y_test, y_pred))
print 'MAS:' + str(mean_absolute_error(y_test, y_pred))
