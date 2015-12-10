# this script fits the linear model and saves the model
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
import sklearn
from sklearn.cross_validation import train_test_split
from sklearn import linear_model

import pickle



# load data
print 'loading data...'
df = pd.read_csv('../cache/linear_model_features.csv')

cols_to_use = list(set(df.columns) - set([u'Unnamed: 0', u'ARR_DELAY', u'FL_DATE', u'UNIQUE_CARRIER', u'ORIGIN', u'DEST', u'DISTANCE', u'AIRCRAFT_MFR']))
cols_to_use.sort()

print 'using columns ' + str(cols_to_use)
X = df[cols_to_use]
y = df[u'ARR_DELAY']

# split into test and train set (use 20% test set)
print 'splitting into sets...'
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# saving sets
print 'saving train/test sets'
X_train.to_csv('../cache/lm_X_train.csv')
X_test.to_csv('../cache/lm_X_test.csv')
y_train.to_csv('../cache/lm_y_train.csv')
y_test.to_csv('../cache/lm_y_test.csv')

# use all CPU power!
clf = sklearn.linear_model.LinearRegression(normalize=True, n_jobs=-1)

# fit the model!
print 'fitting the model...'
clf.fit(X_train, y_train)
print clf.coef_
print 'saving model...'
pickle.dump(clf, open('../data/linear_model.pkl', 'w'))
print 'done'