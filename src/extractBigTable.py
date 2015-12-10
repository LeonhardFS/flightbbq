
# coding: utf-8

# ### Linear Regression Model

# In[1]:

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


# In[3]:

# first step: Accumulate the data
# given a list of column labels remove all columns that are in the df
def filterDF(df, cols):
    colsToKeep = list(set(df.columns) & set(cols))
    
    return df[colsToKeep]

# given a dataframe this function groups all manufacturers into one category whose market share is low (default: 1%)
# also groups together some companies
def compressManufacturers(df, percentage=1.):
    df['AIRCRAFT_MFR'] = df['AIRCRAFT_MFR'].map(lambda x: x.strip())
    mfr_stats = df['AIRCRAFT_MFR'].value_counts()
    
    market_share = mfr_stats.values * 100. / np.sum(mfr_stats.values)
    idxs = np.where(market_share < percentage)
    names = np.array([el for el in list(mfr_stats.keys())])

    # get labels for small manufacturers
    smallMFR = names[idxs]

    # perform merging for the big companies
    # Douglas airplanes
    df.loc[df['AIRCRAFT_MFR'] == 'MCDONNELL DOUGLAS AIRCRAFT CO', 'AIRCRAFT_MFR'] = 'MCDONNELL DOUGLAS'
    df.loc[df['AIRCRAFT_MFR'] == 'MCDONNELL DOUGLAS CORPORATION', 'AIRCRAFT_MFR'] = 'MCDONNELL DOUGLAS'
    df.loc[df['AIRCRAFT_MFR'] == 'MCDONNELL DOUGLAS CORPORATION', 'AIRCRAFT_MFR'] = 'DOUGLAS'

    # Embraer
    df.loc[df['AIRCRAFT_MFR'] == 'EMBRAER S A', 'AIRCRAFT_MFR'] = 'EMBRAER'

    # Airbus
    df.loc[df['AIRCRAFT_MFR'] == 'AIRBUS INDUSTRIE', 'AIRCRAFT_MFR'] = 'AIRBUS'

    # the small manufacturers
    for name in smallMFR:
        df.loc[df['AIRCRAFT_MFR'] == name, 'AIRCRAFT_MFR'] = 'SMALL'
        
    return df


# In[4]:

# MAIN PART:


# In[ ]:

print('loading helper files...')
# load helper files
z = zipfile.ZipFile('../externalData/AircraftInformation.zip')
df_master  = pd.DataFrame.from_csv(z.open('MASTER.txt'))
df_aircrafts  = pd.DataFrame.from_csv(z.open('ACFTREF.txt'))
master = df_master[['MFR MDL CODE', 'YEAR MFR']].reset_index()
aircrafts = df_aircrafts['MFR'].reset_index()
master.columns = ['TAIL_NUM', 'CODE', 'YEAR']
aircrafts.columns = ['CODE', 'MFR']
joinedAircraftInfo = pd.merge(master, aircrafts, how='left', on='CODE')
joinedAircraftInfo.TAIL_NUM = joinedAircraftInfo.TAIL_NUM.apply(lambda x: x.strip())

# possible fields
# [u'YEAR', u'QUARTER', u'MONTH', u'DAY_OF_MONTH', u'DAY_OF_WEEK',
#        u'FL_DATE', u'UNIQUE_CARRIER', u'AIRLINE_ID', u'CARRIER', u'TAIL_NUM',
#        u'FL_NUM', u'ORIGIN', u'ORIGIN_CITY_NAME', u'ORIGIN_STATE_ABR',
#        u'ORIGIN_STATE_FIPS', u'ORIGIN_STATE_NM', u'ORIGIN_WAC', u'DEST',
#        u'DEST_CITY_NAME', u'DEST_STATE_ABR', u'DEST_STATE_FIPS',
#        u'DEST_STATE_NM', u'DEST_WAC', u'CRS_DEP_TIME', u'DEP_TIME',
#        u'DEP_DELAY', u'DEP_DELAY_NEW', u'DEP_DEL15', u'DEP_DELAY_GROUP',
#        u'DEP_TIME_BLK', u'TAXI_OUT', u'WHEELS_OFF', u'WHEELS_ON', u'TAXI_IN',
#        u'CRS_ARR_TIME', u'ARR_TIME', u'ARR_DELAY', u'ARR_DELAY_NEW',
#        u'ARR_DEL15', u'ARR_DELAY_GROUP', u'ARR_TIME_BLK', u'CANCELLED',
#        u'CANCELLATION_CODE', u'DIVERTED', u'CRS_ELAPSED_TIME',
#        u'ACTUAL_ELAPSED_TIME', u'AIR_TIME', u'FLIGHTS', u'DISTANCE',
#        u'DISTANCE_GROUP', u'CARRIER_DELAY', u'WEATHER_DELAY', u'NAS_DELAY',
#        u'SECURITY_DELAY', u'LATE_AIRCRAFT_DELAY', u'FIRST_DEP_TIME',
#        u'TOTAL_ADD_GTIME', u'LONGEST_ADD_GTIME', u'DIV_AIRPORT_LANDINGS',
#        u'DIV_REACHED_DEST', u'DIV_ACTUAL_ELAPSED_TIME', u'DIV_ARR_DELAY',
#        u'DIV_DISTANCE', u'DIV1_AIRPORT', u'DIV1_WHEELS_ON',
#        u'DIV1_TOTAL_GTIME', u'DIV1_LONGEST_GTIME', u'DIV1_WHEELS_OFF',
#        u'DIV1_TAIL_NUM', u'DIV2_AIRPORT', u'DIV2_WHEELS_ON',
#        u'DIV2_TOTAL_GTIME', u'DIV2_LONGEST_GTIME', u'DIV2_WHEELS_OFF',
#        u'DIV2_TAIL_NUM', u'DIV3_AIRPORT', u'DIV3_WHEELS_ON',
#        u'DIV3_TOTAL_GTIME', u'DIV3_LONGEST_GTIME', u'DIV3_WHEELS_OFF',
#        u'DIV3_TAIL_NUM', u'DIV4_AIRPORT', u'DIV4_WHEELS_ON',
#        u'DIV4_TOTAL_GTIME', u'DIV4_LONGEST_GTIME', u'DIV4_WHEELS_OFF',
#        u'DIV4_TAIL_NUM', u'DIV5_AIRPORT', u'DIV5_WHEELS_ON',
#        u'DIV5_TOTAL_GTIME', u'DIV5_LONGEST_GTIME', u'DIV5_WHEELS_OFF',
#        u'DIV5_TAIL_NUM', u'Unnamed: 93', u'AIRCRAFT_YEAR', u'AIRCRAFT_AGE',
#        u'AIRCRAFT_MFR']

# define here which columns to include in the data extraction process
columnsToUse = [u'YEAR', u'QUARTER', u'MONTH', u'DAY_OF_MONTH', u'DAY_OF_WEEK', u'DEST_CITY_NAME', u'ORIGIN_CITY_NAME'
       u'FL_DATE', u'UNIQUE_CARRIER', u'AIRLINE_ID',u'TAIL_NUM',
       u'FL_NUM', u'ORIGIN', u'ORIGIN_CITY_NAME',
       u'ORIGIN_STATE_NM', u'ORIGIN_WAC', u'DEST',
       u'DEST_CITY_NAME',u'ARR_DELAY', u'ARR_DELAY_NEW',
       u'ARR_DEL15', u'CANCELLED', u'DIVERTED', u'DISTANCE',u'AIRCRAFT_YEAR', u'AIRCRAFT_AGE',
       u'AIRCRAFT_MFR', u'ARR_TIME', u'DEP_TIME']

# given the raw BTS data, this function filters it and returns 
# a filtered version along with how much percent has been removed
def processData(rawData):
    # filter to exclude diverted and cancelled flights
    filteredData = rawData[(rawData.DIVERTED == 0) & (rawData.CANCELLED == 0)]

    # this is how much percent have been cleaned away!
    cleaned_away = filteredData.count()[0]

    # remove columns that are not needed for the model
    filteredData = filterDF(filteredData, columnsToUse)
    filteredData.reset_index(inplace=True)

    # perform as next step join to amend information by aircraftdata
    delayFinal = filteredData[['TAIL_NUM','UNIQUE_CARRIER']]
    delayFinal.TAIL_NUM = delayFinal.TAIL_NUM.str.strip('N')
    delaymfr = pd.merge(delayFinal, joinedAircraftInfo, how='left', on=['TAIL_NUM'])
    filteredData.TAIL_NUM = delaymfr.TAIL_NUM
    filteredData['AIRCRAFT_YEAR'] = delaymfr.YEAR
    filteredData['AIRCRAFT_MFR'] = delaymfr.MFR

    # get rid of NAN values
    filteredData.dropna(axis = 0, inplace = True)

    # get rid of empty year values
    filteredData = filteredData[filteredData['AIRCRAFT_YEAR'] != '    ']

    # compute age of aircraft
    filteredData['AIRCRAFT_AGE'] = filteredData.YEAR.astype(int) - filteredData.AIRCRAFT_YEAR.astype(int)

    # now, compress manufacturers to only a small amount of companies
    filteredData = compressManufacturers(filteredData)

    cleaned_away = 1. - filteredData.count()[0] * 1. / cleaned_away
    return filteredData, cleaned_away


# In[ ]:

# the dataframe to store everything in
bigdf = None
ca_statistic = []

years = ['2010', '2011', '2012', '2013', '2014']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
print('starting processing')
for y in years:
    for m in months:
        
        print 'reading {y}{mo}.zip'.format(y=y, mo = m)
        z = zipfile.ZipFile('../cache/{y}{mo}.zip'.format(y=y, mo = m))
        rawData = pd.read_csv(z.open(z.namelist()[0]), low_memory=False)

        print 'processing {y}{mo}.zip'.format(y=y, mo = m)
        df, ca = processData(rawData)
        if bigdf is None:
            bigdf = df
        else:
            bigdf = bigdf.append(df, ignore_index=True)
        ca_statistic.append(('{y}{mo}.zip'.format(y=y, mo = m), ca))
        print '==> cleaned away {pc}%'.format(pc=ca)
        print '==> added entries: {ne}'.format(ne=df.count()[0])
        
# save to csv
bigdf.to_csv('../cache/Big5FlightTable.csv')

