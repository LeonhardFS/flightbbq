
# coding: utf-8

# In[4]:

import datetime
from os import rename
from os.path import splitext
from calendar import month_name
from httplib import HTTPConnection
from time import clock
from urllib import urlretrieve
import urllib2
from zipfile import ZipFile
import numpy as np
from bs4 import BeautifulSoup

from data_scraper import *

import os
import errno
import humanize
import sys
import json
import pandas as pd


# In[5]:

def parseHistoryTable(table):
    temperature = 0 # in degree F
    events = '' # some string i.e. rain
    humidity = 0 # humidity in %
    precipitation = 0 # in inch
    sealevelpressure = 0 # in inch
    visibility = 0 # in miles
    windspeed = 0 # in miles per hour

    # skip the first entries of some fields...
    skipprec = True
    skippres = True

    # Find all the <tr> tag pairs, skip the first one, then for each.
    for row in table.find_all('tr')[1:]:
        # Create a variable of all the <td> tag pairs in each <tr> tag pair,
        col = row.find_all('td')

        # only retrieve relevant columns
        if col[0].text == 'Mean Temperature':
            try:
                temperature = int(col[1].find('span', attrs={"class": "wx-value"}).text)
            except:
                temperature = 9999
        if col[0].text == 'Average Humidity':
            try:
                humidity = int(col[1].text)
            except:
                humidity = -1
        if col[0].text == 'Precipitation':
            if not skipprec:
                try:
                    precipitation = float(col[1].find('span', attrs={"class": "wx-value"}).text)
                except:
                    precipitation = -1.
            skipprec = False
        if col[0].text == 'Sea Level Pressure':
            if not skippres:
                try:
                    sealevelpressure = float(col[1].find('span', attrs={"class": "wx-value"}).text)
                except:
                    sealevelpressure = -1.
            skippres = False
        if col[0].text == 'Visibility':
            try:
                visibility = float(col[1].find('span', attrs={"class": "wx-value"}).text)
            except:
                visibility = -1.
        if col[0].text == 'Wind Speed':
            try:
                windspeed = float(col[1].find('span', attrs={"class": "wx-value"}).text)
            except:
                windspeed = -1.
        if col[0].text == 'Events':
            try:
                events = col[1].text
            except:
                events = '?'
    # return as dictionary
    d = dict(zip(['temperature', 'events', 'humidity', 'precipitation', 'sealevelpressure', 'visibility', 'windspeed'],     [temperature, events, humidity, precipitation, sealevelpressure, visibility, windspeed]))
    
    return d

def getWeather(year, month, day, airportcode):
    url = 'http://www.wunderground.com/cgi-bin/findweather/getForecast?airportorwmo=query&historytype=DailyHistory&backurl=%2Fhistory%2Findex.html&code={airportcode}&month={month}&day={day}&year={year}'
    response = urllib2.urlopen(url.format(year=year, day=day, month=month, airportcode=airportcode))
    html = response.read()
    
    soup = BeautifulSoup(html, "html.parser")
    
    table = soup.find("table", attrs={"id": "historyTable"})
    
    return parseHistoryTable(table)


# In[6]:

year = 2015
day = 3
month = 10
airportcode = 'FRA'

getWeather(2015, 3, 10, airportcode)


# In[7]:

# lazy load dictionary
weatherDict = {}
weatherFile = os.path.join('..', 'cache', 'weather_data.json')
if file_exists(weatherFile):
    # load current dict from json to cache results
    with open(weatherFile) as infile:
        weatherDict = json.load(infile)

# read airport list
dfairports = pd.read_csv(os.path.join('..', 'data', 'airports.csv'), header=None)


# In[29]:

from datetime import timedelta, date

# take 2014
year = 2014

# iterate over all airports
pos = 1
for key, item in dfairports.iterrows():
    airport = item.values[0]
    
    print 'processing %s (%d/%d)...' % (airport, pos, dfairports.count())
    pos += 1
                                     
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    d = start_date
    delta = datetime.timedelta(days=1)

    # iterate over one year to get the data from it
    rows = []
    date_keys = []
    
    # check if for airport data exists already
    if airport in weatherDict.keys():
        rows = weatherDict[airport]
        date_keys = [el['date'] for el in rows]
    
    while d <= end_date:
        key = '%04d%02d%02d' % (d.year, d.month, d.day)
        
        # data already requested? --> skip!
        if not key in date_keys:
            print 'GET %s' % key
            rows.append(dict([('data', getWeather(d.year, d.month, d.day, airport)), ('date', key)]))
        d += delta
    weatherDict[airport] = rows
    
    # save current JSON!
    with open(weatherFile, 'wb') as outfile:
        json.dump(weatherDict, outfile)
        
# save JSON!
with open(weatherFile, 'wb') as outfile:
    json.dump(weatherDict, outfile)

