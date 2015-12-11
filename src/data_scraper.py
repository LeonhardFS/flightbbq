# (c) 2015 FlightBBQ Team
# contains helper functions to scrape data from transtats.gov

import datetime
from os import rename
from os.path import splitext
from calendar import month_name
from httplib import HTTPConnection
from time import clock
from urllib import urlretrieve
from zipfile import ZipFile
import numpy as np

import os
import errno
import humanize
import sys
import json

# huge post request for page
POST = """
UserTableName=On_Time_Performance&DBShortName=On_Time&RawDataTable=T_ONTIME&
sqlstr=+SELECT+YEAR,QUARTER,MONTH,DAY_OF_MONTH,DAY_OF_WEEK,FL_DATE,UNIQUE_CARRIER,
AIRLINE_ID,CARRIER,TAIL_NUM,FL_NUM,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,ORIGIN_STATE_FIPS,
ORIGIN_STATE_NM,ORIGIN_WAC,DEST,DEST_CITY_NAME,DEST_STATE_ABR,DEST_STATE_FIPS,
DEST_STATE_NM,DEST_WAC,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,DEP_DELAY_NEW,DEP_DEL15,
DEP_DELAY_GROUP,DEP_TIME_BLK,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,
ARR_DELAY,ARR_DELAY_NEW,ARR_DEL15,ARR_DELAY_GROUP,ARR_TIME_BLK,CANCELLED,CANCELLATION_CODE,
DIVERTED,CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,FLIGHTS,DISTANCE,DISTANCE_GROUP,
CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY,FIRST_DEP_TIME,
TOTAL_ADD_GTIME,LONGEST_ADD_GTIME,DIV_AIRPORT_LANDINGS,DIV_REACHED_DEST,DIV_ACTUAL_ELAPSED_TIME,
DIV_ARR_DELAY,DIV_DISTANCE,DIV1_AIRPORT,DIV1_WHEELS_ON,DIV1_TOTAL_GTIME,DIV1_LONGEST_GTIME,
DIV1_WHEELS_OFF,DIV1_TAIL_NUM,DIV2_AIRPORT,DIV2_WHEELS_ON,DIV2_TOTAL_GTIME,DIV2_LONGEST_GTIME,
DIV2_WHEELS_OFF,DIV2_TAIL_NUM,DIV3_AIRPORT,DIV3_WHEELS_ON,DIV3_TOTAL_GTIME,DIV3_LONGEST_GTIME,
DIV3_WHEELS_OFF,DIV3_TAIL_NUM,DIV4_AIRPORT,DIV4_WHEELS_ON,DIV4_TOTAL_GTIME,DIV4_LONGEST_GTIME,
DIV4_WHEELS_OFF,DIV4_TAIL_NUM,DIV5_AIRPORT,DIV5_WHEELS_ON,DIV5_TOTAL_GTIME,DIV5_LONGEST_GTIME,
DIV5_WHEELS_OFF,DIV5_TAIL_NUM+FROM++T_ONTIME+WHERE+
Month+={frequency}+AND+YEAR={year}&
varlist=YEAR,QUARTER,MONTH,DAY_OF_MONTH,DAY_OF_WEEK,FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,
TAIL_NUM,FL_NUM,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,ORIGIN_STATE_FIPS,ORIGIN_STATE_NM,
ORIGIN_WAC,DEST,DEST_CITY_NAME,DEST_STATE_ABR,DEST_STATE_FIPS,DEST_STATE_NM,DEST_WAC,CRS_DEP_TIME,
DEP_TIME,DEP_DELAY,DEP_DELAY_NEW,DEP_DEL15,DEP_DELAY_GROUP,DEP_TIME_BLK,TAXI_OUT,WHEELS_OFF,WHEELS_ON,
TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,ARR_DELAY_NEW,ARR_DEL15,ARR_DELAY_GROUP,ARR_TIME_BLK,CANCELLED,
CANCELLATION_CODE,DIVERTED,CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,FLIGHTS,DISTANCE,DISTANCE_GROUP,
CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY,FIRST_DEP_TIME,TOTAL_ADD_GTIME,
LONGEST_ADD_GTIME,DIV_AIRPORT_LANDINGS,DIV_REACHED_DEST,DIV_ACTUAL_ELAPSED_TIME,DIV_ARR_DELAY,DIV_DISTANCE,
DIV1_AIRPORT,DIV1_WHEELS_ON,DIV1_TOTAL_GTIME,DIV1_LONGEST_GTIME,DIV1_WHEELS_OFF,DIV1_TAIL_NUM,DIV2_AIRPORT,
DIV2_WHEELS_ON,DIV2_TOTAL_GTIME,DIV2_LONGEST_GTIME,DIV2_WHEELS_OFF,DIV2_TAIL_NUM,DIV3_AIRPORT,
DIV3_WHEELS_ON,DIV3_TOTAL_GTIME,DIV3_LONGEST_GTIME,DIV3_WHEELS_OFF,DIV3_TAIL_NUM,DIV4_AIRPORT,
DIV4_WHEELS_ON,DIV4_TOTAL_GTIME,DIV4_LONGEST_GTIME,DIV4_WHEELS_OFF,DIV4_TAIL_NUM,DIV5_AIRPORT,
DIV5_WHEELS_ON,DIV5_TOTAL_GTIME,DIV5_LONGEST_GTIME,DIV5_WHEELS_OFF,DIV5_TAIL_NUM&
grouplist=&suml=&sumRegion=&filter1=title=&filter2=title=&
geo=All&time={month}&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY={frequency}&
VarName=YEAR&VarDesc=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName=MONTH&
VarDesc=Month&VarType=Num&VarName=DAY_OF_MONTH&VarDesc=DayofMonth&VarType=Num&VarName=DAY_OF_WEEK&
VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=UNIQUE_CARRIER&
VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=CARRIER&
VarDesc=Carrier&VarType=Char&VarName=TAIL_NUM&VarDesc=TailNum&VarType=Char&VarName=FL_NUM&VarDesc=FlightNum&
VarType=Char&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_CITY_NAME&VarDesc=OriginCityName&
VarType=Char&VarName=ORIGIN_STATE_ABR&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_FIPS&
VarDesc=OriginStateFips&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&
VarType=Char&VarName=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST&VarDesc=Dest&
VarType=Char&VarName=DEST_CITY_NAME&VarDesc=DestCityName&VarType=Char&VarName=DEST_STATE_ABR&
VarDesc=DestState&VarType=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips&VarType=Char&
VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_WAC&VarDesc=DestWac&
VarType=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&
VarType=Char&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarName=DEP_DELAY_NEW&VarDesc=DepDelayMinutes&
VarType=Num&VarName=DEP_DEL15&VarDesc=DepDel15&VarType=Num&VarName=DEP_DELAY_GROUP&VarDesc=DepartureDelayGroups&
VarType=Num&VarName=DEP_TIME_BLK&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType=Num&
VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char&VarName=TAXI_IN
&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName=ARR_TIME&
VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarName=ARR_DELAY_NEW&
VarDesc=ArrDelayMinutes&VarType=Num&VarName=ARR_DEL15&VarDesc=ArrDel15&VarType=Num&VarName=ARR_DELAY_GROUP&
VarDesc=ArrivalDelayGroups&VarType=Num&VarName=ARR_TIME_BLK&VarDesc=ArrTimeBlk&VarType=Char&
VarName=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&
VarType=Char&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarName=CRS_ELAPSED_TIME&VarDesc=CRSElapsedTime&
VarType=Num&VarName=ACTUAL_ELAPSED_TIME&VarDesc=ActualElapsedTime&VarType=Num&VarName=AIR_TIME&
VarDesc=AirTime&VarType=Num&VarName=FLIGHTS&VarDesc=Flights&VarType=Num&VarName=DISTANCE&
VarDesc=Distance&VarType=Num&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&
VarName=CARRIER_DELAY&VarDesc=CarrierDelay&VarType=Num&VarName=WEATHER_DELAY&VarDesc=WeatherDelay&
VarType=Num&VarName=NAS_DELAY&VarDesc=NASDelay&VarType=Num&VarName=SECURITY_DELAY&
VarDesc=SecurityDelay&VarType=Num&VarName=LATE_AIRCRAFT_DELAY&VarDesc=LateAircraftDelay&
VarType=Num&VarName=FIRST_DEP_TIME&VarDesc=FirstDepTime&VarType=Char&VarName=TOTAL_ADD_GTIME&
VarDesc=TotalAddGTime&VarType=Num&VarName=LONGEST_ADD_GTIME&VarDesc=LongestAddGTime&VarType=Num&
VarName=DIV_AIRPORT_LANDINGS&VarDesc=DivAirportLandings&VarType=Num&VarName=DIV_REACHED_DEST&
VarDesc=DivReachedDest&VarType=Num&VarName=DIV_ACTUAL_ELAPSED_TIME&VarDesc=DivActualElapsedTime&
VarType=Num&VarName=DIV_ARR_DELAY&VarDesc=DivArrDelay&VarType=Num&VarName=DIV_DISTANCE&
VarDesc=DivDistance&VarType=Num&VarName=DIV1_AIRPORT&VarDesc=Div1Airport&VarType=Char&
VarName=DIV1_WHEELS_ON&VarDesc=Div1WheelsOn&VarType=Char&VarName=DIV1_TOTAL_GTIME&VarDesc=Div1TotalGTime&
VarType=Num&VarName=DIV1_LONGEST_GTIME&VarDesc=Div1LongestGTime&VarType=Num&VarName=DIV1_WHEELS_OFF&
VarDesc=Div1WheelsOff&VarType=Char&VarName=DIV1_TAIL_NUM&VarDesc=Div1TailNum&VarType=Char&
VarName=DIV2_AIRPORT&VarDesc=Div2Airport&VarType=Char&VarName=DIV2_WHEELS_ON&VarDesc=Div2WheelsOn&
VarType=Char&VarName=DIV2_TOTAL_GTIME&VarDesc=Div2TotalGTime&VarType=Num&VarName=DIV2_LONGEST_GTIME&
VarDesc=Div2LongestGTime&VarType=Num&VarName=DIV2_WHEELS_OFF&VarDesc=Div2WheelsOff&VarType=Char&
VarName=DIV2_TAIL_NUM&VarDesc=Div2TailNum&VarType=Char&VarName=DIV3_AIRPORT&VarDesc=Div3Airport&
VarType=Char&VarName=DIV3_WHEELS_ON&VarDesc=Div3WheelsOn&VarType=Char&VarName=DIV3_TOTAL_GTIME&
VarDesc=Div3TotalGTime&VarType=Num&VarName=DIV3_LONGEST_GTIME&VarDesc=Div3LongestGTime&VarType=Num&
VarName=DIV3_WHEELS_OFF&VarDesc=Div3WheelsOff&VarType=Char&VarName=DIV3_TAIL_NUM&VarDesc=Div3TailNum&
VarType=Char&VarName=DIV4_AIRPORT&VarDesc=Div4Airport&VarType=Char&VarName=DIV4_WHEELS_ON&
VarDesc=Div4WheelsOn&VarType=Char&VarName=DIV4_TOTAL_GTIME&VarDesc=Div4TotalGTime&VarType=Num&
VarName=DIV4_LONGEST_GTIME&VarDesc=Div4LongestGTime&VarType=Num&VarName=DIV4_WHEELS_OFF&VarDesc=Div4WheelsOff&
VarType=Char&VarName=DIV4_TAIL_NUM&VarDesc=Div4TailNum&VarType=Char&VarName=DIV5_AIRPORT&VarDesc=Div5Airport&
VarType=Char&VarName=DIV5_WHEELS_ON&VarDesc=Div5WheelsOn&VarType=Char&VarName=DIV5_TOTAL_GTIME&VarDesc=Div5TotalGTime&
VarType=Num&VarName=DIV5_LONGEST_GTIME&VarDesc=Div5LongestGTime&VarType=Num&VarName=DIV5_WHEELS_OFF&
VarDesc=Div5WheelsOff&VarType=Char&VarName=DIV5_TAIL_NUM&VarDesc=Div5TailNum&VarType=Char"""

# create dir if it does not exists already


def create_dir(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as error:
            if error.errno != errno.EEXIST:
            	raise

# for a given url, this function downloads the file at url to
# cache/filename
def download_file(file_url, cache_path, filename):
    print('downloading %s...' % file_url)

    # show progress in percent & bytes
    def progressHook(num_blocks, block_size, total_size):
        percent = (num_blocks * block_size / (1.0 * total_size)) * 100.0
        sizeH = humanize.naturalsize(num_blocks * block_size, gnu=True)
        total_sizeH = humanize.naturalsize(total_size, gnu=True)
        sys.stdout.write('\r%s / %s \t\t%.2f %%' %
                         (sizeH, total_sizeH, percent))
        sys.stdout.flush()
    # perform download, store result in cache/filename
    print('')
    local_file = urlretrieve(file_url, os.path.join(
        cache_path, filename), progressHook)
    print('')

# retrieves file url for given month / year
def get_file_url(year, month):

	# set url variables
	host = 'www.transtats.bts.gov'
	transtat_url = 'http://www.transtats.bts.gov/DownLoad_Table.asp?' \
	'Table_ID=236&Has_Group=3&Is_Zipped=0'
	frequency = month

    # setup POST string
	post = POST.format(year=year, month=month_name[month], frequency=frequency)

	# remove any eol chars
	post = post.replace('\n', '')

	# create the user agent string, and emulate a browser
	user_agent_string = {
	    "Content-Type": "application/x-www-form-urlencoded",
	}

	# perform post request
	request = HTTPConnection(host)

	print "sending request for %d, %s..." % (year, month_name[month])
	request.request("POST", transtat_url, post, user_agent_string)

	print "retrieving response for POST request..."
	response = request.getresponse()

	if not response.status == 302:
	    raise Exception("Request was not successful with response {}".format(
	        response.status
	    ))

	# get file url (stored in location) from header
	return response.getheader('location')

# get the urls where the zipped data is stored
def get_file_urls(cache_path, years=np.arange(2014, 2015), months=np.arange(1, 13)):
    # by default use 25 years, 12 months

    urldict = {}
    for year in years:
        for month in months:
            key = '%d%02d' % (year, month)
            urldict[key] = get_file_url(year, month)
            print(urldict[key])

    # store result in json
    url_dict_file = 'url_dict.json'
    with open(os.path.join(cache_path, url_dict_file), 'wb') as outfile:
        json.dump(urldict, outfile)

# download files
# dates list of tuples (year, month)
def download_zip_files(dates, urldict, cache_path):

    print('downloading %d files...' % len(dates))

    # download data for all given tuples
    cnt = 1
    for t in dates:
        print('file %d / %d:' % (cnt, len(dates)))
        cnt += 1
        key = '%d%02d' % (t[0], t[1])
        try:
            url = urldict[key]
            download_file(url, cache_path, key + '.zip')
        except KeyError:
            print('KeyError, no url for %s, %s found' %
                  (t[0], month_name[t[1]]))


# helper function to get tuples for a year or many
def get_year_tuples(year):
    # check if list or single element
    if type(year) is list:
        # do for multiple years
        L = []
        for y in year:
            L += get_year_tuples(y)

        return L
    else:
        months = np.arange(1, 13)
        years = [year] * 12

        return zip(years, months)

# checks if file exists and is readable
def file_exists(path):
    return os.path.isfile(path) and os.access(path, os.R_OK)
