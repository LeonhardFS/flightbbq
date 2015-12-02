# (c) 2015 FlightBBQ Team
# use this script to download the zipped data

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

from data_scraper import *

def main():
	# first create cache directory if it does not exist
	cache_path = os.path.join('..', 'cache')
	create_dir(cache_path)

	# check if the dictionary for file urls exists
	url_dict_file = 'url_dict.json'
	if not file_exists(os.path.join(cache_path, url_dict_file)):
	    # will retrieve urls for 25 years (change here if necessary)
	    get_file_urls(cache_path)
	    
	# read urldict
	urldict = {}
	with open(os.path.join(cache_path, url_dict_file)) as infile:
	     urldict = json.load(infile)

	# download 10 years of data as zip files
	dates = get_year_tuples(list(np.arange(2004, 2015)))
	download_zip_files(dates, urldict, cache_path)

if __name__ == "__main__":
    main()