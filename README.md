# flightbbq

see <http://leonhardfs.github.io/flightbbq/> with information about our project! 

#### Downloading the data

In order to download the zipped data from <http://www.transtats.bts.gov/>, run 

```
python download_data.py
```

10 years of data will be roughly ~3GB with each month being around 22.1MB large zipped.
To download individual months pass a list of tuples `(year, month)` to `download_zip_files`

```
dates = [(2014, 1), (2014, 2)]
download_zip_files(dates, urldict, cache_path)
```

As retrieving the file urls takes some time, they are preparsed into a dict where keys adressing year/month are encoded as "201402" for the file url of 2014/02.