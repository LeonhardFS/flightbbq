# Flightbbq

This is CS109 Final Project for flight delay prediction. See <http://leonhardfs.github.io/flightbbq/> with information about our project! 

[![Why Is My Flight Delayed?](https://i.ytimg.com/vi/o10D6I7I7IA/hqdefault.jpg)](https://www.youtube.com/embed/o10D6I7I7IA?autoplay=1 "Why Is My Flight Delayed?")

#### Downloading the data

In order to download the zipped data from <http://www.transtats.bts.gov/>, several scripts are stored in the src folder. I.e. run 

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

#### Process notebook
The process notebook has been divided into four parts which are accessible from the root directory. Computational heavy scripts reside in the `src` directory. Furthermore, data has been included in the repo for easier access. In `results` two fitted linear regression models over ~3M, ~15M entries respectively are located.

---
(c) 2015 Granet, Middelbeck, Lei, Spiegelberg
