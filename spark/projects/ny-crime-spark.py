import wget
import csv
import re

from StrinIO import StringIO 
from collections import namedtuple
from pyspark import SparkContext

#url = 'https://data.cityofnewyork.us/api/views/k384-xu3q/rows.csv?accessType=DOWNLOAD'
#file = wget.download(url)

# path/to/file
file="Desktop/dataset/ny-crime-rows.csv"

sc = SparkContext("local", "NY Crime App")
data = sc.textFile(file).map(lambda x:x.encode("utf-8")) #.cache()

# data.take(10)

# Filter the header row
header = data.first()

# Data without header row
dataWoHeader = data.filter(lambda x: x<>header)

# replace empty fields
#dataWoHeader = dataWoHeader.map(lambda x: re.sub(r"(?<=,)(?=,)", "-", x))

#dataWoHeader.map(lambda x:x.split(',')).take(10)

fields = header.replace("(","_").replace(")","_").replace(" ","_").replace("/","_").split(",")

fieldsNumber=len(header.split(","))

# remove row with incorrect len
dataWoHeaderCorrect = dataWoHeader.filter(lambda x:len(x.split(","))==fieldsNumber+1)
dataWoHeaderIncorrect = dataWoHeader.filter(lambda x:len(x.split(","))!=fieldsNumber+1)

# Crime class
Crime = namedtuple("Crime", fields, verbose=True)

# Parse function return istance of Crime class from csv row
def parse(row):
	reader = csv.reader(StringIO(row))
	row = reader.next()
	return Crime(*row)

# structured RDD object
crimes = dataWoHeaderCorrect.map(parse)

#crimes.first().City
#crimes.map(lambda x:x.City).countByValue()
#crimes.map(lambda x:x.Created_Date).countByValue()

#more realistic data 
#crimeFiltered = crime.filter(lambda x: not (x.Offense=="NA" or x.Occurence_Years==''))\
#			.filter(lambda x: int(x.Occurrence_Years)>=2016)

crimeFilteredLocation = crimes.filter(lambda x: not (x.Location=''))

# exctract coordinates from row
def extractCoords(location):
	location_lat = float(location[1:location.index(",")])
	location_lng = float(location[location.index(",")+1:-1])
	return (location_lat, location_lng)

# minimum latitude and longitude from dataset
#crimeFiltered.map(lambda x:extractCoords(x.Location_1))\
#		.reduce(lambda x,y: min(x[0],y[0]), min(x[1],y[1]))

# maximum latitude and longitude from dataset
#crimeFiltered.map(lambda x:extractCoords(x.Location_1))\
#                .reduce(lambda x,y: max(x[0],y[0]), max(x[1],y[1]))


# New York
# - bounding box: -74,2589, 40,4774, -73,7004, 40,9176
# - source: https://www.flickr.com/places/info/2459115
min_lng = -74.2589
min_lat = 40.4774
max_lng = -73.7004
max_lat = 40.9176

crimeFilteredNyLocation= crimeFilteredLocation.\
				filter(lambda x:extractCoords(x.Location)[0] >= min_lat and \
						extractCoords(x.Location)[0] <= max_lat and \
						extractCoords(x.Location)[1] >= min_lng and \
						extractCoords(x.Location)[1] <= max_lng)

#regex = re.compile(r'^[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] (AM|PM)$')
regex = re.compile(r'^[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]')
crimeFilteredDate = crimeFilteredNyLocation.filter(lambda x: regex.search(x.Created_Date))

crimeFinal = CrimeFilteredDate
#crimeFilteredDate.map(lambda x : re.search('[0-9][0-9][0-9][0-9]', x.Created_Date).group()).countByValue()


# sudo pip install gmplot
import gmplot

map_lat = 37.428
map_lng = -122.145

gmap = gmplot.GoogpleMapPlotter(map_lat,map_lng,16).from_geocode("New York City")

b_lats = crimeFinal.filter(lambda x : re.search('[0-9][0-9][0-9][0-9]', x.Created_Date).group()=="2017")\
					.map(lambda x: extractCoords(x.Location)[0]).collect()

b_lngs = crimeFinal.filter(lambda x : re.search('[0-9][0-9][0-9][0-9]', x.Created_Date).group()=="2017")\
                                        .map(lambda x: extractCoords(x.Location)[1]).collect()

#draw map
gmap.scatter(b_lats, b_lngs, '#DE1515', size=40, marker=False)
gmap.draw("ny-crime-map.html")
