import wget
import csv

from StrinIO import StringIO 
from collections import namedtuple
from pyspark import SparkContext

#url = 'https://data.cityofnewyork.us/api/views/k384-xu3q/rows.csv?accessType=DOWNLOAD'
#file = wget.download(url)

# path/to/file
file="Desktop/dataset/ny-crime-rows.csv"

sc = SparkContext("local", "NY Crime App")
data = sc.textFile(file).cache()

# data.take(10)

# Filter the header row
header = data.first()

# Data without header row
dataWoHeader = data.filter(lambda x: x<>header)
dataWoHeader = dataWoHeader.map(lambda x:x.encode("utf-8"))

# replace empty fields
#dataWoHeader = dataWoHeader.map(lambda x: re.sub(r"(?<=,)(?=,)", "-", x))

#dataWoHeader.map(lambda x:x.split(',')).take(10)

fields = header.replace("(","_").replace(")","_").replace(" ","_").replace("/","_").split(",")

# remove row with incorrect len
dataWoHeaderCorrect = dataWoHeader.filter(lambda x:len(x.split(","))==len(fields)+1)
dataWoHeaderIncorrect = dataWoHeader.filter(lambda x:len(x.split(","))!=len(fields)+1)

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
crimeFiltered = crime.filter(lambda x: not (x.Offense=="NA" or x.Occurence_Years==''))\
			.filter(lambda x: int(x.Occurrence_Years)>=2016)

#crime.map(lambda x:x.Occurrunce_Years).countByValue()

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
min_lat = -74,2589
min_lng = 40,4774
max_lat = -73,7004
max_lng = 40,9176

crimeFinal = crimeFiltered.filter(lambda x:extractLocation(x.Location_1)[0] >= min_lng and \
					 x.extractLocation(x.Location_1)[0] <= max_lng and \
					 x.extractLocation(x.Location_1)[1] >= min_lat and \
					 x.extractLocation(x.Location_1)[1] <= max_lat)


