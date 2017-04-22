"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "/usr/local/spark/README.md"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

out_file = open("/home/ambpl/Desktop/SimpleApp-output.txt","w")
out_file.write("Lines with a: %i, lines with b: %i" % (numAs, numBs))
out_file.close()

#print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
