 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
import csv
import StringIO

sc = SparkContext("local", "Simple App")

def parseLine(line):
	input = StringIO.StringIO(line)
	dictReader = csv.DictReader(line,
		fieldnames=["cdatetime", "address",
			"latitude", "longitude"])
	return dictReader.next()

data = (sc.textFile("SacramentoCrimes.csv")
	.zipWithIndex()
	.filter(lambda x: x[1] > 0)
	.map(parseLine))

print(data.take(7))
