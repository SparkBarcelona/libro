 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
from numpy import array
from pyspark.mllib.clustering import KMeans
from math import sqrt

sc = SparkContext("local", "Simple App")

def show (x): print(x)

data = sc.textFile("ejemplo.txt")
data.foreach(show)

parsedData = data.map(
		lambda line : array([float(x) for x in line.split(' ')])
	).cache()
parsedData.foreach(show)

clusters = KMeans.train(parsedData, 2, maxIterations=10, runs=10, initializationMode="random")

def error(point):
	center = clusters.centers[clusters.predict(point)]
	return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point:error(point)).reduce(lambda x, y: x + y)

print("Within Set Sum of Squared Error = " + str(WSSSE))
