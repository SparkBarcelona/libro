 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
import re

sc = SparkContext("local", "Simple App")

qui = sc.textFile("quijote.txt")
palabras = (qui
	.flatMap(lambda linea: re.compile("\W").split(linea))
	.filter(lambda palabra: palabra != '')
	.map(lambda palabra: palabra.lower()))

histograma = (palabras
	.map(lambda palabra : (palabra,1))
	.reduceByKey(lambda x,y: x+y))

masFrecuentes = (histograma
	.sortBy(lambda v: -v[1]).take(15))

for p in masFrecuentes:
	print (p[0] + ": %d" % p[1])

