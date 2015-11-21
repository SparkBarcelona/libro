 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
miRDD = sc.textFile("extracto-quijote.txt") 

num = miRDD.filter(lambda line: "un" in line).count()

print ("Líneas con 'un': %i" % (num))
