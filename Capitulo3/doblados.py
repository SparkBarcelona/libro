 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

orig = sc.parallelize([1,2,3,4,5,6,7,8])
doble = orig.map(lambda x : x * 2)
print ("La suma de los elementos doblados es %d" % doble.sum())

menor6 = doble.filter(lambda x : x < 6)
print ("Hay %d elementos doblados menores que 6" % menor6.count())
