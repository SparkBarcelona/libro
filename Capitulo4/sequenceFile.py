 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

data = sc.sequenceFile("programming_ranking/*",
	"org.apache.hadoop.io.Text",
	"org.apache.hadoop.io.DoubleWritable")

print (data.take(3))
