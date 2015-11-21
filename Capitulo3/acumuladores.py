 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam

sc = SparkContext("local", "Simple App")

class MultiplicadorAccum(AccumulatorParam):
	def zero(self, initialValue):
		return 1
	def addInPlace(self, v1, v2):
		return v1*v2


acc = sc.accumulator(1,MultiplicadorAccum())
sc.parallelize([1,2,3,4,5,5,6,7,7]).foreach(lambda x: acc.add(x))

print("value %d " % acc.value)
