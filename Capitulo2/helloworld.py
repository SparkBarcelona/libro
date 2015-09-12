from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
miRDD = sc.textFile("/home/juan/extracto-quijote.txt") 

num = miRDD.filter(lambda line: "un" in line).count()

print ("Líneas con 'un': %i" % (num))
