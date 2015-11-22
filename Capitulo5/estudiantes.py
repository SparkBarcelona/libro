 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions


sc = SparkContext("local", "Simple App")
sqlCtx = SQLContext(sc)

estud = sqlCtx.read.json("estudiantes.json",
	StructType([
		StructField("_id",StringType(),False),
		StructField("nombre",StringType(),False),
		StructField("apellidos",StringType(),False),
		StructField("edad",ByteType(),False),
		StructField("email",StringType(),False),
		StructField("nota",DecimalType(),False)]))

estadistica = estud.agg(
	functions.min(estud.edad),
	functions.max(estud.edad),
	functions.avg(estud.nota))

## Estadística
print(estadistica.collect())

## GroupBy
print(estud.groupBy('nombre').avg().show())

## Select
print( (estud.select('apellidos','nombre')
	.orderBy(estud.apellidos.asc())
	.show()) )
