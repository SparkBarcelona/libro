 #!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions

sc = SparkContext("local", "Simple App")
sqlCtx = SQLContext(sc)

estud = sqlCtx.read.json("estudiantes.json")
estud.registerTempTable("Estudiantes")
notables = sqlCtx.sql("""
		SELECT nombre, apellidos, nota
		FROM Estudiantes
		WHERE nota >= 8
		ORDER BY apellidos ASC
	""")

print("Notables: ")
print(notables.show())

## Funciones definidas por el usuario
def notatxt(nota):
	if nota < 5:
		return "suspenso"
	if nota < 6.5:
		return "aprobado"
	if nota < 9:
		return "notable"
	if nota < 9.9:
		return "excelente"
	return "matrícula"

sqlCtx.registerFunction("notatxt",notatxt)
publicada = sqlCtx.sql("""
		SELECT apellidos,nombre,notatxt(nota) AS Expediente
		FROM Estudiantes
		ORDER BY apellidos
	""")

print("Notas txt: ")
print(publicada.show())
