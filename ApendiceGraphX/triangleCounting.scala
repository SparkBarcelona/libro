import org.apache.spark.graphx._

val grafo = GraphLoader.
		edgeListFile(sc, "relaciones.txt", true).
		partitionBy(PartitionStrategy.RandomVertexCut)

// Ejecución de TriangleCounting sobre el grafo
val triCount = grafo.triangleCount().vertices

// Carga de usuarios
val usuarios = sc.textFile("usuarios.txt").
	map{ usu =>
		val usu_split = usu.split(",")
		(usu_split(0).toLong, usu_split(2))
	}

// Obtención del número de triángulos al que pertenece cada usuario
val triUsu = usuarios.join(triCount).
	map{
		case(id, (nombre, tC)) => (nombre, tC)
	}

triUsu.take(7)
