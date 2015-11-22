import org.apache.spark.graphx._

// Creamos el grafo a partir de las relaciones
val grafo = GraphLoader.edgeListFile(sc, "relaciones.txt")

// Ejecutamos PageRank sobre el grafo
val rank = grafo.pageRank(0.001).vertices

// Cargamos los usuarios
val usuarios = sc.textFile("usuarios.txt").
	map{ usu =>
		val usu_split = usu.split(",")
		(usu_split(0).toLong, usu_split(2))
	}

// Obtenemos la lista de usuarios y su ranking
val listaRanks = usuarios.join(rank).
	map{
		case (id, (nombre, rank)) => (nombre, rank)
	}.sortBy(r => r._2, false)

listaRanks.take(7)
