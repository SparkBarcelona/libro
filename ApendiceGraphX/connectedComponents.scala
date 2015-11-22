import org.apache.spark.graphx._

// Creamos el grafo a partir de las relaciones
val grafo = GraphLoader.edgeListFile(sc, "relaciones.txt")

// Ejecutamos ConnectedComponents sobre el grafo
val connectedComponents = grafo.connectedComponents().vertices

// Cargamos los usuarios
val usuarios = sc.textFile("usuarios.txt").
	map{ usu =>
		val usu_split = usu.split(",")
		(usu_split(0).toLong, usu_split(2))
	}

// Obtenemos el ID del cluster al que esta´ asociado cada usuario
val clustUser = usuarios.join(connectedComponents).
	map{
		case(id, (nombre, clstr)) => (nombre, clstr)
	}

clustUser.take(7)
