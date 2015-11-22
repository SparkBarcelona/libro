import org.apache.spark.rdd._
import org.apache.spark.graphx._

// Nodos del grafo
val dataAparcamientos = Array((1L, "A"),
	(2L, "B"),
	(3L, "C")
)

val aparcamientos : VertexRDD[String] = VertexRDD(sc.parallelize(dataAparcamientos))

// Ejes que relacionan los nodos
val dataRelaciones = Array(
	Edge(1L, 2L, 500),
	Edge(2L, 1L, 700),
	Edge(2L, 3L, 200),
	Edge(3L, 2L, 150),
	Edge(1L, 3L, 900)
)

val relaciones : EdgeRDD[Int] = EdgeRDD.fromEdges(sc.parallelize(dataRelaciones))

// Grafo con nodos y relaciones
val grafo = Graph(aparcamientos, relaciones)
