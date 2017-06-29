package net.sansa_stack.template.spark.graphOps

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.mutable.ListBuffer
import net.sansa_stack.template.spark.reader.TripleUtils
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object GraphOps {
  def main(args: Array[String]) = {
    val input = "src/main/resources/rdf.nt"
    val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
    val tripleRDD = spark.sparkContext.textFile(input).map(TripleUtils.parsTriples)
    val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
    type VertexId = Long
    val seq = new ListBuffer[VertexId]()
    val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
    val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
      {
        case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
      })
    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
    vertices.collect().foreach { case (a, b) => seq += a }

    val graph = Graph(vertices, edges)
    //val gra = ShortestPaths.run(graph, seq.toList);
    for (a <- seq) {

      val allpath = Allpaths.runPregel(a, 654L, graph, EdgeDirection.Either)
      if (allpath.length != 0) {
        print("Number of paths between 654L " + a + " is", allpath.length)
        for (a <- allpath) {
          for (b <- a) {
            print(b.edgeToString())
          }
        }
      }

    }
  }

}