package net.sansa_stack.template.spark.rdf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j._


object FrequentPathInfo extends App {
  
  
  Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)
  
  val inputDataSet = "src/main/resources/datset.nt"
  val conf = new SparkConf().setAppName("GraphXTest").setMaster("local")
  val sc = new SparkContext(conf)
   
  val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
  val tripleRDD = spark.sparkContext.textFile(inputDataSet).map(TripleUtils.parsTriples)
  
  val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
  
  type VertexId = Long
  val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
     
  val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
      
  val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
        {
          case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
        })
       
       
  val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
   
  val graph = Graph(vertices, edges.distinct())
  
  val edges: RDD[(String, String)] = graph.triplets.map(f=>(f.srcAttr, f.dstAttr))
  val startVertices: RDD[String] = vertexRDD1.map(f=>f._2)
  
 val initialStep = edges.join( startVertices.map( (_, "") ) ).mapValues( _._1 )
  val index = edges.map( _.swap ).persist() // we will iteratively join with this RDD
  index.foreach(println)
  //: RDD[(String, String)]
 
  def stepOver(prevStep: RDD[(String, String)], iteration: Int = 1): RDD[(String, String)] = {
      val currentStep = index.cogroup(prevStep.map( _.swap )).flatMapValues(pair =>
        for (i <- pair._1.iterator; ps <- pair._2.iterator)
          yield (ps, i)).setName( s"""Step_$iteration""").persist()
      val count = currStep.count()
       if (count == 0 || iteration == 25) currStep
      else currentStep union stepOver(currentStep, iteration + 1)
  
}
  
/*val allPaths = initialStep union stepOver(initialStep)
/* now we can collect all paths */
val result = startVertices.map( (_, "") ).cougroup(allPaths).map( pair => (pair._1, pair._2._2.toList) )*/
}
