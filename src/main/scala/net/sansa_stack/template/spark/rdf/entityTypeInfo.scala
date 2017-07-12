
package net.sansa_stack.template.spark.rdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
//AUTHOR:
import org.apache.log4j._

object GraphOps {
  def main(args: Array[String]) = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)
    val t1 = System.nanoTime
    val input = "src/main/resources/datset.nt"
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local")
    val sc = new SparkContext(conf)
   
    val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
    val tripleRDD = spark.sparkContext.textFile(input).map(TripleUtils.parsTriples)
    
    val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
    
    type VertexId = Long
    val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
   
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
    
    val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
      {
        case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
      })
     
     
    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
 
    val graph = Graph(vertices, edges)
   
    //Taking one predicate of FPC to know entities meanwhile
    val predlist = List("http://xmlns.com/foaf/0.1/depiction")
   //val predlist = graph.edges.flatMap(x=>List(x.attr)).collect().distinct.toList
 
  
  
  def searchedge(x:String, y:List[String]):Boolean={
    y.contains(x)
    
  }
  
  //val setofentity = graph.triplets.filter(x=> searchedge(x.attr, predlist)).flatMap(x=>List(x.srcAttr, x.dstAttr))
  val setofentity = graph.triplets.filter(x=> searchedge(x.attr, predlist)).flatMap(x=>List(x.srcAttr))
  val setofdistictentity = setofentity.distinct().collect().toList
  val printsetofdistictentity = setofdistictentity.foreach(println)
  
  def searchdes(x:String, y:List[String], a:String, b:String):Boolean= {
    ((y.contains(x))&&(a==b))
   }
  
 
  val setoftypelist = graph.triplets.filter(x=>searchdes(x.srcAttr, setofdistictentity , x.attr, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).map(x=>(x.dstAttr, 1)).reduceByKey((a,b)=>(a+b))
  // val printsetoftypelist = setoftypelist.foreach(println)
   
   //Remove Types according to thresold
   val support_count = 20
   var freqsetoftypelist = setoftypelist.filter(x=>x._2 >= support_count).sortBy(_._2, false)
   
  // val printfreqsetoftypelist = freqsetoftypelist.foreach(println)
   println("")
   
   //Count > K remove
  //println(freqsetoftypelist.count())
  val k:Int = 4
  if(freqsetoftypelist.count() > k )
  {
    
     freqsetoftypelist = sc.parallelize(freqsetoftypelist.take(k)) //I am losing RDD here
  }
    
  println("---------------------Freqsetoftypelist-------------------")
   freqsetoftypelist.map(x=>(x._1)).foreach(println)
    
   // To do Task Merge set of distinct entities (setofdistictentity) and predicate (freqsetoftypelist) and add it to original RDD graph.--------------------------------------
  
    spark.stop
    val duration = (System.nanoTime - t1) / 1e9d
    println("\nDone--"+"Duration:"+duration)
  }
}
