package net.sansa_stack.template.spark.graphOps

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object AllpathsImpovised {

  def getPath(graph: Graph[String, String]): VertexRDD[List[Path]] = {
    val initialMsg: List[Path] = List.empty
    val maxIterations = 5
    val activeDirection = EdgeDirection.Out
    val pregelGraph = graph.mapVertices((id, label) => (id, label, List.empty[Path]))

    val messages = pregelGraph.pregel[List[Path]](initialMsg, maxIterations, activeDirection)(
      (vid, oldMessage, newMessage) => (vid, oldMessage._2, newMessage ++ oldMessage._3),
      (triplet) => {
        val receivedPathsSrc = triplet.srcAttr._3
        val receivedPathsDest = triplet.dstAttr._3

        if ((receivedPathsSrc.length == 0) && (receivedPathsDest.length == 0)) {
          val path = new Path(1, List(triplet.dstAttr._1))
          val newMsg = path :: triplet.srcAttr._3
          Iterator((triplet.srcId, newMsg))
        } else if ((receivedPathsDest.length == 0) && (receivedPathsSrc.length != 0)) {
          for (a <- receivedPathsDest) {
            if (a.path.contains(triplet.dstId)) {
              triplet.dstId :: a.path
            }
          }

          Iterator((triplet.srcId, receivedPathsDest))
        } else if ((receivedPathsDest.length != 0) && (receivedPathsSrc.length != 0)) {
          for (a <- receivedPathsDest) {
            if (a.path.contains(triplet.dstId)) {
              a.path ++ triplet.dstAttr._3
            }
          }

          Iterator((triplet.srcId, receivedPathsDest))
        } else {
          Iterator.empty
        }
      },
      (msg1, msg2) => msg1 ++ msg2)

    val vertexWithType = messages.vertices
    vertexWithType.collect.foreach(println)
    graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (typeList._3))

  }
}
case class Path(length: Int, path: List[VertexId])