package net.sansa_stack.template.spark.graphOps

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object AllpathsImpovised {

  def getPath(graph: Graph[String, String]): VertexRDD[List[Path]] = {
    val initialMsg: List[Path] = List.empty
    val maxIterations = 2
    val activeDirection = EdgeDirection.Out
    val pregelGraph = graph.mapVertices((id, label) => (id, List.empty[Path]))

    val messages = pregelGraph.pregel[List[Path]](initialMsg, maxIterations, activeDirection)(
      (vid, oldMessage, newMessage) => (vid, newMessage ),
      (triplet) => {
        val receivedPathsSrc = triplet.srcAttr._2
        val receivedPathsDest = triplet.dstAttr._2

        if ((receivedPathsSrc.isEmpty) && (receivedPathsDest.isEmpty)) {
          val path = new Path(1, List(triplet.dstAttr._1))
          val newMsg = path :: triplet.srcAttr._2
          Iterator((triplet.dstId, newMsg))
        } else if ((receivedPathsSrc.length != 0)) {
          for (a <- receivedPathsDest) {
            if (a.path.contains(triplet.dstId)) {
              a.path :+ triplet.dstId
              a.length += 1
            }
          }
          Iterator((triplet.dstId, receivedPathsDest))
        } /*else if ((receivedPathsDest.length != 0) && (receivedPathsSrc.length != 0)) {
          for (a <- receivedPathsDest) {
            if (a.path.contains(triplet.dstId)) {
              for (x <- triplet.dstAttr._2) {
                a.path ++ x.path
                a.length += x.length

              }
            }
          }

          Iterator((triplet.srcId, receivedPathsDest))
        }*/ else {
          Iterator.empty
        }
      },
      (msg1, msg2) => msg1 ++ msg2)

    val vertexWithType = messages.vertices
    vertexWithType.collect.foreach(println)
    graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (typeList._2))

  }
}
case class Path(var length: Int, var path: List[VertexId])