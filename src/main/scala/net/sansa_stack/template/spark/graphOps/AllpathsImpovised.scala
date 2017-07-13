package net.sansa_stack.template.spark.graphOps

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object AllpathsImpovised {

  class Path(val srcId: VertexId, val edgeLabel: String, val dstId: VertexId) extends Serializable {

    def edgeToString(): String = {

      val stringRep = srcId + " ; " +
        edgeLabel + " ; " + dstId
      return stringRep
    }

    def containsId(vid: Long): Boolean = {
      return (srcId == vid || dstId == vid)
    }
  }

  // type PMap = Map[VertexId, PathEdge]

  def runPregel[VD, ED: ClassTag](
    graph: Graph[VD, ED], activeDirection: EdgeDirection): VertexRDD[List[List[Path]]] = {

    val initialMsg = List.empty[List[Path]]
    val pregelGraph = graph.mapVertices((id, nodeData) => (id, List.empty[List[Path]])).cache
    val messages = pregelGraph.pregel[List[List[Path]]](initialMsg, 3, activeDirection)(
      //Pregel Vertex program
      (vid, vertexDataWithPaths, newPathsReceived) => {

        (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)

      },

      //Pregel Send Message 
      triplet => {

        val receivedPathsSrc = triplet.srcAttr._2
        val receivedPathsDest = triplet.dstAttr._2
        val pathLength = getPathLength(receivedPathsSrc, receivedPathsDest)

        //if its Start of iteration (=0)
        if (pathLength == 0) {
          val path = new Path(triplet.srcId, triplet.attr.toString(), triplet.dstId)
          Iterator((triplet.dstId, List(List(path))))
        } else {
          //All other iterations: A triplet is active, 
          // iff source and/or destination have received a message from previous iteration
          var sendMsgIterator: Set[(VertexId, List[List[Path]])] = Set.empty

          // Is triplet.source an active vertex
          if (isNodeActive(triplet.srcId, receivedPathsSrc, pathLength)) {

            val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
            if (filteredPathsSrc.length != 0) {

              //println("Valid Paths( without possible cycles =" + filteredPathsSrc.length)
              val newEdgeToAddToPathsSrc = new Path(triplet.srcId, triplet.attr.toString(),
                triplet.dstId)
              //Append new edge to remaining and send
              val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
              val sendMsgDest = (triplet.dstId, newPathsSrc)
              sendMsgIterator = sendMsgIterator.+(sendMsgDest)
            }
          }
          
          // Is triplet.destination an active vertex
          if (isNodeActive(triplet.dstId, receivedPathsDest, pathLength)) {
            val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
            if (filteredPathsDest.length != 0) {
              val newEdgeToAddToPathsDest = new Path(triplet.dstId, triplet.attr.toString(), triplet.srcId)
              //Append new edge to remaining and send
              val newPathsDst = filteredPathsDest.map(path => newEdgeToAddToPathsDest :: path)
              val sendMsgSrc = (triplet.srcId, newPathsDst)
              sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
            }
          }
          sendMsgIterator.toIterator
        }
      },

      //Pregel Merge message
      (pathList1, pathList2) => pathList1 ++ pathList2) // End of Pregel

    val vertexWithType = messages.vertices
    //vertexWithType.collect().foreach(a => println(a._1 + " :" + a._2._2.foreach { x => for (a <- x) { a.edgeToString() } }))

    graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (typeList._2))

  }
  def getPathLength(pathList1: List[List[Path]], pathList2: List[List[Path]]): Int = {

    if (pathList1.length == 0 && pathList2.length == 0)
      return 0
    else {
      val numPaths1 = pathList1.length
      val numPaths2 = pathList2.length
      if (numPaths1 == 0) {
        return pathList2.head.size
      } else if (numPaths2 == 0) {
        return pathList1.head.size
      } else {
        // Both lists have data
        return Math.max(pathList1.last.size, pathList2.last.size)
      }
    }
  }

  def isNodeActive(nodeId: VertexId, pathList: List[List[Path]], iteration: Int): Boolean = {
    if (pathList.length == 0) {
      return false
    } else {
      return (pathList.head.size == iteration)
    }
  }
}
