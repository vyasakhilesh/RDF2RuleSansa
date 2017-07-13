package net.sansa_stack.template.spark.graphOps
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
object Allpaths {
  //case class PathEdge(length: Int, path: List[VertexId], val srcId: VertexId, val edgeLabel: String, val dstId: VertexId)
  class PathEdge(val srcId: VertexId, val edgeLabel: String, val dstId: VertexId,
                 val isOutgoing: Boolean = true) extends Serializable {

    def edgeToString(): String = {
      var directionString = "Outgoing"
      if (isOutgoing == false)
        directionString = "Incoming"
      val stringRep = directionString + " ; " + srcId + " ; " +
        edgeLabel + " ; " + dstId
      return stringRep
    }

    def containsId(vid: Long): Boolean = {
      return (srcId == vid || dstId == vid)
    }
  }

  // type PMap = Map[VertexId, PathEdge]

  def runPregel[VD, ED: ClassTag](src: (VertexId), dest: (VertexId),
                                  graph: Graph[VD, ED], activeDirection: EdgeDirection): List[List[PathEdge]] = {

    val pathSrcId = src
    val pathDstId = dest
    val initialMsg = List.empty[List[PathEdge]]
    val pregelGraph = graph.mapVertices((id, nodeData) => (id, List.empty[List[PathEdge]])).cache
    val messages = pregelGraph.pregel[List[List[PathEdge]]](initialMsg, 3, activeDirection)(
      //Pregel Vertex program
      (vid, vertexDataWithPaths, newPathsReceived) => {
        // If reached destination, save all the paths received so far, 
        // else, discard paths from previous iteration and work with results from this iteration
        if (vid == pathDstId)
          (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)
        else
          (vertexDataWithPaths._1, newPathsReceived)
      },

      //Pregel Send Message 
      triplet => {

        val receivedPathsSrc = triplet.srcAttr._2
        val receivedPathsDest = triplet.dstAttr._2
        val pathLength = getPathLength(receivedPathsSrc, receivedPathsDest)

        //if its Start of iteration (=0)
        if (pathLength == 0) {
          // Either edge.sourceNode should be Path_starter_node
          if (triplet.srcId == pathSrcId && (triplet.dstId == pathDstId)) {
            val path = new PathEdge(triplet.srcId, triplet.attr.toString(), triplet.dstId, true)
            Iterator((triplet.dstId, List(List(path))))
          } else if (triplet.dstId == pathSrcId && (triplet.srcId == pathDstId)) {
            // Or edge.destNode should be Path_starter_node
            val path = new PathEdge(triplet.srcId, triplet.attr.toString(), triplet.dstId, true)
            Iterator((triplet.srcId, List(List(path))))
          } else {
            Iterator.empty
          }
        } //All other iterations: A triplet is active, 
        // iff source and/or destination have received a message from previous iteration
        else {
          var sendMsgIterator: Set[(VertexId, List[List[PathEdge]])] = Set.empty

          // Is triplet.source an active vertex
          if (isNodeActive(triplet.srcId, receivedPathsSrc, pathLength, pathDstId) &&
            (triplet.dstId == pathDstId)) {

           // val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
          //  if (filteredPathsSrc.length != 0) {
              //println("Valid Paths( without possible cycles =" + filteredPathsSrc.length)
              val newEdgeToAddToPathsSrc = new PathEdge(triplet.srcId, triplet.attr.toString(),
                triplet.dstId, true)
              //Append new edge to remaining and send
              val newPathsSrc = receivedPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
              val sendMsgDest = (triplet.dstId, newPathsSrc)
              sendMsgIterator = sendMsgIterator.+(sendMsgDest)
           // }
          }

          // Is triplet.destination an active vertex
          if (isNodeActive(triplet.dstId, receivedPathsDest, pathLength, pathDstId) &&
            (triplet.srcId == pathDstId)) {

           // val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
            //if (filteredPathsDest.length != 0) {
              val newEdgeToAddToPathsDest = new PathEdge(triplet.dstId,
                triplet.attr.toString(), triplet.srcId, false)

              //Append new edge to remaining and send
              val newPathsDst = receivedPathsDest.map(path => newEdgeToAddToPathsDest :: path)
              val sendMsgSrc = (triplet.srcId, newPathsDst)
              sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
           // }
          }
          sendMsgIterator.toIterator
        }
      },

      //Pregel Merge message
      (pathList1, pathList2) => pathList1 ++ pathList2) // End of Pregel

    val allPathsToDestination = messages.vertices.filter(_._1 == pathDstId).collect.apply(0)._2._2
    return allPathsToDestination
  }
  def getPathLength(pathList1: List[List[PathEdge]], pathList2: List[List[PathEdge]]): Int = {

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

  def isNodeActive(nodeId: VertexId, pathList: List[List[PathEdge]], iteration: Int, finalDestId: VertexId): Boolean = {
    if (nodeId == finalDestId || pathList.length == 0) {
      return false
    } else {
      //=> Node is not destination and has some data, 
      // checking if data is valid for this iteration
      return (pathList.head.size == iteration)
    }
  }
}