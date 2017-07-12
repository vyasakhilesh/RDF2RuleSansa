package net.sansa_stack.template.spark.graphOps
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object Allpaths {

  class Path(val srcId: VertexId, val edgeLabel: String, val dstId: VertexId,
             val isOutgoing: Boolean = true) {

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

  def runPregel[VD, ED: ClassTag](graph: Graph[VD, ED], activeDirection: EdgeDirection): VertexRDD[(String, List[List[Path]])] = {
    val initialMsg = List.empty[List[Path]]
    val pregelGraph = graph.mapVertices((id, nodeData) => (id, List.empty[List[Path]])).cache
    val messages = pregelGraph.pregel[List[List[Path]]](initialMsg, 5, activeDirection)(
      //Pregel Vertex program
      (vid, vertexDataWithPaths, newPathsReceived) => {
        (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)

      },
      //Pregel Send Message 
      triplet => {
        val receivedPathsSrc = triplet.srcAttr._2
        val pathLength = getPathLength(receivedPathsSrc)

        //if its Start of iteration (=0)
        if (pathLength == 0) {
          val path = new Path(triplet.srcId, triplet.attr.toString(), triplet.dstId, true)
          Iterator((triplet.dstId, List(List(path))))

        } else if (pathLength <10) {
          var sendMsgIterator: Set[(VertexId, List[List[Path]])] = Set.empty
          // Is triplet.source an active vertex
          val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
          if (filteredPathsSrc.length != 0) {
            val newEdgeToAddToPathsSrc = new Path(triplet.srcId, triplet.attr.toString(),
              triplet.dstId, true)
            //Append new edge to remaining and send
            val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
            val sendMsgDest = (triplet.dstId, newPathsSrc)
            sendMsgIterator = sendMsgIterator.+(sendMsgDest)
          }
          sendMsgIterator.toIterator
        } else {
          Iterator.empty
        }

      },
      //Pregel Merge message
      (pathList1, pathList2) => pathList1 ++ pathList2) // End of Pregel

    val allPathsToDestination = messages.vertices
    allPathsToDestination.collect().foreach(a => println(a._1 + " :" + a._2._2.foreach { x => for (a <- x) { a.edgeToString() } }))

    graph.vertices.innerJoin(allPathsToDestination)((vid, label, pathlist) => (label.toString(), pathlist._2))
  }

  def getPathLength(pathList1: List[List[Path]]): Int = {
    if (pathList1.length == 0)
      return 0

    else
      return pathList1.last.size
  }

}