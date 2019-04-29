package com.examples.simapp

import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, StronglyConnectedComponents}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}
import com.graphx.part.GraphPart

import scala.util.control.NonFatal
/**
  * code: by prashant on 04/2/19.
  */


trait GraphXInterface {

  type GraphRDD = RDD[Map[String, Any]]

  def getEdges(df: DataFrame, graph: GraphPart): RDD[Edge[String]] = {
    df.rdd.map {
      case row: Row =>
        val startEdge = row.getAs[Any](graph.startNode).toString.toLong
        val endEdge = row.getAs[Any](graph.endNode).toString.toLong
        val edgeAttr = row.getAs[Any](graph.eData).toString
        Edge(startEdge, endEdge, edgeAttr)
    }
  }

  def getVertices(df: DataFrame, graph: GraphPart): RDD[(Long, String)] = df.rdd.map {
    case row =>
      val startEdge = row.getAs[Any](graph.startNode).toString.toLong
      val endEdge = row.getAs[Any](graph.eData).toString
      (startEdge, endEdge)
  }

  def runGraph(df: DataFrame, graphComponent: GraphPart): Either[String, GraphRDD] = {
    try {
      val edges = getEdges(df, graphComponent)
      val vertices = getVertices(df, graphComponent)
      val graph = Graph(vertices, edges).cache()
      for {
        result <- useGraphXAlgo(graph, graphComponent.startNode).right
      } yield result
    } catch {
      case NonFatal(t: Throwable) =>
        Left(t.getMessage)
    }
  }

  def useGraphXAlgo(graph: Graph[String, String], primaryIndex: String): Either[String, GraphRDD] = {
    try {
      //Page rank Algorithm
      val graphOnPR = PageRank.run(graph, 1, 0.001)
      
      val graphWithPageRank = graph.outerJoinVertices(graphOnPR.vertices) { // PageRank join with Graph
        case (id, attr, Some(pr)) => (pr, attr)
        case (id, attr, None) => (0.0, attr)
      }

      val graphOnTri = graphWithPageRank.outerJoinVertices(graph.partitionBy(PartitionStrategy.RandomVertexCut)
        .triangleCount().vertices) {  //Triangle count join with Graph page rank
        case (id, (rank, attr), Some(tri)) => (rank, tri, attr)
        case (id, (rank, attr), None) => (rank, 0, attr)
      }

      
      val graphOnCC = graphOnTri.outerJoinVertices(ConnectedComponents.run(graph).vertices) { 
        case (id, (rank, tri, attr), Some(cc)) => (rank, tri, cc, attr)  //CC join with Triange Component
        case (id, (rank, tri, attr), None) => (rank, tri, -1L, attr)
      }

      //Result will be sent to Right, Success
      val result = graphOnCC.triplets.map {
        case line =>
          val relation = Map("from" -> line.srcId.toString, "to" -> line.dstId.toString, "type" -> line.attr)
          Map(primaryIndex -> line.srcId.toLong, "PRAlgo" -> line.srcAttr._1.toDouble,
            "TriAlgo" -> line.srcAttr._2.toInt, "CCAlgo" -> line.srcAttr._3.toLong,
            "Assoc" -> relation)
      }
      print("************************************Result is***********************************")
      println("**********" + result.collect().mkString("\n"))
      // Top 5 Players
      println(graphOnCC.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
      Right(result)
    } catch {
      case NonFatal(t: Throwable) => Left(t.getMessage)
    }
  }
}
