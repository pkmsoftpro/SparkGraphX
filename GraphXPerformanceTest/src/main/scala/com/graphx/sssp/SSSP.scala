package com.graphx.sssp

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}

object SSSPExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // in order to test the SSSP change the number of verticesCount as per requirement
    val verticesCount = 100000
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = verticesCount).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // considering vertex 42 as the root
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val startTime = System.nanoTime()
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
    val duration = (System.nanoTime - startTime) / 1e9d
    
    print("Processing time in seconds of" + verticesCount +" records for SSSP is: ");
    
    println(duration);
    // processing complete

    spark.stop()
  }
}