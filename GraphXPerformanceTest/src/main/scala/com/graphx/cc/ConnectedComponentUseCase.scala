package com.graphx.cc

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader

object ConnectedComponentUseCase {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkPageRank")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "hdfs://localhost:54310/user/prashant/data/graphx/followers.txt", true)
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("hdfs://localhost:54310/user/prashant/data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}