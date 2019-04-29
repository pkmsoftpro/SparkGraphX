package com.graphx.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, StronglyConnectedComponents}
import org.apache.spark.graphx.{VertexId, Edge, Graph, PartitionStrategy}

object PageRankPerformanceTest {
  def main(args: Array[String]) {
    val logFile = "/usr/local/spark-2.4.1-bin-without-hadoop-scala-2.12/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("PageRank Use Case").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /*
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    * 
    */
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);
    import sqlContext.implicits._
    
    val startTime = System.nanoTime()
    
    val df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("hdfs://localhost:54310/user/prashant/data/graphx/2017-fordgobike-tripdata.csv")

    //println(df.count())
    //df.printSchema()
    
    val justStations = df.selectExpr("float(start_station_id) as station_id", "start_station_name").distinct()
    
    //justStations.printSchema()

    val stations = df.select("start_station_id", "end_station_id").rdd.distinct().flatMap(x => Iterable(x(0).asInstanceOf[Number].longValue, x(1).asInstanceOf[Number].longValue)).distinct().toDF()

    //stations.printSchema()

    val stationVertices: RDD[(VertexId, String)] = stations.join(justStations, stations("value") === justStations("station_id")).select("station_id", "start_station_name").rdd.map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String]))

    stationVertices.collect().foreach(println)	

    val stationEdges:RDD[Edge[Long]] = df.select("start_station_id", "end_station_id").rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))

   // stationEdges.collect().foreach(println)
    
    val defaultStation = ("Missing Station")
    
    val stationGraph = Graph(stationVertices, stationEdges, defaultStation)
    
    //stationGraph.cache()
    
    //stationGraph.numVertices
    
    //stationGraph.numEdges
    
    println(df.count())
    
    val ranks = stationGraph.pageRank(0.0001).vertices
    
    //val ranksByUsername = ranks.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2))
    
    val ranksByUsername = stationVertices.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    
    println(ranksByUsername.collect().mkString("\n"))
    
    val duration = (System.nanoTime - startTime) / 1e9d
    
    print("Processing time for PageRank is: ");
    
    println(duration);
    
    /*
     * uncomment the following to test the triplets, groupEdges, in and out degrees
     *
    stationGraph.groupEdges((edge1, edge2) => edge1 +edge2). triplets .sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)
  
    stationGraph.inDegrees. join(stationVertices) .sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))
    
    stationGraph .outDegrees. join(stationVertices) .sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " out degrees."))
    */
    
    sc.stop()
  }
}