package com.graphx.cc

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, StronglyConnectedComponents}
import org.apache.spark.graphx.{VertexId, Edge, Graph, PartitionStrategy}

object ConnectedComponentPerfTest {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("PageRank Use Case").setMaster("local[*]")
    val sc = new SparkContext(conf)
        
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);
    import sqlContext.implicits._
    
    val startTime = System.nanoTime()
    
    val df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("hdfs://localhost:54310/user/prashant/data/graphx/2017-fordgobike-tripdata.csv")
    
    val justStations = df.selectExpr("float(start_station_id) as station_id", "start_station_name").distinct()

    val stations = df.select("start_station_id", "end_station_id").rdd.distinct().flatMap(x => Iterable(x(0).asInstanceOf[Number].longValue, x(1).asInstanceOf[Number].longValue)).distinct().toDF()

    val stationVertices: RDD[(VertexId, String)] = stations.join(justStations, stations("value") === justStations("station_id")).select("station_id", "start_station_name").rdd.map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String]))

    stationVertices.collect().foreach(println)	

    val stationEdges:RDD[Edge[Long]] = df.select("start_station_id", "end_station_id").rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))

    val defaultStation = ("Missing Station")
    
    val stationGraph = Graph(stationVertices, stationEdges, defaultStation)
    
    val cc = stationGraph.connectedComponents().vertices
    
    cc.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2))
    
    val ccByStation = stationVertices.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByStation.collect().mkString("\n"))
    
    val duration = (System.nanoTime - startTime) / 1e9d
    
    print("Processing time for Connected Component is: ");
    
    println(duration);
    
    sc.stop()
  }
}