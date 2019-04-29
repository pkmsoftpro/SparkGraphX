package com.graphx.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.examples.simapp.GraphXInterface
import com.graphx.part.GraphPart
import scala.util.{Left, Right}
/**
  * code: by prashant on 04/2/19.
  */

object GraphXMainApp extends GraphXInterface {

  val conf = new SparkConf().setAppName("GraphApp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc);

  def main(args: Array[String]) {
    val df = sqlContext.read.format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load("src/main/resources/playerinfo.csv")
    val graphPart = GraphPart("StartEdge", "Player", "Association", "EndEdge")
    runGraph(df, graphPart) match {
      case Right(data) => println(s"GraphX process successfully completed.")
      case Left(error) => println(s"Error during graphX:::${error}")
        sqlContext.sparkContext.stop()
    }
  }
}