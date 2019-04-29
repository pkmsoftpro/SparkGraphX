package com.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PageRankSparkTest {

  def main(args: Array[String]) {
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkPageRank")
      .getOrCreate()
      

    val iters = 1
    val startTime = System.nanoTime();
    val lines = spark.read.textFile("hdfs://localhost:54310/user/prashant/data/spark/roadNet-CA_adj.tsv").rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    val duration = (System.nanoTime - startTime) / 1e9d
    print("Processing time of" + " 55M" +" records for spark pagerank is: ");
    
    println(duration);
    spark.stop()
  }
}