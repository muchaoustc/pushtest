package com.tencent.omg.push.offline

import java.time.LocalDateTime

import org.apache.spark.{SparkConf, SparkContext}

//import com.tencent.omg.utils._
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object PushOffline {

  def main(args: Array[String]): Unit = {
    val logFile = "file:///usr/local/spark/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

