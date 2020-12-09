package com.gigahex.tpch

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
    //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: String, outputDir: String): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = outputDir + "/dbgen/output"

    val results = new ListBuffer[(String, Float)]


      val t0 = System.nanoTime()

      val query = Class.forName(f"com.gigahex.tpch.${queryNum}").newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(spark, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)



     results
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TPCH-Queries").getOrCreate()
//    val conf = new SparkConf()().setAppName("Simple Application")
//    val sc = new SparkContext(conf)

    // read files from local FS
    val INPUT_DIR = args(0)
    val queryNum = args(1)
    val outputDir = args(2)

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    val schemaProvider = new TpchSchemaProvider(spark, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(spark, schemaProvider, queryNum, outputDir)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}