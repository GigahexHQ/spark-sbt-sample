package org.handson.spark.collections

import org.apache.spark.sql.SparkSession

object MapTransformer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]") getOrCreate ()
    val sc = spark.sparkContext

    val range = 1 to 100
    val numberSet = sc.parallelize(range)
    val squares = numberSet.map(x => Math.pow(x, 2)).cache()
    println("************************************")
    println("* Squares *")
    println("************************************")
    squares.collect().foreach(n => println(n))

    /**
     * Map with filter, to find the odd numbers. This is called chaining transformations
     */
    val odds = squares.filter(num => num % 2 != 0)
    println("************************************")
    println("* Odd numbers *")
    println("************************************")
    odds.collect().foreach(n => println(n))

    spark.stop()

  }

}
