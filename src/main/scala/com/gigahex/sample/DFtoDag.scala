package com.gigahex.sample

import org.apache.spark.sql.functions._

object DFtoDag extends App with SparkUtils with BasicOptionParser {

  implicit val spark = getLocalSession(getClass.getSimpleName)
  val programArgs = parse(args)
  val inputPeople = programArgs.input.split(",")(0)
  val inputCities = programArgs.input.split(",")(1)

  //Create the dataframe for people and cities
  val people = readCSV(inputPeople)
  val cities = readCSV(inputCities)

  //Count of people living in each city
  cities.join(people, cities.col("id") === people.col("city_id"), "left_outer")
    .groupBy("city")
    .agg(count(people.col("id")) as "count")
    .select("city","count")
    .write.csv(programArgs.output)

  spark.sparkContext.stop()

}
