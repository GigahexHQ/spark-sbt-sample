package com.gigahex.sample

//import com.gigahex.error.Gigahex
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFtoDag extends App with SparkUtils with BasicOptionParser {

  private implicit val spark: SparkSession = getLocalSession(getClass.getSimpleName)


  val programArgs = parse(args)
  val inputPeople = programArgs.input.split(",")(0)
  val inputCities = programArgs.input.split(",")(1)

  //Create the dataframe for people and cities
  val people = readCSV(inputPeople)
  val cities = readCSV(inputCities)
try {

  cities.join(people, cities.col("id") === people.col("city_id"), "left_outer")
    .groupBy("city")
    .agg(count(people.col("id")) as "count")
    .select("city", "count")
    .write
    .csv(programArgs.output)

} catch {
  case e : Exception =>
    throw e
    //Gigahex.catchException(e, spark.sparkContext)
}

  spark.sparkContext.stop()

}
