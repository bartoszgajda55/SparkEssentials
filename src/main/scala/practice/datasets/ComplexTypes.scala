package practice.datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .master("local[1]")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDf.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

  /**
    * Exercise
    * 1. Dealing with multiple formats
    * 2. Read stocks dataframe an parse dates
    */

  // 1 parse the DF multiple times and union
  // 2
  val stocksDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithParseDate = stocksDf.select(
    col("symbol"),
    to_date(col("date"), "MMM d yyyy"))
    .show()
}
