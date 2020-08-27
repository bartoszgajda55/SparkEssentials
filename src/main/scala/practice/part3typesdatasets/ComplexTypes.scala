package practice.part3typesdatasets

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

  // Structures

  // 1 - with column operators
  moviesDf.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    .show()

  // 2 - with expression strings
  moviesDf.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
    .show()

  // Arrays
  val moviesWithWordsDf = moviesDf.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // array of strings
  moviesWithWordsDf.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
}
