package practice.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations")
    .master("local[1]")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDf = moviesDf.select(count(col("Major_Genre"))) // all except null
  moviesDf.selectExpr("count(Major_Genre)")

  // Counting all
  moviesDf.select(count("*")).show() // count all rows, with nulls
  // Counting distinct
  moviesDf.select(countDistinct("Major_Genre")).show()
  // Approximate count
  moviesDf.select(approx_count_distinct(col("Major_Genre")))

  // Min and Max
  val minRatingDf = moviesDf.select(min(col("IMDB_Rating")))
  moviesDf.selectExpr("min(IMDB_Rating)")

  // Sum
  moviesDf.select(sum(col("US_Gross")))
  moviesDf.selectExpr("sum(US_Gross)")

  // Avg
  moviesDf.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDf.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // Data Science
  moviesDf.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDf = moviesDf
    .groupBy(col("Major_Genre")) // includes null
    .count()
  countByGenreDf.show()

  val avgRatingByGenreDf = moviesDf
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
  avgRatingByGenreDf.show()

  val aggregationsByGenreDf = moviesDf
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
  aggregationsByGenreDf.show()
}
















