package practice.part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .master("local[1]")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Adding a value to Df
  moviesDf.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDf.select("Title").where(dramaFilter)

  val moviesWithGoodnessFlagDf = moviesDf.select(col("Title"), preferredFilter.as("good_movie"))
  // Filtering on column name
  moviesWithGoodnessFlagDf.filter("good_movie")
  // Negation
  moviesWithGoodnessFlagDf.filter(not(col("good_movie")))

  // Math operations
  val moviesAverageRatingsDf = moviesDf.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // Correlations - PCC
  println(moviesDf.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // Strings
  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Capitalization
  carsDf.select(initcap(col("Name")))

  // Contains
  carsDf.select("*").where(col("Name").contains("vw"))

  // Regex
  val regexString = "volkswagen|vw"
  val vwDf = carsDf.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  // Replacing
  vwDf.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()
}













