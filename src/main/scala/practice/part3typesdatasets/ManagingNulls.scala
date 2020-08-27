package practice.part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .master("local[1]")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Select the first non-null value
  moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  // Checking for nulls
  moviesDf.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // Nulls when ordering
  moviesDf.orderBy(col("IMDB_Rating").desc_nulls_last)

  // Removing nulls
  moviesDf.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  // Replace nulls
  moviesDf.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDf.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // Complex operations
  moviesDf.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // returns null if two values are equal, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)" //
  )
}
