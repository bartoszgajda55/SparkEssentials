package practice.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local[1]")
    .getOrCreate()

  val guitarsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // Joins
  val joinCondition = guitaristsDf.col("band") === bandsDf.col("id")
  val guitaristsBandsDf = guitaristsDf
    .join(bandsDf, joinCondition, "inner")

  // Outer joins
  // Lef outer
  guitaristsDf.join(bandsDf, joinCondition, "left_outer")

  // Right outer
  guitaristsDf.join(bandsDf, joinCondition, "right_outer")

  // Full outer join
  guitaristsDf.join(bandsDf, joinCondition, "outer")

  // Semi joins
  guitaristsDf.join(bandsDf, joinCondition, "left_semi")

  // Anti join
  guitaristsDf.join(bandsDf, joinCondition, "left_anti")

  // This crashes - two 'id' columns exist
  //  guitaristsBandsDf.select("id", "band")

  // 1 - rename the column on which the join is made
  guitaristsDf.join(bandsDf.withColumnRenamed("id", "band"), "band")

  // 2 - drop the duplicate column
  guitaristsBandsDf.drop(bandsDf.col("id"))

  // 3 - rename offending column
  val bandsModDf = bandsDf.withColumnRenamed("id", "bandId")
  guitaristsDf.join(bandsModDf, guitaristsDf.col("band") === bandsModDf.col("bandId"))

  // Using complex types
  guitaristsDf.join(guitarsDf.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)"))
}
