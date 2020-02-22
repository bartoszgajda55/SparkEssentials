package practice.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import practice.dataframes.DataframesBasics.spark

object Datasources extends App {
  val spark = SparkSession.builder()
    .appName("Data sources and format")
    .config("spark.master", "local[1]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /**
    * Reading DF:
    * - format
    * - schema (infer ot strict)
    * - options
    */
  val carsDf = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  val carsDfWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /**
    * Writing Dfs
    * - format
    * - save mode
    * - path
    * - options
    */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.json")
}
