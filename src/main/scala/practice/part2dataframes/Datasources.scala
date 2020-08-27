package practice.part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
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
    StructField("Year", DateType),
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

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy or deflate
    .json("src/main/resources/data/cars.json")

  // CSV
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDf.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  /**
    * Exercise
    * - read movies df
    * - save as tab separated csv
    * - save as a snappy parquet
    */
  val movies = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  movies.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  movies.write.save("src/main/resources/data/movies.parquet")
}
