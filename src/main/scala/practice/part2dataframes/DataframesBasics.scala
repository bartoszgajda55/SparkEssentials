package practice.part2dataframes

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataframesBasics extends App {
  val spark = SparkSession
    .builder()
    .appName("")
    .master("local[1]")
    .getOrCreate()

  // Reading Df
  val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // Printing to console
  firstDf.show()
  firstDf.printSchema()

  // Get rows
  firstDf.take(10).foreach(println)

  // Spark built in types
  val longType = LongType

  // Schema
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

  // Obtain a schema
  val carsDfSchema = firstDf.schema
  println(carsDfSchema)

  // Read a Df with custom schema
  val carsDfWithSchema = spark.read
    .format("json")
    .schema(carsDfSchema)
    .load("src/main/resources/data/cars.json")

  // Create rows by hand
  val row = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // Create Df from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDf = spark.createDataFrame(cars) // schema auto-inferred

  // Create Df with implicits
  import spark.implicits._
  val manualCarsDfWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDf.printSchema()
  manualCarsDfWithImplicits.printSchema()

  /**
    * Exercise
    * 1) Create a manual DF describing smartphones, show, print schema
    * 2) Read file from data folder (movies), print schema and count number of rows
    */

  val smartphones = Seq(
    ("Samsung", "Galaxy S10", 1000.0),
    ("Apple", "iPhone 11", 1200.0)
  )
  val smarthponesDf = smartphones.toDF("Name", "Model", "Price")
  smarthponesDf.show()
  smarthponesDf.printSchema()

  val movies = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  movies.printSchema()
  println(movies.count())
}
