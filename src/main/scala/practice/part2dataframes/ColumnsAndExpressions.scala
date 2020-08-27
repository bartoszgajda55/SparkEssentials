package practice.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {
  val spark = SparkSession
    .builder()
    .appName("DF columns and expressions")
    .master("local[1]")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDf.show()

  // Columns
  val firstColumn = carsDf.col("Name")

  // Selecting column (projecting)
  val carNamesDf = carsDf.select(firstColumn)

  // Various select methods
  import spark.implicits._
  carsDf.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto converted to column
    $"Horsepower", // Interpolated string, returns column
    expr("Origin") // Expression
  )

  // Select multiple columns
  carsDf.select("Name", "Year")

  // Expressions
  val simplestExpression = carsDf.col("Weight_in_lbs")
  val weightInKgExpression = carsDf.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDf = carsDf.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  // selectExpr
  val carsWithSelectExprWeightDf = carsDf.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // Df processing

  // Adding column
  val carsWithKg3Df = carsDf.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Renaming column
  val carsWithColumnRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // Escaping quotes
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // Removing a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDf = carsDf.filter(col("Origin") =!= "USA")
val europeanCarsDf2 = carsDf.where(col("Origin") =!= "USA")
  // Filtering with expression string
  val americanCarsDf = carsDf.filter("Origin = 'USA'")
  // Chain filters
  val americanPowerfulCarsDf = carsDf.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDf2 = carsDf.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDf3 = carsDf.filter("Origin == 'USA' and Horsepower > 150")

  // Unioning - adding more rows
  val moreCarsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDf = carsDf.union(moreCarsDf) // have to have same schema

  // Distinct
  val allCountries = carsDf.select("Origin").distinct()
  allCountries.show()

  /**
    * 1. Read movies, select 2 cols of choice
    * 2. Create a new col, summing up total profit
    * 3. Select all comedies with IMDB rating above 6
    */

  val moviesDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // 1
  val moviesColDf = moviesDf.select("Title", "Release_Date")
  val moviesColDf2 = moviesDf.select(col("Title"), col("Release_Date"))
  val moviesColDf3 = moviesDf.select($"Title", $"Release_Date")

  // 2
  val moviesProfitDf = moviesDf.withColumn("Total_Profit", $"US_Gross" + $"Worldwide_Gross" + $"US_DVD_Sales")

  // 3
  val goodComediesDf = moviesDf.filter($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)
}
















