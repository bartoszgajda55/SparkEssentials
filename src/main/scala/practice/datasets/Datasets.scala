package practice.datasets

import java.sql.Date

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .master("local[1]")
    .getOrCreate()

  val numbersDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  // Converting Df to Ds
  implicit val intEncoder = Encoders.scalaInt
  val numbersDs: Dataset[Int] = numbersDf.as[Int]

  // Dataset of complex type
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )
  def readDf(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  import spark.implicits._
  val carsDf = readDf("cars.json")
  val carsDs = carsDf.as[Car]

  // Ds collection functions
  numbersDs.filter(_ < 100)

  // map, flatMap, folds, reduce, filter, for comprehensions, etc.
  val carNamesDs = carsDs.map(car => car.Name.toUpperCase())
}
