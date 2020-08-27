package practice.part3typesdatasets

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

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDs = readDf("guitars.json").as[Guitar]
  val guitarPlayersDs = readDf("guitarPlayers.json").as[GuitarPlayer]
  val bandsDs = readDf("bands.json").as[Band]

  val guitarPlayerBandsDs: Dataset[(GuitarPlayer, Band)] = guitarPlayersDs.joinWith(bandsDs, guitarPlayersDs.col("band") === bandsDs.col("id"), "inner")

  /**
    * Exercise:
    * 1. Join guitarsDs and guitarPlayersDs, array_contains, outer join
    */
  val guitarGuitarPlayerDs = guitarsDs.joinWith(guitarPlayersDs, array_contains(guitarPlayersDs.col("guitars"), guitarsDs.col("id")), "outer")

  // Grouping
  val carsGroupedByOrigin = carsDs.groupByKey(_.Origin).count()
  carsGroupedByOrigin.show()

  // Joins and groups are WIDE transformations
}













