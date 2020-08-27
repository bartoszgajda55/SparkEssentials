package practice.part5lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("RDDs")
    .master("local[1]")
    .getOrCreate()

  val sc= spark.sparkContext

  // Parallelling existing collection
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)

  // Reading from files
  case class StockValue(company: String, date: String, price: Double)
  def readStocks(filename: String) = {
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksRdd = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // Reading from files v2
  val stocksRdd2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // Reading from DF
  val stocksDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDs = stocksDf.as[StockValue]
  val stocksRdd3 = stocksDs.rdd

  // Rdd to Df
  val numbersDf = numbersRDD.toDF("numbers")
}
