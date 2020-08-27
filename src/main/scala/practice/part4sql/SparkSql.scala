package practice.part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part4sql.SparkSql.{readTable, spark, transferTables}

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .master("local[1]")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Regular DF API
  carsDf.select(col("Name")).where(col("Origin") === "USA")

  // SQL way
  carsDf.createOrReplaceTempView("cars")
  val americanCarsDf = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // Running SQL statements
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDf = spark.sql("show databases")

  // Transfering db tables to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  val employeesDf = readTable("employees")
  employeesDf.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")
}
