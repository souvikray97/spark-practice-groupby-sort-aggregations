package org.souvik.application


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, year}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object Main {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName("spark-practice2")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

    val df: DataFrame =spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    val stockData = df.select(renameColumns: _*)

    import spark.implicits._

    stockData
      .groupBy(year($"Date").as("year"))
      .agg(functions.max($"close").as("maxClose"), functions.avg($"close").as("avgClose"))
      .sort($"maxClose".desc)
      .show()

    stockData
      .groupBy(year($"date").as("year")) 
      .max("close","high")
      .show()

    val highestClosingPrices = highestClosingPricesPerYear(stockData)


  }

  def highestClosingPricesPerYear(df: DataFrame): DataFrame = {

    import df.sparkSession.implicits._
    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .drop("rank")
      .sort($"close".desc)
  }


  }
