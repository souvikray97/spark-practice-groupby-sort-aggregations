package org.souvik.application

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, types}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class FirstTest extends AnyFunSuite{

  private val spark = SparkSession.builder()
    .appName("FirstName")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(Seq(
    StructField("Date", DateType, true),
    StructField("Open", DoubleType, true),
    StructField("Close", DoubleType, true)
  ))

  test("returns highest closing prices for year"){
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testDf = spark.createDataset(testRows)
    val actualRows = Main.highestClosingPricesPerYear(testDf)
      .collect()

    actualRows should contain theSameElementsAs expected
  }
}
