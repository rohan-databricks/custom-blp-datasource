package com.example.blp

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import scala.util.Random
import java.sql.Timestamp

// import org.apache.spark.sql.sources.{BaseRelation, TableScan}

case class BlpRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
) extends BaseRelation with TableScan {


  override def schema: StructType = {
    val serviceName = parameters.getOrElse("serviceName", "ReferenceDataRequest")
    serviceName match {
      case "HistoricalDataRequest" =>
        StructType(List(
          StructField("security", StringType),
          StructField("field", StringType),
          StructField("date", StringType),
          StructField("value", DoubleType)
        ))

      case "IntradayTickRequest" | "IntradayBarRequest" =>
        StructType(List(
          StructField("security", StringType),
          StructField("field", StringType),
          StructField("timestamp", TimestampType),
          StructField("value", DoubleType)
        ))

      case "ReferenceDataRequest" =>
        StructType(List(
          StructField("security", StringType),
          StructField("field", StringType),
          StructField("value", StringType)
        ))

      case _ =>
        throw new IllegalArgumentException(s"Unsupported serviceName: $serviceName")
    }
  }

  override def buildScan(): org.apache.spark.rdd.RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rand = new Random()

    val serviceName = parameters.getOrElse("serviceName", "ReferenceDataRequest")
    val fields = splitToList(parameters.getOrElse("fields", "['PX_LAST']"))
    val securities = splitToList(parameters.getOrElse("securities", parameters.getOrElse("security", "['SPY US EQUITY']")))
    val startDate = parameters.getOrElse("startDate", "2022-01-01")
    val startDateTime = parameters.getOrElse("startDateTime", "2022-11-01T00:00:00")
    val timestamp = Timestamp.valueOf(startDateTime.replace("T", " "))
    val interval = parameters.getOrElse("interval", "60").toInt

    val rows = serviceName match {
      case "HistoricalDataRequest" =>
        for {
          sec <- securities
          field <- fields
        } yield Row(sec, field, startDate, randBetween(100.0, 200.0))

      case "IntradayTickRequest" | "IntradayBarRequest" =>
        for {
          sec <- securities
          field <- fields
        } yield Row(sec, field, timestamp, randBetween(300.0, 500.0))

      case "ReferenceDataRequest" =>
        for {
          sec <- securities
          field <- fields
        } yield Row(sec, field, randBetween(100.0, 500.0).toString)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported serviceName: $serviceName")
    }

    sc.parallelize(rows)
  }

  private def splitToList(raw: String): List[String] = {
    raw.replace("[", "")
      .replace("]", "")
      .replace("'", "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList
  }

  private def randBetween(min: Double, max: Double): Double = {
    min + (max - min) * new Random().nextDouble()
  }
}
