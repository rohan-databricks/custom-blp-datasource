package com.example.mktdata

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random

class MktdataStreamSource(sqlContext: SQLContext, parameters: Map[String, String]) extends Source {

  private val securities = parameters.getOrElse("securities", "['SPY US EQUITY']").replace("[", "").replace("]", "").replace("'", "").split(",").map(_.trim)
  private val fields = parameters.getOrElse("fields", "['BID']").replace("[", "").replace("]", "").replace("'", "").split(",").map(_.trim)

  private var currentOffset: Long = 0L

  override def schema: StructType = StructType(Seq(
    StructField("timestamp", TimestampType),
    StructField("security", StringType),
    StructField("field", StringType),
    StructField("value", DoubleType)
  ))

  override def getOffset: Option[Offset] = {
    currentOffset += 1
    Some(LongOffset(currentOffset))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rand = new Random()
    val rows = new mutable.ArrayBuffer[Row]()
    val now = new java.sql.Timestamp(System.currentTimeMillis())

    for (sec <- securities; field <- fields) {
      val value = rand.nextDouble() * 400 + 100
      rows += Row(now, sec, field, value)
    }

    sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(rows), schema)
  }

  override def stop(): Unit = {}

  case class LongOffset(offset: Long) extends Offset {
    override def json(): String = offset.toString
  }
}
