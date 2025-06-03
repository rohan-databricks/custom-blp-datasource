package com.example.mktdata

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends StreamSourceProvider with DataSourceRegister {

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]
  ): Source = {
    new MktdataStreamSource(sqlContext, parameters)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]
  ): (String, StructType) = {
    ("mktdata", new MktdataStreamSource(sqlContext, parameters).schema)
  }

  // override def shortName(): String = "//blp/mktdata"
  override def shortName(): String = "blp-mktdata"
}
