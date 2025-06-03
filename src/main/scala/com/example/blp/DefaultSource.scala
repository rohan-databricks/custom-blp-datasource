package com.example.blp

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 * Entry point for the custom Bloomberg (emulated) data source.
 * Supports batch read through DataSource V1.
 */
class DefaultSource extends RelationProvider with DataSourceRegister {

  // Register this data source under format name "//blp/refdata"
  override def shortName(): String = "//blp/refdata"

  /**
   * This is called when `.format("//blp/refdata").load(...)` is invoked.
   * You get access to the Spark SQLContext and the user-specified options.
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): BaseRelation = {
    // Simply construct the BlpRelation using SQLContext and all parameters.
    BlpRelation(sqlContext, parameters)
  }
}
