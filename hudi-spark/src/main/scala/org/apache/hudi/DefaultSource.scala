/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Hoodie Spark Datasource, for reading and writing hoodie datasets
  *
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with Serializable {

  private val log = LogManager.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              optParams: Map[String, String],
                              schema: StructType): BaseRelation = {
    // Add default options for unspecified read options keys.
    val parameters = Map(VIEW_TYPE_OPT_KEY -> DEFAULT_VIEW_TYPE_OPT_VAL) ++: optParams

    val path = parameters.get("path")
    if (path.isEmpty) {
      throw new HoodieException("'path' must be specified.")
    }

    if (parameters(VIEW_TYPE_OPT_KEY).equals(VIEW_TYPE_REALTIME_OPT_VAL)) {
      throw new HoodieException("Realtime view not supported yet via data source. Please use HiveContext route.")
    }

    if (parameters(VIEW_TYPE_OPT_KEY).equals(VIEW_TYPE_INCREMENTAL_OPT_VAL)) {
      new IncrementalRelation(sqlContext, path.get, optParams, schema)
    } else {
      // this is just effectively RO view only, where `path` can contain a mix of
      // non-hoodie/hoodie path files. set the path filter up
      sqlContext.sparkContext.hadoopConfiguration.setClass(
        "mapreduce.input.pathFilter.class",
        classOf[HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter]);

      log.info("Constructing hoodie (as parquet) data source with options :" + parameters)
      // simply return as a regular parquet relation
      DataSource.apply(
        sparkSession = sqlContext.sparkSession,
        userSpecifiedSchema = Option(schema),
        className = "parquet",
        options = parameters)
        .resolveRelation()
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              optParams: Map[String, String],
                              df: DataFrame): BaseRelation = {

    val parameters = HoodieSparkSqlWriter.parametersWithWriteDefaults(optParams)
    HoodieSparkSqlWriter.write(sqlContext, mode, parameters, df)
    createRelation(sqlContext, parameters, df.schema)
  }

  override def createSink(sqlContext: SQLContext,
                          optParams: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val parameters = HoodieSparkSqlWriter.parametersWithWriteDefaults(optParams)
    new HoodieStreamingSink(
      sqlContext,
      parameters,
      partitionColumns,
      outputMode)
  }

  override def shortName(): String = "hudi"
}
