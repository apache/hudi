/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cdc

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, HoodieDataSourceHelper, HoodieTableSchema}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor
import org.apache.hudi.common.table.cdc.HoodieCDCOperation._
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode._
import org.apache.hudi.common.table.cdc.HoodieCDCUtils._
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Success, Try}

/**
 * Hoodie CDC Relation extends Spark's [[BaseRelation]], provide the schema of cdc
 * and the [[buildScan]] to return the change-data in a specified range.
 */
class CDCRelation(
    sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    startInstant: String,
    endInstant: String,
    rangeType: RangeType = InstantRange.RangeType.OPEN_CLOSED
) extends Logging {

  imbueConfigs(sqlContext)

  val spark: SparkSession = sqlContext.sparkSession

  val (tableAvroSchema, _) = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val avroSchema = Try(schemaUtil.getTableAvroSchema) match {
      case Success(schema) => schema
      case Failure(e) =>
        throw new IllegalArgumentException("Failed to fetch schema from the table", e)
    }
    // try to find internalSchema
    val internalSchemaFromMeta = try {
      schemaUtil.getTableInternalSchemaFromCommitMetadata.orElse(InternalSchema.getEmptyInternalSchema)
    } catch {
      case _: Exception => InternalSchema.getEmptyInternalSchema
    }
    (avroSchema, internalSchemaFromMeta)
  }

  val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  val cdcExtractor: HoodieCDCExtractor =
    new HoodieCDCExtractor(
      metaClient,
      InstantRange.builder()
        .startInstant(startInstant)
        .endInstant(endInstant)
        .nullableBoundary(true)
        .rangeType(rangeType).build(),
      false)

  def schema: StructType = CDCRelation.FULL_CDC_SPARK_SCHEMA

  def imbueConfigs(sqlContext: SQLContext): Unit = {
    // Disable vectorized reading for CDC relation
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }
}

object CDCRelation {

  val CDC_OPERATION_DELETE: UTF8String = UTF8String.fromString(DELETE.getValue)
  val CDC_OPERATION_INSERT: UTF8String = UTF8String.fromString(INSERT.getValue)
  val CDC_OPERATION_UPDATE: UTF8String = UTF8String.fromString(UPDATE.getValue)

  /**
   * CDC Schema For Spark.
   * Also it's schema when `hoodie.table.cdc.supplemental.logging.mode` is [[DATA_BEFORE_AFTER]].
   * Here we use the debezium format.
   */
  val FULL_CDC_SPARK_SCHEMA: StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_COMMIT_TIMESTAMP, StringType),
        StructField(CDC_BEFORE_IMAGE, StringType),
        StructField(CDC_AFTER_IMAGE, StringType)
      )
    )
  }

  /**
   * CDC Schema For Spark when `hoodie.table.cdc.supplemental.logging.mode` is [[OP_KEY_ONLY]].
   */
  val MIN_CDC_SPARK_SCHEMA: StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_RECORD_KEY, StringType)
      )
    )
  }

  /**
   * CDC Schema For Spark when `hoodie.table.cdc.supplemental.logging.mode` is [[DATA_BEFORE]].
   */
  val CDC_WITH_BEFORE_SPARK_SCHEMA: StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_RECORD_KEY, StringType),
        StructField(CDC_BEFORE_IMAGE, StringType)
      )
    )
  }

  def isCDCEnabled(metaClient: HoodieTableMetaClient): Boolean = {
    metaClient.getTableConfig.isCDCEnabled
  }

  /**
   * The only approach to create the CDC relation.
   */
  def getCDCRelation(
      sqlContext: SQLContext,
      metaClient: HoodieTableMetaClient,
      options: Map[String, String],
      rangeType: RangeType = RangeType.OPEN_CLOSED): CDCRelation = {

    if (!isCDCEnabled(metaClient)) {
      throw new IllegalArgumentException(s"It isn't a CDC hudi table on ${metaClient.getBasePath}")
    }

    val startCompletionTime = options.getOrElse(DataSourceReadOptions.START_COMMIT.key(),
      throw new HoodieException(s"CDC Query should provide the valid start completion time "
        + s"through the option ${DataSourceReadOptions.START_COMMIT.key()}")
    )
    val endCompletionTime = options.getOrElse(DataSourceReadOptions.END_COMMIT.key(),
      getTimestampOfLatestInstant(metaClient)
    )

    new CDCRelation(sqlContext, metaClient, startCompletionTime, endCompletionTime, rangeType)
  }

  def getTimestampOfLatestInstant(metaClient: HoodieTableMetaClient): String = {
    val latestInstant = metaClient.getActiveTimeline.lastInstant()
    if (latestInstant.isPresent) {
      latestInstant.get().requestedTime
    } else {
      throw new HoodieException("No valid instant in Active Timeline.")
    }
  }
}
