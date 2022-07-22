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

import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.HoodieDataSourceHelper
import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.common.table.cdc.CDCUtils._
import org.apache.hudi.common.table.cdc.CDCOperationEnum._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.cdc.CDCExtractor
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class CDCRelation(
    override val sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    cdcSupplementalLogging: Boolean,
    startInstant: String,
    endInstant: String,
    options: Map[String, String]
) extends BaseRelation with PrunedFilteredScan with Logging {

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

  val cdcExtractor: CDCExtractor = new CDCExtractor(metaClient, startInstant, endInstant)

  override final def needConversion: Boolean = false

  override def schema: StructType = CDCRelation.CDC_SPARK_SCHEMA

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val internalRows = buildScan0(requiredColumns, filters)
    internalRows.asInstanceOf[RDD[Row]]
  }

  def buildScan0(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow] = {
    val nameToField = schema.fields.map(f => f.name -> f).toMap
    val requiredSchema = StructType(requiredColumns.map(nameToField))
    val originTableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchema.toString)
    val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = spark,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = Nil,
      options = options,
      hadoopConf = spark.sessionState.newHadoopConf()
    )

    val changes = cdcExtractor.extractor().values().asScala.map { pairs =>
      HoodieCDCFileGroupSplit(
        pairs.asScala.map(pair => (pair.getLeft, pair.getRight)).sortBy(_._1).toArray
      )
    }
    val cdcRdd = new HoodieCDCRDD(
      spark,
      metaClient,
      cdcSupplementalLogging,
      parquetReader,
      originTableSchema,
      schema,
      requiredSchema,
      changes.toArray
    )
    cdcRdd.asInstanceOf[RDD[InternalRow]]
  }
}

object CDCRelation {

  val CDC_OPERATION_DELETE: UTF8String = UTF8String.fromString(DELETE.getValue)
  val CDC_OPERATION_INSERT: UTF8String = UTF8String.fromString(INSERT.getValue)
  val CDC_OPERATION_UPDATE: UTF8String = UTF8String.fromString(UPDATE.getValue)

  /**
   * CDC Schema For Spark.
   * Here we use the debezium format.
   */
  val CDC_SPARK_SCHEMA: StructType = {
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
   * CDC Schema For Spark when `hoodie.table.cdc.supplemental.logging` is false.
   */
  val CDC_LOG_FILE_SPARK_SCHEMA: StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_RECORD_KEY, StringType)
      )
    )
  }

  /**
   * the schema of cdc log file.
   */
  def cdcLogFileSchema(cdcSupplementalLogging: Boolean): StructType = {
    if (cdcSupplementalLogging) {
      CDC_SPARK_SCHEMA
    } else {
      CDC_LOG_FILE_SPARK_SCHEMA
    }
  }

  def isCDCTable(metaClient: HoodieTableMetaClient): Boolean = {
    metaClient.getTableConfig.isCDCEnabled
  }

  /**
   * The only approach to create the CDC relation.
   */
  def getCDCRelation(
      sqlContext: SQLContext,
      metaClient: HoodieTableMetaClient,
      options: Map[String, String]): CDCRelation = {

    if (!isCDCTable(metaClient)) {
      throw new IllegalArgumentException(s"It isn't a CDC hudi table on ${metaClient.getBasePathV2.toString}")
    }

    val startingInstant = options.getOrElse(DataSourceReadOptions.BEGIN_INSTANTTIME.key(),
      throw new HoodieException("CDC Query should provide the valid start version or timestamp")
    )
    val endingInstant = options.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(),
      getTimestampOfLatestInstant(metaClient)
    )
    if (startingInstant > endingInstant) {
      throw new HoodieException(s"This is not a valid range between $startingInstant and $endingInstant")
    }

    val supplementalLogging = metaClient.getTableConfig.isCDCSupplementalLoggingEnabled
    new CDCRelation(sqlContext, metaClient, supplementalLogging, startingInstant, endingInstant, options)
  }

  def getTimestampOfLatestInstant(metaClient: HoodieTableMetaClient): String = {
    val latestInstant = metaClient.getActiveTimeline.lastInstant()
    if (latestInstant.isPresent) {
      latestInstant.get().getTimestamp
    } else {
      throw new HoodieException("No valid instant in Active Timeline.")
    }
  }
}
