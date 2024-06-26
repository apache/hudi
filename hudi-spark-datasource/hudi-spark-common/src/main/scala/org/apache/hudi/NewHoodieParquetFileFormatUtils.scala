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

import org.apache.hudi.HoodieBaseRelation._
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.config.ConfigProperty
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.config.HoodieBootstrapConfig.DATA_QUERIES_ONLY
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.{HoodieSchemaException, InternalSchema}
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.execution.datasources.parquet.NewHoodieParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileStatusCache, HadoopFsRelation}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class NewHoodieParquetFileFormatUtils(val sqlContext: SQLContext,
                                      val metaClient: HoodieTableMetaClient,
                                      val optParamsInput: Map[String, String],
                                      private val schemaSpec: Option[StructType]) extends SparkAdapterSupport {
  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected val optParams: Map[String, String] = optParamsInput.filter(kv => !kv._1.equals(DATA_QUERIES_ONLY.key()))
  protected def tableName: String = metaClient.getTableConfig.getTableName

  protected lazy val resolver: Resolver = sparkSession.sessionState.analyzer.resolver

  private lazy val metaFieldNames = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  protected lazy val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
  protected lazy val jobConf = new JobConf(conf)

  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  protected lazy val basePath: StoragePath = metaClient.getBasePath

  protected lazy val (tableAvroSchema: Schema, internalSchemaOpt: Option[InternalSchema]) = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val internalSchemaOpt = if (!isSchemaEvolutionEnabledOnRead(optParams, sparkSession)) {
      None
    } else {
      Try {
        specifiedQueryTimestamp.map(schemaResolver.getTableInternalSchemaFromCommitMetadata)
          .getOrElse(schemaResolver.getTableInternalSchemaFromCommitMetadata)
      } match {
        case Success(internalSchemaOpt) => toScalaOption(internalSchemaOpt)
        case Failure(e) =>
          None
      }
    }

    val (name, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
    val avroSchema = internalSchemaOpt.map { is =>
      AvroInternalSchemaConverter.convert(is, namespace + "." + name)
    } orElse {
      specifiedQueryTimestamp.map(schemaResolver.getTableAvroSchema)
    } orElse {
      schemaSpec.map(s => convertToAvroSchema(s, tableName))
    } getOrElse {
      Try(schemaResolver.getTableAvroSchema) match {
        case Success(schema) => schema
        case Failure(e) =>
          throw new HoodieSchemaException("Failed to fetch schema from the table")
      }
    }

    (avroSchema, internalSchemaOpt)
  }

  protected lazy val tableStructSchema: StructType = {
    val converted = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
    val metaFieldMetadata = sparkAdapter.createCatalystMetadataForMetaField

    // NOTE: Here we annotate meta-fields with corresponding metadata such that Spark (>= 3.2)
    //       is able to recognize such fields as meta-fields
    StructType(converted.map { field =>
      if (metaFieldNames.exists(metaFieldName => resolver(metaFieldName, field.name))) {
        field.copy(metadata = metaFieldMetadata)
      } else {
        field
      }
    })
  }

  protected lazy val preCombineFieldOpt: Option[String] =
    Option(tableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  protected lazy val recordKeyField: String =
    if (tableConfig.populateMetaFields()) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = tableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

  private def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.getTimestamp))

  protected lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  private def getConfigValue(config: ConfigProperty[String],
                             defaultValueOption: Option[String] = Option.empty): String = {
    optParams.getOrElse(config.key(),
      sqlContext.getConf(config.key(), defaultValueOption.getOrElse(config.defaultValue())))
  }

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path
    val shouldOmitPartitionColumns = metaClient.getTableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    val shouldUseBootstrapFastRead = optParams.getOrElse(DATA_QUERIES_ONLY.key(), "false").toBoolean

    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath || shouldUseBootstrapFastRead
  }

  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  protected lazy val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  def hasSchemaOnRead: Boolean = internalSchemaOpt.isDefined

  def getHadoopFsRelation(isMOR: Boolean, isBootstrap: Boolean): BaseRelation = {

    val fileIndex = HoodieFileIndex(sparkSession, metaClient, Some(tableStructSchema), optParams, FileStatusCache.getOrCreate(sparkSession), isMOR)
    val recordMergerImpls = ConfigUtils.split2List(getConfigValue(HoodieWriteConfig.RECORD_MERGER_IMPLS)).asScala.toList
    val recordMergerStrategy = getConfigValue(HoodieWriteConfig.RECORD_MERGER_STRATEGY,
      Option(metaClient.getTableConfig.getRecordMergerStrategy))

    val tableState = // Subset of the state of table's configuration as of at the time of the query
      HoodieTableState(
        tablePath = basePath.toString,
        latestCommitTimestamp = queryTimestamp,
        recordKeyField = recordKeyField,
        preCombineFieldOpt = preCombineFieldOpt,
        usesVirtualKeys = !tableConfig.populateMetaFields(),
        recordPayloadClassName = tableConfig.getPayloadClass,
        metadataConfig = fileIndex.metadataConfig,
        recordMergerImpls = recordMergerImpls,
        recordMergerStrategy = recordMergerStrategy
      )

    val mandatoryFields = if (isMOR) {
      mandatoryFieldsForMerging
    } else {
      Seq.empty
    }
    fileIndex.shouldEmbedFileSlices = true
    HadoopFsRelation(
      location = fileIndex,
      partitionSchema = fileIndex.partitionSchema,
      dataSchema = fileIndex.dataSchema,
      bucketSpec = None,
      fileFormat = new NewHoodieParquetFileFormat(sparkSession.sparkContext.broadcast(tableState),
        sparkSession.sparkContext.broadcast(HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt)),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields, isMOR, isBootstrap),
      optParams)(sparkSession)
  }
}
