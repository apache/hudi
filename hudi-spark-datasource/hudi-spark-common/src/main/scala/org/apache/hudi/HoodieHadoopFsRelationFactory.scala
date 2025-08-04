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

package org.apache.hudi

import org.apache.hudi.HoodieBaseRelation.{convertToAvroSchema, isSchemaEvolutionEnabledOnRead}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieFileIndex.getConfigProperties
import org.apache.hudi.common.config.{ConfigProperty, HoodieMetadataConfig, HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieMetadataConfig.{DEFAULT_METADATA_ENABLE_FOR_READERS, ENABLE}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.ConfigUtils
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.keygen.{CustomAvroKeyGenerator, CustomKeyGenerator, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait HoodieHadoopFsRelationFactory {
  def build(): HadoopFsRelation
  def buildFileIndex(): FileIndex
  def buildFileFormat(): FileFormat
  def buildPartitionSchema(): StructType
  def buildDataSchema(): StructType
  def buildBucketSpec(): Option[BucketSpec]
  def buildOptions(): Map[String, String]
}

abstract class HoodieBaseHadoopFsRelationFactory(val sqlContext: SQLContext,
                                                 val metaClient: HoodieTableMetaClient,
                                                 val options: Map[String, String],
                                                 val schemaSpec: Option[StructType],
                                                 val isBootstrap: Boolean
                                                ) extends SparkAdapterSupport with HoodieHadoopFsRelationFactory with Logging {
  protected lazy val sparkSession: SparkSession = sqlContext.sparkSession
  protected lazy val optParams: Map[String, String] = options

  private lazy val resolver: Resolver = sparkSession.sessionState.analyzer.resolver
  private lazy val metaFieldNames: Set[String] = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  private lazy val tableName: String = metaClient.getTableConfig.getTableName
  private lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig
  private lazy val basePath: StoragePath = metaClient.getBasePath
  private lazy val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  // very much not recommended to use a partition column as the precombine
  private lazy val partitionColumnsHasPrecombine = preCombineFields.nonEmpty && partitionColumns.exists(col => preCombineFields.contains(col))

  private lazy val keygenTypeHasVariablePartitionCols = isTimestampKeygen || isCustomKeygen

  private lazy val isTimestampKeygen = !isNullOrEmpty(tableConfig.getKeyGeneratorClassName) &&
    (tableConfig.getKeyGeneratorClassName.equals(classOf[TimestampBasedKeyGenerator].getName) ||
    tableConfig.getKeyGeneratorClassName.equals(classOf[TimestampBasedAvroKeyGenerator].getName))

  private lazy val isCustomKeygen = !isNullOrEmpty(tableConfig.getKeyGeneratorClassName) &&
    (tableConfig.getKeyGeneratorClassName.equals(classOf[CustomKeyGenerator].getName) ||
    tableConfig.getKeyGeneratorClassName.equals(classOf[CustomAvroKeyGenerator].getName))

  private lazy val variableTimestampKeygenPartitionCols: Seq[String] = if (isTimestampKeygen) {
    tableConfig.getPartitionFields.orElse(Array.empty).toSeq
  } else if (isCustomKeygen) {
    val timestampFieldsOpt = CustomAvroKeyGenerator.getTimestampFields(tableConfig)
    if (timestampFieldsOpt.isPresent) {
      timestampFieldsOpt.get().asScala.toSeq
    } else {
      // timestamp fields above are determined using partition type
      // For older tables the partition type may not be available so falling back to partition fields in those cases
      tableConfig.getPartitionFields.orElse(Array.empty).toSeq
    }
  } else {
    Seq.empty
  }

  protected lazy val partitionColumnsToRead: Seq[String] = {
    if (shouldExtractPartitionValuesFromPartitionPath) {
      Seq.empty
    } else if (partitionColumnsHasPrecombine) {
      logWarning(s"Not recommended for field '${preCombineFields}' to be both precombine and partition")
      if (keygenTypeHasVariablePartitionCols) {
        // still need to read any timestamp/custom keygen timestamp columns
        if (variableTimestampKeygenPartitionCols.exists(col => preCombineFields.contains(col))) {
          // precombine is already included in the list
          variableTimestampKeygenPartitionCols
        } else {
          // precombine is not included in the list so we append it
          variableTimestampKeygenPartitionCols ++ preCombineFields
        }
      } else {
        // not timestamp/custom keygen so just need to read precombine
        preCombineFields
      }
    } else if (keygenTypeHasVariablePartitionCols) {
      variableTimestampKeygenPartitionCols
    } else {
      Seq.empty
    }
  }

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
        case Failure(e) => throw e
      }
    }

    (avroSchema, internalSchemaOpt)
  }

  private lazy val validCommits: String = if (internalSchemaOpt.nonEmpty) {
    val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator
    timeline.getInstants.iterator.asScala.map(instant => instantFileNameGenerator.getFileName(instant)).mkString(",")
  } else {
    ""
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

  protected lazy val preCombineFields: List[String] = {
    val tablePrecombineFields = tableConfig.getPreCombineFields
    if (tablePrecombineFields.isEmpty) {
      DataSourceOptionsHelper.getPreCombineFields(optParams)
        .orElse(java.util.Collections.emptyList[String])
        .asScala.toList
    } else {
      tablePrecombineFields.asScala.toList
    }
  }

  private lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  private val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path
    val shouldOmitPartitionColumns = metaClient.getTableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath || isBootstrap
  }

  private lazy val shouldUseRecordPosition: Boolean = checkIfAConfigurationEnabled(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS)

  private lazy val queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.requestedTime))

  private lazy val timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  private def checkIfAConfigurationEnabled(config: ConfigProperty[java.lang.Boolean],
                                           defaultValueOption: Option[String] = Option.empty): Boolean = {
    optParams.getOrElse(config.key(),
      sqlContext.getConf(config.key(), defaultValueOption.getOrElse(String.valueOf(config.defaultValue())))).toBoolean
  }

  protected lazy val fileStatusCache: FileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  protected def getMandatoryFields: Seq[String] = partitionColumnsToRead

  protected def isMOR: Boolean

  protected def isIncremental: Boolean

  protected def getRequiredFilters: Seq[Filter]

  override def buildFileFormat(): FileFormat = {
    val tableConfig = metaClient.getTableConfig
    new HoodieFileGroupReaderBasedFileFormat(basePath.toString,
      HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt),
      tableConfig.getTableName, queryTimestamp.get, getMandatoryFields, isMOR, isBootstrap,
      isIncremental, validCommits, shouldUseRecordPosition, getRequiredFilters,
      tableConfig.isMultipleBaseFileFormatsEnabled, tableConfig.getBaseFileFormat)
  }

  override def buildBucketSpec(): Option[BucketSpec] = None

  override def buildOptions(): Map[String, String] = optParams
  override def build(): HadoopFsRelation = {
    HadoopFsRelation(
      location = buildFileIndex(),
      partitionSchema = buildPartitionSchema(),
      dataSchema = buildDataSchema(),
      bucketSpec = buildBucketSpec(),
      fileFormat = buildFileFormat(),
      options = buildOptions())(sparkSession)
  }
}

class HoodieMergeOnReadSnapshotHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                       override val metaClient: HoodieTableMetaClient,
                                                       override val options: Map[String, String],
                                                       override val schemaSpec: Option[StructType],
                                                       isBootstrap: Boolean)
  extends HoodieBaseHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {
  private val fileIndex: HoodieFileIndex = new HoodieFileIndex(
    sparkSession,
    metaClient,
    Some(tableStructSchema),
    optParams,
    fileStatusCache,
    includeLogFiles = true,
    shouldEmbedFileSlices = true)

  override def buildFileIndex(): FileIndex = fileIndex

  override protected def isMOR: Boolean = true

  override protected def isIncremental: Boolean = false

  override protected def getRequiredFilters: Seq[Filter] = Seq.empty

  override def buildPartitionSchema(): StructType = fileIndex.partitionSchema

  override def buildDataSchema(): StructType = fileIndex.dataSchema
}

abstract class HoodieBaseMergeOnReadIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                                       override val metaClient: HoodieTableMetaClient,
                                                                       override val options: Map[String, String],
                                                                       override val schemaSpec: Option[StructType],
                                                                       isBootstrap: Boolean)
  extends HoodieBaseHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  override protected def getMandatoryFields: Seq[String] = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++ partitionColumnsToRead

  override protected def isMOR: Boolean = true

  override protected def isIncremental: Boolean = true
}

abstract class HoodieMergeOnReadIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                                   override val metaClient: HoodieTableMetaClient,
                                                                   override val options: Map[String, String],
                                                                   override val schemaSpec: Option[StructType],
                                                                   isBootstrap: Boolean,
                                                                   mergeOnReadIncrementalRelation: MergeOnReadIncrementalRelation)
  extends HoodieBaseMergeOnReadIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  private val incrementalFileIndex = new HoodieIncrementalFileIndex(
    sparkSession, metaClient, schemaSpec, options, fileStatusCache, true, mergeOnReadIncrementalRelation)

  override def buildFileIndex(): HoodieFileIndex = incrementalFileIndex

  override protected def getRequiredFilters: Seq[Filter] = incrementalFileIndex.getRequiredFilters

  override def buildPartitionSchema(): StructType = incrementalFileIndex.partitionSchema

  override def buildDataSchema(): StructType = incrementalFileIndex.dataSchema
}

class HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1(override val sqlContext: SQLContext,
                                                            override val metaClient: HoodieTableMetaClient,
                                                            override val options: Map[String, String],
                                                            override val schemaSpec: Option[StructType],
                                                            isBootstrap: Boolean)
  extends HoodieMergeOnReadIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap,
    MergeOnReadIncrementalRelationV1(sqlContext, options, metaClient, schemaSpec))

class HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2(override val sqlContext: SQLContext,
                                                            override val metaClient: HoodieTableMetaClient,
                                                            override val options: Map[String, String],
                                                            override val schemaSpec: Option[StructType],
                                                            isBootstrap: Boolean,
                                                            rangeType: RangeType = RangeType.CLOSED_CLOSED)
  extends HoodieMergeOnReadIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap,
    MergeOnReadIncrementalRelationV2(sqlContext, options, metaClient, schemaSpec, None, rangeType))

class HoodieMergeOnReadCDCHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                  override val metaClient: HoodieTableMetaClient,
                                                  override val options: Map[String, String],
                                                  override val schemaSpec: Option[StructType],
                                                  isBootstrap: Boolean,
                                                  rangeType: RangeType = RangeType.OPEN_CLOSED)
  extends HoodieBaseMergeOnReadIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {
  private val hoodieCDCFileIndex = new HoodieCDCFileIndex(
    sparkSession, metaClient, schemaSpec, options, fileStatusCache, true, rangeType)

  override def buildFileIndex(): HoodieFileIndex = hoodieCDCFileIndex

  override def buildDataSchema(): StructType = hoodieCDCFileIndex.cdcRelation.schema

  override def buildPartitionSchema(): StructType = StructType(Nil)

  override protected def getRequiredFilters: Seq[Filter] = Seq.empty
}

class HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                        override val metaClient: HoodieTableMetaClient,
                                                        override val options: Map[String, String],
                                                        override val schemaSpec: Option[StructType],
                                                        isBootstrap: Boolean)
  extends HoodieBaseHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  val fileIndex: HoodieFileIndex = new HoodieFileIndex(
    sparkSession,
    metaClient,
    Some(tableStructSchema),
    optParams,
    fileStatusCache,
    shouldEmbedFileSlices = true)

  override def buildFileIndex(): HoodieFileIndex = fileIndex

  override protected def isMOR: Boolean = false

  override protected def isIncremental: Boolean = false

  override protected def getRequiredFilters: Seq[Filter] = Seq.empty

  override def buildPartitionSchema(): StructType = fileIndex.partitionSchema

  override def buildDataSchema(): StructType = fileIndex.dataSchema
}

abstract class HoodieBaseCopyOnWriteIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                                       override val metaClient: HoodieTableMetaClient,
                                                                       override val options: Map[String, String],
                                                                       override val schemaSpec: Option[StructType],
                                                                       isBootstrap: Boolean)
  extends HoodieBaseHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  override protected def getMandatoryFields(): Seq[String] = Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++
    preCombineFields ++ partitionColumnsToRead

  override protected def isMOR: Boolean = false

  override protected def isIncremental: Boolean = true

}

abstract class HoodieCopyOnWriteIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                                       override val metaClient: HoodieTableMetaClient,
                                                                       override val options: Map[String, String],
                                                                       override val schemaSpec: Option[StructType],
                                                                       isBootstrap: Boolean,
                                                                       mergeOnReadIncrementalRelation: MergeOnReadIncrementalRelation)
  extends HoodieBaseCopyOnWriteIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  private val incrementalFileIndex = new HoodieIncrementalFileIndex(
    sparkSession, metaClient, schemaSpec, options, fileStatusCache, false, mergeOnReadIncrementalRelation)

  override def buildFileIndex(): HoodieFileIndex = incrementalFileIndex

  override protected def getRequiredFilters: Seq[Filter] = incrementalFileIndex.getRequiredFilters

  override def buildPartitionSchema(): StructType = incrementalFileIndex.partitionSchema

  override def buildDataSchema(): StructType = incrementalFileIndex.dataSchema
}

class HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1(override val sqlContext: SQLContext,
                                                            override val metaClient: HoodieTableMetaClient,
                                                            override val options: Map[String, String],
                                                            override val schemaSpec: Option[StructType],
                                                            isBootstrap: Boolean)
extends HoodieCopyOnWriteIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap,
  MergeOnReadIncrementalRelationV1(sqlContext, options, metaClient, schemaSpec))

class HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2(override val sqlContext: SQLContext,
                                                            override val metaClient: HoodieTableMetaClient,
                                                            override val options: Map[String, String],
                                                            override val schemaSpec: Option[StructType],
                                                            isBootstrap: Boolean,
                                                            rangeType: RangeType)
  extends HoodieCopyOnWriteIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap,
    MergeOnReadIncrementalRelationV2(sqlContext, options, metaClient, schemaSpec, None, rangeType))

class HoodieCopyOnWriteCDCHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                  override val metaClient: HoodieTableMetaClient,
                                                  override val options: Map[String, String],
                                                  override val schemaSpec: Option[StructType],
                                                  isBootstrap: Boolean,
                                                  rangeType: RangeType = RangeType.OPEN_CLOSED)
  extends HoodieBaseCopyOnWriteIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  private val hoodieCDCFileIndex = new HoodieCDCFileIndex(
    sparkSession, metaClient, schemaSpec, options, fileStatusCache, false, rangeType)
  override def buildFileIndex(): HoodieFileIndex = hoodieCDCFileIndex

  override def buildDataSchema(): StructType = hoodieCDCFileIndex.cdcRelation.schema

  override def buildPartitionSchema(): StructType = StructType(Nil)

  override protected def getRequiredFilters: Seq[Filter] = Seq.empty
}


