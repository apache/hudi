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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.{convertToAvroSchema, isSchemaEvolutionEnabledOnRead}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieFileIndex.getConfigProperties
import org.apache.hudi.common.config.HoodieMetadataConfig.{DEFAULT_METADATA_ENABLE_FOR_READERS, ENABLE}
import org.apache.hudi.common.config.{ConfigProperty, HoodieMetadataConfig, HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.keygen.{CustomAvroKeyGenerator, CustomKeyGenerator, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.StoragePath
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedParquetFileFormat
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}

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
                                                ) extends SparkAdapterSupport with HoodieHadoopFsRelationFactory {
  protected lazy val sparkSession: SparkSession = sqlContext.sparkSession
  protected lazy val optParams: Map[String, String] = options
  protected lazy val hadoopConfig: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
  protected lazy val jobConf = new JobConf(hadoopConfig)

  protected lazy val resolver: Resolver = sparkSession.sessionState.analyzer.resolver
  protected lazy val metaFieldNames: Set[String] = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  protected lazy val tableName: String = metaClient.getTableConfig.getTableName
  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig
  protected lazy val basePath: StoragePath = metaClient.getBasePath
  protected lazy val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  private lazy val keygenTypeHasVariablePartitionCols = isTimestampKeygen || isCustomKeygen

  private lazy val isTimestampKeygen = !isNullOrEmpty(tableConfig.getKeyGeneratorClassName) &&
    (tableConfig.getKeyGeneratorClassName.equals(classOf[TimestampBasedKeyGenerator].getName) ||
    tableConfig.getKeyGeneratorClassName.equals(classOf[TimestampBasedAvroKeyGenerator].getName))

  private lazy val isCustomKeygen = !isNullOrEmpty(tableConfig.getKeyGeneratorClassName) &&
    (tableConfig.getKeyGeneratorClassName.equals(classOf[CustomKeyGenerator].getName) ||
    tableConfig.getKeyGeneratorClassName.equals(classOf[CustomAvroKeyGenerator].getName))

  protected lazy val partitionColumnsToRead: Seq[String] = if (shouldExtractPartitionValuesFromPartitionPath || !keygenTypeHasVariablePartitionCols) {
    Seq.empty
  } else if (isTimestampKeygen) {
    tableConfig.getPartitionFields.orElse(Array.empty).toSeq
  } else {
    //it's custom keygen
    val timestampFieldsOpt = CustomAvroKeyGenerator.getTimestampFields(tableConfig)
    if (timestampFieldsOpt.isPresent) {
      timestampFieldsOpt.get().asScala.toSeq
    } else {
      // timestamp fields above are determined using partition type
      // For older tables the partition type may not be available so falling back to partition fields in those cases
      tableConfig.getPartitionFields.orElse(Array.empty).toSeq
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

  protected lazy val validCommits: String = if (internalSchemaOpt.nonEmpty) {
    timeline.getInstants.iterator.asScala.map(_.getFileName).mkString(",")
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

  protected lazy val preCombineFieldOpt: Option[String] =
    Option(tableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  protected lazy val recordKeyField: String =
    if (tableConfig.populateMetaFields()) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = tableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

  protected lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)
  protected val recordMergerImpls = ConfigUtils.split2List(getConfigValue(HoodieWriteConfig.RECORD_MERGER_IMPLS)).asScala.toList
  protected val recordMergerStrategy = getConfigValue(HoodieWriteConfig.RECORD_MERGER_STRATEGY,
    Option(metaClient.getTableConfig.getRecordMergerStrategy))

  protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path
    val shouldOmitPartitionColumns = metaClient.getTableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath || isBootstrap
  }

  protected lazy val shouldUseRecordPosition: Boolean = checkIfAConfigurationEnabled(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS)

  protected def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.getTimestamp))

  protected def hasSchemaOnRead: Boolean = internalSchemaOpt.isDefined

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  protected def getConfigValue(config: ConfigProperty[String],
                             defaultValueOption: Option[String] = Option.empty): String = {
    optParams.getOrElse(config.key(),
      sqlContext.getConf(config.key(), defaultValueOption.getOrElse(config.defaultValue())))
  }

  protected def checkIfAConfigurationEnabled(config: ConfigProperty[java.lang.Boolean],
                                           defaultValueOption: Option[String] = Option.empty): Boolean = {
    optParams.getOrElse(config.key(),
      sqlContext.getConf(config.key(), defaultValueOption.getOrElse(String.valueOf(config.defaultValue())))).toBoolean
  }
}

class HoodieMergeOnReadSnapshotHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                       override val metaClient: HoodieTableMetaClient,
                                                       override val options: Map[String, String],
                                                       override val schemaSpec: Option[StructType],
                                                       isBootstrap: Boolean)
  extends HoodieBaseHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  val fileIndex = new HoodieFileIndex(
    sparkSession,
    metaClient,
    Some(tableStructSchema),
    optParams,
    FileStatusCache.getOrCreate(sparkSession),
    includeLogFiles = true,
    shouldEmbedFileSlices = true,
    shouldUseStringTypeForTimestampPartitionKeyType = true)

  val configProperties: TypedProperties = getConfigProperties(sparkSession, options, metaClient.getTableConfig)
  val metadataConfig: HoodieMetadataConfig = HoodieMetadataConfig.newBuilder
    .fromProperties(configProperties)
    .enable(configProperties.getBoolean(ENABLE.key, DEFAULT_METADATA_ENABLE_FOR_READERS)
      && HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient)).build

  val tableState: HoodieTableState = // Subset of the state of table's configuration as of at the time of the query
    HoodieTableState(
      tablePath = basePath.toString,
      latestCommitTimestamp = queryTimestamp,
      recordKeyField = recordKeyField,
      preCombineFieldOpt = preCombineFieldOpt,
      usesVirtualKeys = !tableConfig.populateMetaFields(),
      recordPayloadClassName = tableConfig.getPayloadClass,
      metadataConfig = metadataConfig,
      recordMergerImpls = recordMergerImpls,
      recordMergerStrategy = recordMergerStrategy
    )
  val mandatoryFields: Seq[String] = partitionColumnsToRead

  override def buildFileIndex(): FileIndex = fileIndex

  override def buildFileFormat(): FileFormat = {
    if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled && !isBootstrap) {
      new HoodieMultipleBaseFileFormat(sparkSession.sparkContext.broadcast(tableState),
        sparkSession.sparkContext.broadcast(HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt)),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields, true, false, Seq.empty)
    } else {
      new HoodieFileGroupReaderBasedParquetFileFormat(
        tableState, HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields, true, isBootstrap,
        false, fileIndex.isInstanceOf[HoodieCDCFileIndex], validCommits, shouldUseRecordPosition, Seq.empty)
    }
  }

  override def buildDataSchema(): StructType = fileIndex.dataSchema

  override def buildPartitionSchema(): StructType = fileIndex.partitionSchema

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

class HoodieMergeOnReadIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                          override val metaClient: HoodieTableMetaClient,
                                                          override val options: Map[String, String],
                                                          override val schemaSpec: Option[StructType],
                                                          isBootstrap: Boolean)
  extends HoodieMergeOnReadSnapshotHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  override val mandatoryFields: Seq[String] = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++ partitionColumnsToRead

  override val fileIndex = new HoodieIncrementalFileIndex(
    sparkSession, metaClient, schemaSpec, options, FileStatusCache.getOrCreate(sparkSession), true, true)

  override def buildFileFormat(): FileFormat = {
    if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled && !isBootstrap) {
      new HoodieMultipleBaseFileFormat(sparkSession.sparkContext.broadcast(tableState),
        sparkSession.sparkContext.broadcast(HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt)),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields,
        true, true, fileIndex.getRequiredFilters)
    } else {
      new HoodieFileGroupReaderBasedParquetFileFormat(
        tableState, HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields,
        true, isBootstrap, true, fileIndex.isInstanceOf[HoodieCDCFileIndex],
        validCommits, shouldUseRecordPosition, fileIndex.getRequiredFilters)
    }
  }
}

class HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                        override val metaClient: HoodieTableMetaClient,
                                                        override val options: Map[String, String],
                                                        override val schemaSpec: Option[StructType],
                                                        isBootstrap: Boolean)
  extends HoodieMergeOnReadSnapshotHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  override val mandatoryFields: Seq[String] = partitionColumnsToRead

  override val fileIndex = new HoodieFileIndex(
    sparkSession,
    metaClient,
    Some(tableStructSchema),
    optParams,
    FileStatusCache.getOrCreate(sparkSession),
    shouldEmbedFileSlices = true,
    shouldUseStringTypeForTimestampPartitionKeyType = true)

  override def buildFileFormat(): FileFormat = {
    if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled && !isBootstrap) {
      new HoodieMultipleBaseFileFormat(sparkSession.sparkContext.broadcast(tableState),
        sparkSession.sparkContext.broadcast(HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt)),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields, false, false, Seq.empty)
    } else {
      new HoodieFileGroupReaderBasedParquetFileFormat(
        tableState, HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields,
        false, isBootstrap, false, fileIndex.isInstanceOf[HoodieCDCFileIndex], validCommits,
        shouldUseRecordPosition, Seq.empty)
    }
  }
}

class HoodieCopyOnWriteIncrementalHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                          override val metaClient: HoodieTableMetaClient,
                                                          override val options: Map[String, String],
                                                          override val schemaSpec: Option[StructType],
                                                          isBootstrap: Boolean)
  extends HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {

  override val mandatoryFields: Seq[String] = Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++
    preCombineFieldOpt.map(Seq(_)).getOrElse(Seq()) ++ partitionColumnsToRead

  override val fileIndex = new HoodieIncrementalFileIndex(
    sparkSession, metaClient, schemaSpec, options, FileStatusCache.getOrCreate(sparkSession), false, isBootstrap)

  override def buildFileFormat(): FileFormat = {
    if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled && !isBootstrap) {
      new HoodieMultipleBaseFileFormat(sparkSession.sparkContext.broadcast(tableState),
        sparkSession.sparkContext.broadcast(HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt)),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields,
        false, true, fileIndex.getRequiredFilters)
    } else {
      new HoodieFileGroupReaderBasedParquetFileFormat(
        tableState, HoodieTableSchema(tableStructSchema, tableAvroSchema.toString, internalSchemaOpt),
        metaClient.getTableConfig.getTableName, mergeType, mandatoryFields,
        false, isBootstrap, true, fileIndex.isInstanceOf[HoodieCDCFileIndex],
        validCommits, shouldUseRecordPosition, fileIndex.getRequiredFilters)
    }
  }
}

class HoodieMergeOnReadCDCHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                   override val metaClient: HoodieTableMetaClient,
                                                   override val options: Map[String, String],
                                                   override val schemaSpec: Option[StructType],
                                                   isBootstrap: Boolean)
  extends HoodieMergeOnReadIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {
  override val fileIndex = new HoodieCDCFileIndex(
    sparkSession, metaClient, schemaSpec, options, FileStatusCache.getOrCreate(sparkSession), true, true)

  override def buildDataSchema(): StructType = fileIndex.cdcRelation.schema

  override def buildPartitionSchema(): StructType = StructType(Nil)
}

class HoodieCopyOnWriteCDCHadoopFsRelationFactory(override val sqlContext: SQLContext,
                                                  override val metaClient: HoodieTableMetaClient,
                                                  override val options: Map[String, String],
                                                  override val schemaSpec: Option[StructType],
                                                  isBootstrap: Boolean)
  extends HoodieCopyOnWriteIncrementalHadoopFsRelationFactory(sqlContext, metaClient, options, schemaSpec, isBootstrap) {
  override val fileIndex = new HoodieCDCFileIndex(
    sparkSession, metaClient, schemaSpec, options, FileStatusCache.getOrCreate(sparkSession), false, isBootstrap)

  override def buildDataSchema(): StructType = fileIndex.cdcRelation.schema

  override def buildPartitionSchema(): StructType = StructType(Nil)
}


