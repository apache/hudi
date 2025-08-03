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

import org.apache.hudi.AutoRecordKeyGenerationUtils.mayBeValidateParamsForAutoGenerationOfRecordKeys
import org.apache.hudi.AvroConversionUtils.{convertAvroSchemaToStructType, convertStructTypeToAvroSchema, getAvroRecordNameAndNamespace}
import org.apache.hudi.DataSourceOptionsHelper.fetchMissingWriteConfigsFromTableConfig
import org.apache.hudi.DataSourceUtils.SparkDataSourceWriteStatusValidator
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.{toProperties, toScalaOption}
import org.apache.hudi.HoodieSparkSqlWriter.StreamingWriteParams
import org.apache.hudi.HoodieSparkSqlWriterInternal.{handleInsertDuplicates, shouldDropDuplicatesForInserts, shouldFailWhenDuplicatesFound}
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.commit.{DatasetBucketRescaleCommitActionExecutor, DatasetBulkInsertCommitActionExecutor, DatasetBulkInsertOverwriteCommitActionExecutor, DatasetBulkInsertOverwriteTableCommitActionExecutor}
import org.apache.hudi.common.config._
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model._
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.common.util.{CommitUtils, Option => HOption, StringUtils}
import org.apache.hudi.common.util.ConfigUtils.getAllConfigKeys
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieBootstrapConfig.{BASE_PATH, INDEX_CLASS_NAME}
import org.apache.hudi.config.HoodieWriteConfig.{SPARK_SQL_MERGE_INTO_PREPPED_KEY, WRITE_TABLE_VERSION}
import org.apache.hudi.exception.{HoodieException, HoodieRecordCreationException, HoodieWriteConflictException}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.{HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.keygen.{BaseKeyGenerator, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.metrics.Metrics
import org.apache.hudi.storage.HoodieStorage
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.sync.common.util.SyncUtilHelpers
import org.apache.hudi.sync.common.util.SyncUtilHelpers.getHoodieMetaSyncException
import org.apache.hudi.util.{SparkConfigUtils, SparkKeyGenUtils}
import org.apache.hudi.util.SparkConfigUtils.getStringWithAltKeys

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.HoodieDataTypeUtils.tryOverrideParquetWriteLegacyFormatProperty
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object HoodieSparkSqlWriter {

  case class StreamingWriteParams(hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
                                  asyncCompactionTriggerFn: Option[SparkRDDWriteClient[_] => Unit] = Option.empty,
                                  asyncClusteringTriggerFn: Option[SparkRDDWriteClient[_] => Unit] = Option.empty,
                                  extraPreCommitFn: Option[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]] = Option.empty)

  /**
   * Controls whether incoming batch's schema's nullability constraints should be canonicalized
   * relative to the table's schema. For ex, in case field A is marked as null-able in table's schema, but is marked
   * as non-null in the incoming batch, w/o canonicalization such write might fail as we won't be able to read existing
   * null records from the table (for updating, for ex). Note, that this config has only effect when
   * 'hoodie.datasource.write.reconcile.schema' is set to false
   *
   * NOTE: This is an internal config that is not exposed to the public
   */
  val CANONICALIZE_SCHEMA: ConfigProperty[Boolean] =
    ConfigProperty.key("hoodie.internal.write.schema.canonicalize")
      .defaultValue(true)

  /**
   * For merge into from spark-sql, we need some special handling. for eg, schema validation should be disabled
   * for writes from merge into. This config is used for internal purposes.
   */
  val SQL_MERGE_INTO_WRITES: ConfigProperty[Boolean] =
    ConfigProperty.key("hoodie.internal.sql.merge.into.writes")
      .defaultValue(false)

  /**
   * For spark streaming use-cases, holds the batch Id.
   */
  val SPARK_STREAMING_BATCH_ID = "hoodie.internal.spark.streaming.batch.id"

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            optParams: Map[String, String],
            sourceDf: DataFrame,
            streamingWritesParamsOpt: Option[StreamingWriteParams] = Option.empty,
            hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {
    new HoodieSparkSqlWriterInternal().write(sqlContext, mode, optParams, sourceDf, streamingWritesParamsOpt, hoodieWriteClient)
  }

  def bootstrap(sqlContext: SQLContext,
                mode: SaveMode,
                optParams: Map[String, String],
                df: DataFrame,
                hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
                streamingWritesParamsOpt: Option[StreamingWriteParams] = Option.empty,
                hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty): Boolean = {
    new HoodieSparkSqlWriterInternal().bootstrap(sqlContext, mode, optParams, df, hoodieTableConfigOpt, streamingWritesParamsOpt, hoodieWriteClient)
  }

  def cleanup(): Unit = {
    Metrics.shutdownAllMetrics()
  }

  def getBulkInsertRowConfig(writerSchema: org.apache.hudi.common.util.Option[Schema], hoodieConfig: HoodieConfig,
                             basePath: String, tblName: String): HoodieWriteConfig = {
    var writerSchemaStr: String = null
    if (writerSchema.isPresent) {
      writerSchemaStr = writerSchema.get().toString
    }
    // Make opts mutable since it could be modified by tryOverrideParquetWriteLegacyFormatProperty
    val optsWithoutSchema = mutable.Map() ++ hoodieConfig.getProps.asScala
    val opts = if (writerSchema.isPresent) {
      optsWithoutSchema ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key -> writerSchemaStr)
    } else {
      optsWithoutSchema
    }

    if (writerSchema.isPresent) {
      // Auto set the value of "hoodie.parquet.writelegacyformat.enabled"
      tryOverrideParquetWriteLegacyFormatProperty(opts.asJava, convertAvroSchemaToStructType(writerSchema.get))
    }

    DataSourceUtils.createHoodieConfig(writerSchemaStr, basePath, tblName, opts.asJava)
  }
}

class HoodieSparkSqlWriterInternal {

  private val log = LoggerFactory.getLogger(getClass)
  private var tableExists: Boolean = false
  private var asyncCompactionTriggerFnDefined: Boolean = false
  private var asyncClusteringTriggerFnDefined: Boolean = false

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            optParams: Map[String, String],
            sourceDf: DataFrame,
            streamingWritesParamsOpt: Option[StreamingWriteParams] = Option.empty,
            hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {

    val retryWrite: () => (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = () => {
      var succeeded = false
      var counter = 0
      val maxRetry: Integer = Integer.parseInt(optParams.getOrElse(HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key(), HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.defaultValue().toString))
      var toReturn: (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = null

      while (counter <= maxRetry && !succeeded) {
        try {
          toReturn = writeInternal(sqlContext, mode, optParams, sourceDf, streamingWritesParamsOpt, hoodieWriteClient)
          if (counter > 0) {
            log.warn(s"Succeeded with attempt no $counter")
          }
          succeeded = true
        } catch {
          case e: HoodieWriteConflictException =>
            val writeConcurrencyMode = optParams.getOrElse(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue())
            if (WriteConcurrencyMode.supportsMultiWriter(writeConcurrencyMode) && counter < maxRetry) {
              counter += 1
              log.warn(s"Conflict found. Retrying again for attempt no $counter")
            } else {
              throw e
            }
        }
      }
      toReturn
    }
    retryWrite()
  }

  private def writeInternal(sqlContext: SQLContext,
                            mode: SaveMode,
                            optParams: Map[String, String],
                            sourceDf: DataFrame,
                            streamingWritesParamsOpt: Option[StreamingWriteParams] = Option.empty,
                            hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)

    val spark = sqlContext.sparkSession
    val sparkContext = sqlContext.sparkContext

    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    var tableConfig = getHoodieTableConfig(sparkContext, path, mode, streamingWritesParamsOpt.map(_.hoodieTableConfigOpt).orElse(Option.apply(Option.empty)).get)
    // get params w/o injecting default and validate
    val paramsWithoutDefaults = HoodieWriterUtils.getParamsWithAlternatives(optParams)
    val originKeyGeneratorClassName = HoodieWriterUtils.getOriginKeyGenerator(paramsWithoutDefaults)
    val timestampKeyGeneratorConfigs = extractConfigsRelatedToTimestampBasedKeyGenerator(
      originKeyGeneratorClassName, paramsWithoutDefaults)

    // Validate datasource and tableconfig keygen are the same
    validateKeyGeneratorConfig(originKeyGeneratorClassName, tableConfig)
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite)

    asyncCompactionTriggerFnDefined = streamingWritesParamsOpt.map(_.asyncCompactionTriggerFn.isDefined).orElse(Some(false)).get
    asyncClusteringTriggerFnDefined = streamingWritesParamsOpt.map(_.asyncClusteringTriggerFn.isDefined).orElse(Some(false)).get
    // re-use table configs and inject defaults.
    var (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode, streamingWritesParamsOpt.isDefined)
    val databaseName = hoodieConfig.getStringOrDefault(HoodieTableConfig.DATABASE_NAME, "")
    val tblName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TBL_NAME,
      s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.").trim
    val tableIdentifier = TableIdentifier(tblName, if (databaseName.isEmpty) None else Some(databaseName))

    assert(!StringUtils.isNullOrEmpty(hoodieConfig.getString(HoodieWriteConfig.TBL_NAME)),
      s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.")

    sparkContext.getConf.getOption("spark.serializer") match {
      case Some(ser) if ser.equals("org.apache.spark.serializer.KryoSerializer") =>
      case _ => throw new HoodieException("hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer")
    }
    val tableType = HoodieTableType.valueOf(hoodieConfig.getString(TABLE_TYPE))
    val operation = deduceOperation(hoodieConfig, paramsWithoutDefaults, sourceDf)

    val preppedSparkSqlMergeInto = parameters.getOrElse(SPARK_SQL_MERGE_INTO_PREPPED_KEY, "false").toBoolean
    val preppedSparkSqlWrites = parameters.getOrElse(SPARK_SQL_WRITES_PREPPED_KEY, "false").toBoolean
    val preppedWriteOperation = canDoPreppedWrites(hoodieConfig, parameters, operation, sourceDf)

    val jsc = new JavaSparkContext(sparkContext)
    if (streamingWritesParamsOpt.map(_.asyncCompactionTriggerFn.isDefined).orElse(Some(false)).get) {
      if (jsc.getConf.getOption(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY).isDefined) {
        jsc.setLocalProperty("spark.scheduler.pool", SparkConfigs.SPARK_DATASOURCE_WRITER_POOL_NAME)
      }
    }

    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(TypedProperties.copy(hoodieConfig.getProps))
    val tableVersion = Integer.valueOf(getStringWithAltKeys(parameters, HoodieWriteConfig.WRITE_TABLE_VERSION))
    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      (false, common.util.Option.empty(), common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
    } else {
      // Handle various save modes
      handleSaveModes(sqlContext.sparkSession, mode, basePath, tableConfig, tblName, operation, fs)
      val partitionColumns = SparkKeyGenUtils.getPartitionColumns(keyGenerator, toProperties(parameters), false)
      val partitionColumnsForKeyGenerator = SparkKeyGenUtils.getPartitionColumnsForKeyGenerator(toProperties(parameters), HoodieTableVersion.fromVersionCode(tableVersion))
      val timelineTimeZone = HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE))
      val tableMetaClient = if (tableExists) {
        HoodieInstantTimeGenerator.setCommitTimeZone(timelineTimeZone)
        HoodieTableMetaClient.builder
          .setConf(HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration))
          .setBasePath(path)
          .build()
      } else {
        val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.BASE_FILE_FORMAT)
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_HISTORY_PATH)
        val populateMetaFields = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)
        val useBaseFormatMetaFile = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT);
        val payloadClass = hoodieConfig.getString(DataSourceWriteOptions.PAYLOAD_CLASS_NAME)
        val recordMergeStrategyId = hoodieConfig.getString(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID)
        val keyGenProp =
          if (StringUtils.nonEmpty(hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)))
            hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)
          else KeyGeneratorType.getKeyGeneratorClassName(hoodieConfig)
        val tableFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.TABLE_FORMAT)
        val autoUpgrade = hoodieConfig.getBooleanOrDefault(
          HoodieWriteConfig.AUTO_UPGRADE_VERSION, HoodieWriteConfig.AUTO_UPGRADE_VERSION.defaultValue())
        HoodieTableMetaClient.newTableBuilder()
          .setTableType(tableType)
          .setTableVersion(tableVersion)
          .setAutoUpgrade(autoUpgrade)
          .setTableFormat(tableFormat)
          .setDatabaseName(databaseName)
          .setTableName(tblName)
          .setBaseFileFormat(baseFileFormat)
          .setArchiveLogFolder(archiveLogFolder)
          // we can't fetch preCombine field from hoodieConfig object, since it falls back to "ts" as default value,
          // but we are interested in what user has set, hence fetching from optParams.
          .setPreCombineFields(optParams.getOrElse(PRECOMBINE_FIELD.key(), null))
          .setPartitionFields(partitionColumnsForKeyGenerator)
          .setPopulateMetaFields(populateMetaFields)
          .setRecordKeyFields(hoodieConfig.getString(RECORDKEY_FIELD))
          .setSecondaryKeyFields(hoodieConfig.getString(SECONDARYKEY_COLUMN_NAME))
          .setCDCEnabled(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED))
          .setCDCSupplementalLoggingMode(hoodieConfig.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE))
          .setKeyGeneratorClassProp(keyGenProp)
          .set(timestampKeyGeneratorConfigs.asJava.asInstanceOf[java.util.Map[String, Object]])
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .setShouldDropPartitionColumns(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.DROP_PARTITION_COLUMNS))
          .setCommitTimezone(timelineTimeZone)
          .setPayloadClassName(payloadClass)
          .setRecordMergeStrategyId(recordMergeStrategyId)
          .setRecordMergeMode(RecordMergeMode.getValue(hoodieConfig.getString(HoodieWriteConfig.RECORD_MERGE_MODE)))
          .setMultipleBaseFileFormatsEnabled(hoodieConfig.getBoolean(HoodieTableConfig.MULTIPLE_BASE_FILE_FORMATS_ENABLE))
          .setTableFormat(hoodieConfig.getStringOrDefault(HoodieTableConfig.TABLE_FORMAT))
          .initTable(HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration), path)
      }

      // take care of partition level bucket index which is simple bucket index.
      // for BUCKET_RESCALE action will set related configs in call command, so skip here.
      if (hoodieConfig.getStringOrDefault(HoodieIndexConfig.INDEX_TYPE, "") == HoodieIndex.IndexType.BUCKET.name
        && PartitionBucketIndexUtils.isPartitionSimpleBucketIndex(tableMetaClient.getStorageConf, basePath.toString)
        && hoodieConfig.getStringOrDefault(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE, "") != WriteOperationType.BUCKET_RESCALE.value()) {

        val latestHashingConfig = PartitionBucketIndexHashingConfig.loadingLatestHashingConfig(tableMetaClient)
        hoodieConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key, latestHashingConfig.getExpressions)
        hoodieConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, latestHashingConfig.getDefaultBucketNumber.toString)
        hoodieConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.key, latestHashingConfig.getRule)
        parameters = parameters ++ Map(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key -> latestHashingConfig.getExpressions)
        parameters = parameters ++ Map(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key -> latestHashingConfig.getDefaultBucketNumber.toString)
        parameters = parameters ++ Map(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.key -> latestHashingConfig.getRule)
      }

      var instantTime: String = null
      tableConfig = tableMetaClient.getTableConfig

      val commitActionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)

      // Register Avro classes ([[Schema]], [[GenericData]]) w/ Kryo
      sparkContext.getConf.registerKryoClasses(
        Array(classOf[GenericData],
          classOf[Schema]))

      val shouldReconcileSchema = parameters(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
      val latestTableSchemaOpt = getLatestTableSchema(spark, tableIdentifier, tableMetaClient)
      val df = if (preppedWriteOperation || preppedSparkSqlWrites || preppedSparkSqlMergeInto || sourceDf.isStreaming) {
        sourceDf
      } else {
        sourceDf.drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)
      }
      // NOTE: We need to make sure that upon conversion of the schemas b/w Catalyst's [[StructType]] and
      //       Avro's [[Schema]] we're preserving corresponding "record-name" and "record-namespace" that
      //       play crucial role in establishing compatibility b/w schemas
      val (avroRecordName, avroRecordNamespace) = latestTableSchemaOpt.map(s => (s.getName, s.getNamespace))
        .getOrElse(getAvroRecordNameAndNamespace(tblName))

      val sourceSchema = convertStructTypeToAvroSchema(df.schema, avroRecordName, avroRecordNamespace)
      val internalSchemaOpt = HoodieSchemaUtils.getLatestTableInternalSchema(hoodieConfig, tableMetaClient).orElse {
        // In case we need to reconcile the schema and schema evolution is enabled,
        // we will force-apply schema evolution to the writer's schema
        if (shouldReconcileSchema && hoodieConfig.getBooleanOrDefault(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
          val allowOperationMetaDataField = parameters.getOrElse(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key(), "false").toBoolean
          Some(AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(latestTableSchemaOpt.getOrElse(sourceSchema), allowOperationMetaDataField)))
        } else {
          None
        }
      }

      val (writeResult: HoodieWriteResult, writeClient: SparkRDDWriteClient[_]) =
        operation match {
          case WriteOperationType.DELETE | WriteOperationType.DELETE_PREPPED =>
            mayBeValidateParamsForAutoGenerationOfRecordKeys(parameters, hoodieConfig)
            val genericRecords = HoodieSparkUtils.createRdd(df, avroRecordName, avroRecordNamespace)
            // Convert to RDD[HoodieKey]
            val hoodieKeysAndLocationsToDelete = genericRecords.mapPartitions(it => {
              val keyGenerator: Option[BaseKeyGenerator] = if (preppedSparkSqlWrites || preppedWriteOperation) {
                None
              } else {
                Some(HoodieSparkKeyGeneratorFactory.createKeyGenerator(TypedProperties.copy(hoodieConfig.getProps))
                  .asInstanceOf[BaseKeyGenerator])
              }
              it.map { avroRec =>
                HoodieCreateRecordUtils.getHoodieKeyAndMaybeLocationFromAvroRecord(keyGenerator, avroRec, preppedSparkSqlWrites || preppedWriteOperation, preppedSparkSqlWrites || preppedSparkSqlMergeInto || preppedWriteOperation)
              }
            }).toJavaRDD()

            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            // Create a HoodieWriteClient & issue the delete.
            val internalSchemaOpt = HoodieSchemaUtils.getLatestTableInternalSchema(hoodieConfig, tableMetaClient)
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
                null, path, tblName,
                addSchemaEvolutionParameters(parameters, internalSchemaOpt).asJava))
              .asInstanceOf[SparkRDDWriteClient[_]]

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              streamingWritesParamsOpt.map(_.asyncCompactionTriggerFn.get.apply(client))
            }
            if (isAsyncClusteringEnabled(client, parameters)) {
              streamingWritesParamsOpt.map(_.asyncClusteringTriggerFn.get.apply(client))
            }

            // Issue deletes
            instantTime = client.startCommit(commitActionType)
            val writeStatuses = DataSourceUtils.doDeleteOperation(client, hoodieKeysAndLocationsToDelete, instantTime, preppedSparkSqlWrites || preppedWriteOperation)
            (writeStatuses, client)

          case WriteOperationType.DELETE_PARTITION =>
            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(TypedProperties.copy(hoodieConfig.getProps))
            val tableMetaClient = HoodieTableMetaClient.builder
              .setConf(HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration))
              .setBasePath(basePath.toString)
              .build()
            // Get list of partitions to delete
            val partitionsToDelete = if (parameters.contains(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key())) {
              val partitionColsToDelete = parameters(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key()).split(",")
              java.util.Arrays.asList(resolvePartitionWildcards(java.util.Arrays.asList(partitionColsToDelete: _*).asScala.toList, jsc,
                tableMetaClient.getStorage, hoodieConfig, basePath.toString, tableMetaClient): _*)
            } else {
              val genericRecords = HoodieSparkUtils.createRdd(df, avroRecordName, avroRecordNamespace)
              genericRecords.map(gr => keyGenerator.getKey(gr).getPartitionPath).toJavaRDD().distinct().collect()
            }

            // Issue the delete.
            val schemaStr = new TableSchemaResolver(tableMetaClient).getTableAvroSchema(false).toString
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
                schemaStr, path, tblName,
                parameters.asJava))
              .asInstanceOf[SparkRDDWriteClient[_]]
            // Issue delete partitions
            instantTime = client.startDeletePartitionCommit(tableMetaClient)
            val writeResult =  DataSourceUtils.doDeletePartitionsOperation(client, partitionsToDelete, instantTime)
            (writeResult, client)

          // Here all other (than DELETE, DELETE_PARTITION) write operations are handled
          case _ =>
            // NOTE: Target writer's schema is deduced based on
            //         - Source's schema
            //         - Existing table's schema (including its Hudi's [[InternalSchema]] representation)
            val writerSchema = HoodieSchemaUtils.deduceWriterSchema(sourceSchema, latestTableSchemaOpt, internalSchemaOpt, parameters)

            validateSchemaForHoodieIsDeleted(writerSchema)
            mayBeValidateParamsForAutoGenerationOfRecordKeys(parameters, hoodieConfig)

            // Check whether partition columns should be persisted w/in the data-files, or should
            // be instead omitted from them and simply encoded into the partition path (which is Spark's
            // behavior by default)
            // TODO move partition columns handling down into the handlers
            val shouldDropPartitionColumns = hoodieConfig.getBoolean(DataSourceWriteOptions.DROP_PARTITION_COLUMNS)
            val dataFileSchema = if (shouldDropPartitionColumns) {
              val truncatedSchema = generateSchemaWithoutPartitionColumns(partitionColumns, writerSchema)
              // NOTE: We have to register this schema w/ Kryo to make sure it's able to apply an optimization
              //       allowing it to avoid the need to ser/de the whole schema along _every_ Avro record
              registerAvroSchemasWithKryo(sparkContext, truncatedSchema)
              truncatedSchema
            } else {
              writerSchema
            }

            // Remove meta columns from writerSchema if isPrepped is true.
            val processedDataSchema = if (preppedSparkSqlWrites || preppedSparkSqlMergeInto || preppedWriteOperation) {
              HoodieAvroUtils.removeMetadataFields(dataFileSchema)
            } else {
              dataFileSchema
            }

            // Create a HoodieWriteClient & issue the write.
            val client = hoodieWriteClient.getOrElse {
              val finalOpts = addSchemaEvolutionParameters(parameters, internalSchemaOpt, Some(writerSchema))
              // TODO(HUDI-4772) proper writer-schema has to be specified here
              DataSourceUtils.createHoodieClient(jsc, processedDataSchema.toString, path, tblName, finalOpts.asJava)
            }

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              streamingWritesParamsOpt.map(_.asyncCompactionTriggerFn.get.apply(client))
            }

            if (isAsyncClusteringEnabled(client, parameters)) {
              streamingWritesParamsOpt.map(_.asyncClusteringTriggerFn.get.apply(client))
            }

            // Short-circuit if bulk_insert via row is enabled.
            // scalastyle:off
            if (hoodieConfig.getBoolean(ENABLE_ROW_WRITER) && operation == WriteOperationType.BULK_INSERT) {
              return bulkInsertAsRow(client, parameters, hoodieConfig, df, mode, tblName, basePath, writerSchema, tableConfig)
            }
            // scalastyle:on

            val writeConfig = client.getConfig
            if (writeConfig.getRecordMerger.getRecordType == HoodieRecordType.SPARK && tableType == MERGE_ON_READ && writeConfig.getLogDataBlockFormat.orElse(HoodieLogBlockType.AVRO_DATA_BLOCK) != HoodieLogBlockType.PARQUET_DATA_BLOCK) {
              throw new UnsupportedOperationException(s"${writeConfig.getRecordMerger.getClass.getName} only support parquet log.")
            }
            instantTime = client.startCommit(commitActionType)
            // Convert to RDD[HoodieRecord]
            val hoodieRecords = Try(HoodieCreateRecordUtils.createHoodieRecordRdd(
              HoodieCreateRecordUtils.createHoodieRecordRddArgs(df, writeConfig, parameters, avroRecordName,
                avroRecordNamespace, writerSchema, processedDataSchema, operation, instantTime, preppedSparkSqlWrites,
                preppedSparkSqlMergeInto, preppedWriteOperation))) match {
              case Success(recs) => recs
              case Failure(e) => throw new HoodieRecordCreationException("Failed to create Hoodie Spark Record", e)
            }

            // Remove duplicates from incoming records based on existing keys from storage.
            val dedupedHoodieRecords = handleInsertDuplicates(hoodieRecords, hoodieConfig, operation, jsc, parameters)
            try {
              val writeResult = DataSourceUtils.doWriteOperation(client, dedupedHoodieRecords, instantTime, operation,
                preppedSparkSqlWrites || preppedWriteOperation)
              (writeResult, client)
            } catch {
              case e: HoodieException =>
                // close the write client in all cases
                closeWriteClient(client, tableConfig, parameters, jsc.hadoopConfiguration())
                throw e
            }
        }

      // Check for errors and commit the write.
      try {
        val (writeSuccessful, compactionInstant, clusteringInstant) =
          commitAndPerformPostOperations(sqlContext.sparkSession, df.schema,
            writeResult, parameters, writeClient, tableConfig, jsc,
            TableInstantInfo(basePath, instantTime, commitActionType, operation), streamingWritesParamsOpt.map(_.extraPreCommitFn)
              .orElse(Option.apply(Option.empty)).get)

        (writeSuccessful, common.util.Option.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
      } finally {
        closeWriteClient(writeClient, tableConfig, parameters, jsc.hadoopConfiguration())
      }
    }
  }

  private def closeWriteClient(writeClient: SparkRDDWriteClient[_], tableConfig: HoodieTableConfig, parameters: Map[String, String], configuration: Configuration): Unit = {
    // close the write client in all cases
    val asyncCompactionEnabled = isAsyncCompactionEnabled(writeClient, tableConfig, parameters, configuration)
    val asyncClusteringEnabled = isAsyncClusteringEnabled(writeClient, parameters)
    if (!asyncCompactionEnabled && !asyncClusteringEnabled) {
      log.info("Closing write client")
      writeClient.close()
    }
  }

  def deduceOperation(hoodieConfig: HoodieConfig, paramsWithoutDefaults: Map[String, String], df: Dataset[Row]): WriteOperationType = {
    var operation = WriteOperationType.fromValue(hoodieConfig.getString(OPERATION))
    // TODO clean up
    // It does not make sense to allow upsert() operation if INSERT_DROP_DUPS is true
    // or INSERT_DUP_POLICY is `drop` or `fail`.
    // Auto-correct the operation to "insert" if OPERATION is set to "upsert" wrongly
    // or not set (in which case it will be set as "upsert" by parametersWithWriteDefaults()) .
    if ((hoodieConfig.getBoolean(INSERT_DROP_DUPS) ||
      shouldDropDuplicatesForInserts(hoodieConfig) ||
      shouldFailWhenDuplicatesFound(hoodieConfig))
      && operation == WriteOperationType.UPSERT) {

      log.warn(s"$UPSERT_OPERATION_OPT_VAL is not applicable " +
        s"when $INSERT_DROP_DUPS is set to be true, or $INSERT_DUP_POLICY is set to fail or drop, " +
        s"overriding the $OPERATION to be $INSERT_OPERATION_OPT_VAL")

      operation = WriteOperationType.INSERT
      operation
    } else {
      // if no record key, and no meta fields, we should treat it as append only workload and make bulk_insert as operation type.
      if (!hoodieConfig.contains(DataSourceWriteOptions.RECORDKEY_FIELD.key())
        && !paramsWithoutDefaults.contains(OPERATION.key()) && !df.schema.fieldNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
        log.warn(s"Choosing BULK_INSERT as the operation type since auto record key generation is applicable")
        operation = WriteOperationType.BULK_INSERT
      }
      operation
    }
  }

  /**
   * Resolve wildcards in partitions
   *
   * @param partitions list of partitions that may contain wildcards
   * @param jsc        instance of java spark context
   * @param storage    [[HoodieStorage]] instance
   * @param cfg        hoodie config
   * @param basePath   base path
   * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
   */
  private def resolvePartitionWildcards(partitions: List[String], jsc: JavaSparkContext,
                                        storage: HoodieStorage, cfg: HoodieConfig, basePath: String, metaClient: HoodieTableMetaClient): List[String] = {
    //find out if any of the input partitions have wildcards
    //note:spark-sql may url-encode special characters (* -> %2A)
    var (wildcardPartitions, fullPartitions) = partitions.partition(partition => partition.matches(".*(\\*|%2A).*"))

    val allPartitions = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc): HoodieEngineContext,
      metaClient, HoodieMetadataConfig.newBuilder().fromProperties(cfg.getProps).build())

    if (fullPartitions.nonEmpty) {
      fullPartitions = fullPartitions.filter(partition => allPartitions.contains(partition))
    }

    if (wildcardPartitions.nonEmpty) {
      //go through list of partitions with wildcards and add all partitions that match to val fullPartitions
      wildcardPartitions.foreach(partition => {
        //turn wildcard into regex
        //regex for start of line is ^ and end of line is $
        //If the partitionpath was just alphanumeric we would just replace * with .*
        //Since there could be characters that could be considered regex in the partitionpath we must
        //prevent that from happening. Any text inbetween \\Q and \\E is considered literal
        //So we start the string with \\Q and end with \\E and then whenever we find a * we add \\E before
        //and \\Q after so all other characters besides .* will be enclosed between a set of \\Q \\E
        val wildcardToken: String = if (partition.contains("*")) "*" else "%2A"
        val regexPartition = "^\\Q" + partition.replace(wildcardToken, "\\E.*\\Q") + "\\E$"

        //filter all partitions with the regex and append the result to the list of full partitions
        fullPartitions = List.concat(fullPartitions, allPartitions.asScala.filter(_.matches(regexPartition)))
      })
    }
    fullPartitions.distinct
  }

  def getPartitionColumns(partitionParam: String): Seq[String] = {
    partitionParam.split(",")
      .map(partitionField => partitionField.trim)
      .filter(_.nonEmpty)
      .toSeq
  }

  def generateSchemaWithoutPartitionColumns(partitionParam: String, schema: Schema): Schema = {
    val partitionColumns = getPartitionColumns(partitionParam)
    HoodieAvroUtils.removeFields(schema, partitionColumns.toSet.asJava)
  }

  def addSchemaEvolutionParameters(parameters: Map[String, String], internalSchemaOpt: Option[InternalSchema], writeSchemaOpt: Option[Schema] = None): Map[String, String] = {
    val schemaEvolutionEnable = if (internalSchemaOpt.isDefined) "true" else "false"

    val schemaValidateEnable = if (schemaEvolutionEnable.toBoolean && parameters.getOrElse(DataSourceWriteOptions.RECONCILE_SCHEMA.key(), "false").toBoolean) {
      // force disable schema validate, now we support schema evolution, no need to do validate
      "false"
    } else {
      parameters.getOrElse(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key(), HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue())
    }
    // correct internalSchema, internalSchema should contain hoodie metadata columns.
    val correctInternalSchema = internalSchemaOpt.map { internalSchema =>
      if (internalSchema.findField(HoodieRecord.RECORD_KEY_METADATA_FIELD) == null && writeSchemaOpt.isDefined) {
        val allowOperationMetaDataField = parameters.getOrElse(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key(), "false").toBoolean
        AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(writeSchemaOpt.get, allowOperationMetaDataField))
      } else {
        internalSchema
      }
    }
    parameters ++ Map(HoodieWriteConfig.INTERNAL_SCHEMA_STRING.key() -> SerDeHelper.toJson(correctInternalSchema.getOrElse(null)),
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key() -> schemaEvolutionEnable,
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key() -> schemaValidateEnable)
  }

  private def registerAvroSchemasWithKryo(sparkContext: SparkContext, targetAvroSchemas: Schema*): Unit = {
    sparkContext.getConf.registerAvroSchemas(targetAvroSchemas: _*)
  }

  private def getLatestTableSchema(spark: SparkSession,
                                   tableId: TableIdentifier,
                                   tableMetaClient: HoodieTableMetaClient): Option[Schema] = {
    val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
    val latestTableSchemaFromCommitMetadata =
      toScalaOption(tableSchemaResolver.getTableAvroSchemaFromLatestCommit(false))
    latestTableSchemaFromCommitMetadata.orElse {
      getCatalogTable(spark, tableId).map { catalogTable =>
        val (structName, namespace) = getAvroRecordNameAndNamespace(tableId.table)
        convertStructTypeToAvroSchema(catalogTable.schema, structName, namespace)
      }
    }
  }

  private def getCatalogTable(spark: SparkSession, tableId: TableIdentifier): Option[CatalogTable] = {
    if (spark.sessionState.catalog.tableExists(tableId)) {
      Some(spark.sessionState.catalog.getTableMetadata(tableId))
    } else {
      None
    }
  }

  def bootstrap(sqlContext: SQLContext,
                mode: SaveMode,
                optParams: Map[String, String],
                df: DataFrame,
                hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
                streamingWritesParamsOpt: Option[StreamingWriteParams] = Option.empty,
                hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty): Boolean = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    // fetch table config for an already existing table and SaveMode is not Overwrite.
    val tableConfig = getHoodieTableConfig(sparkContext, path, mode, hoodieTableConfigOpt)
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite)

    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode, streamingWritesParamsOpt.isDefined)
    val tableName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TBL_NAME, s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.")
    val tableType = hoodieConfig.getStringOrDefault(TABLE_TYPE)
    val bootstrapBasePath = hoodieConfig.getStringOrThrow(BASE_PATH,
      s"'${BASE_PATH.key}' is required for '${BOOTSTRAP_OPERATION_OPT_VAL}'" +
        " operation'")
    assert(!path.equals(bootstrapBasePath), "Bootstrap base path and Hudi table base path must be different")
    val bootstrapIndexClass = hoodieConfig.getStringOrDefault(INDEX_CLASS_NAME)

    var schema: String = null
    if (df.schema.nonEmpty) {
      val (structName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
      schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, namespace).toString
    } else {
      schema = HoodieAvroUtils.getNullSchema.toString
    }

    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      if (!hoodieWriteClient.isEmpty) {
        hoodieWriteClient.get.close()
      }
      false
    } else {
      // Handle various save modes
      handleSaveModes(sqlContext.sparkSession, mode, basePath, tableConfig, tableName, WriteOperationType.BOOTSTRAP, fs)

      if (!tableExists) {
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_HISTORY_PATH)
        val tableVersion = Integer.valueOf(getStringWithAltKeys(parameters, HoodieWriteConfig.WRITE_TABLE_VERSION))
        val partitionColumnsWithType = SparkKeyGenUtils.getPartitionColumnsForKeyGenerator(toProperties(parameters), HoodieTableVersion.fromVersionCode(tableVersion))
        val recordKeyFields = hoodieConfig.getString(DataSourceWriteOptions.RECORDKEY_FIELD)
        val payloadClass = hoodieConfig.getString(DataSourceWriteOptions.PAYLOAD_CLASS_NAME)
        val recordMergerStrategy = hoodieConfig.getString(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID)
        val keyGenProp =
          if (StringUtils.nonEmpty(hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)))
            hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)
          else KeyGeneratorType.getKeyGeneratorClassName(hoodieConfig)
        val timestampKeyGeneratorConfigs = extractConfigsRelatedToTimestampBasedKeyGenerator(keyGenProp, parameters)
        val populateMetaFields = java.lang.Boolean.parseBoolean(parameters.getOrElse(
          HoodieTableConfig.POPULATE_META_FIELDS.key(),
          String.valueOf(HoodieTableConfig.POPULATE_META_FIELDS.defaultValue())
        ))
        val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.BASE_FILE_FORMAT)
        val useBaseFormatMetaFile = java.lang.Boolean.parseBoolean(parameters.getOrElse(
          HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key(),
          String.valueOf(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.defaultValue())
        ))
        val tableFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.TABLE_FORMAT)

        HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.valueOf(tableType))
          .setTableName(tableName)
          .setRecordKeyFields(recordKeyFields)
          .setTableVersion(tableVersion)
          .setTableFormat(tableFormat)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(payloadClass)
          .setRecordMergeMode(RecordMergeMode.getValue(hoodieConfig.getString(HoodieWriteConfig.RECORD_MERGE_MODE)))
          .setRecordMergeStrategyId(recordMergerStrategy)
          .setPreCombineFields(hoodieConfig.getStringOrDefault(PRECOMBINE_FIELD, null))
          .setBootstrapIndexClass(bootstrapIndexClass)
          .setBaseFileFormat(baseFileFormat)
          .setBootstrapBasePath(bootstrapBasePath)
          .setPartitionFields(partitionColumnsWithType)
          .setCDCEnabled(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED))
          .setCDCSupplementalLoggingMode(hoodieConfig.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE))
          .setPopulateMetaFields(populateMetaFields)
          .setKeyGeneratorClassProp(keyGenProp)
          .set(timestampKeyGeneratorConfigs.asJava.asInstanceOf[java.util.Map[String, Object]])
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setCommitTimezone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE)))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .initTable(HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration), path)
      }

      val jsc = new JavaSparkContext(sqlContext.sparkContext)
      val writeClient = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
        schema, path, tableName, parameters.asJava))
      try {
        writeClient.bootstrap(org.apache.hudi.common.util.Option.empty())
      } finally {
        writeClient.close()
      }
      val metaSyncSuccess = metaSync(sqlContext.sparkSession, hoodieConfig, basePath, df.schema)
      metaSyncSuccess
    }
  }

  def validateSchemaForHoodieIsDeleted(schema: Schema): Unit = {
    if (schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) != null &&
      resolveNullableSchema(schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).schema()).getType != Schema.Type.BOOLEAN) {
      throw new HoodieException(HoodieRecord.HOODIE_IS_DELETED_FIELD + " has to be BOOLEAN type. Passed in dataframe's schema has type "
        + schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).schema().getType)
    }
  }

  def bulkInsertAsRow(writeClient: SparkRDDWriteClient[_],
                      parameters: Map[String, String],
                      hoodieConfig: HoodieConfig,
                      df: DataFrame,
                      mode: SaveMode,
                      tblName: String,
                      basePath: Path,
                      writerSchema: Schema,
                      tableConfig: HoodieTableConfig):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {
    if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet")
    }
    val sqlContext = writeClient.getEngineContext.asInstanceOf[HoodieSparkEngineContext].getSqlContext
    val jsc = writeClient.getEngineContext.asInstanceOf[HoodieSparkEngineContext].getJavaSparkContext

    val writeConfig = HoodieSparkSqlWriter.getBulkInsertRowConfig(org.apache.hudi.common.util.Option.of(writerSchema), hoodieConfig, basePath.toString, tblName)
    val overwriteOperationType = Option(hoodieConfig.getString(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE))
      .map(WriteOperationType.fromValue)
      .orNull
    val executor = mode match {
      case _ if overwriteOperationType == null =>
        // Don't need to overwrite
        new DatasetBulkInsertCommitActionExecutor(writeConfig, writeClient)
      case SaveMode.Append if overwriteOperationType == WriteOperationType.INSERT_OVERWRITE =>
        // INSERT OVERWRITE PARTITION uses Append mode
        new DatasetBulkInsertOverwriteCommitActionExecutor(writeConfig, writeClient)
      case SaveMode.Append if overwriteOperationType == WriteOperationType.BUCKET_RESCALE =>
        new DatasetBucketRescaleCommitActionExecutor(writeConfig, writeClient)
      case SaveMode.Overwrite if overwriteOperationType == WriteOperationType.INSERT_OVERWRITE_TABLE =>
        new DatasetBulkInsertOverwriteTableCommitActionExecutor(writeConfig, writeClient)
      case _ =>
        throw new HoodieException(s"$mode with bulk_insert in row writer path is not supported yet");
    }

    val writeResult = executor.execute(df, tableConfig.isTablePartitioned)
    val instantTime = executor.getInstantTime

    try {
      val (writeSuccessful, compactionInstant, clusteringInstant) = commitAndPerformPostOperations(
        sqlContext.sparkSession, df.schema, writeResult, parameters, writeClient, tableConfig, jsc,
        TableInstantInfo(basePath, instantTime, executor.getCommitActionType, executor.getWriteOperationType), Option.empty)
      (writeSuccessful, HOption.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
    } finally {
      closeWriteClient(writeClient, tableConfig, parameters, jsc.hadoopConfiguration())
    }
  }

  private def handleSaveModes(spark: SparkSession, mode: SaveMode, tablePath: Path, tableConfig: HoodieTableConfig, tableName: String,
                              operation: WriteOperationType, fs: FileSystem): Unit = {
    if (mode == SaveMode.Append && tableExists) {
      val existingTableName = tableConfig.getTableName
      val resolver = spark.sessionState.conf.resolver
      if (!resolver(existingTableName, tableName)) {
        throw new HoodieException(s"hoodie table with name $existingTableName already exists at $tablePath," +
          s" can not append data to the table with another name $tableName.")
      }
    }

    if (!WriteOperationType.isDelete(operation)) {
      if (mode == SaveMode.ErrorIfExists && tableExists) {
        throw new HoodieException(s"hoodie table at $tablePath already exists.")
      } else if (mode == SaveMode.Overwrite && tableExists && operation != WriteOperationType.INSERT_OVERWRITE_TABLE) {
        // When user set operation as INSERT_OVERWRITE_TABLE,
        // overwrite will use INSERT_OVERWRITE_TABLE operator in doWriteOperation
        // TODO HUDI-6286 should not delete old data if using `Overwrite` mode
        log.warn(s"hoodie table at $tablePath already exists. Deleting existing data & overwriting with new data.")
        fs.delete(tablePath, true)
        tableExists = false
      }
    } else {
      // Delete Operation only supports Append mode
      if (mode != SaveMode.Append) {
        throw new HoodieException(s"Append is the only save mode applicable for ${operation.toString} operation")
      }
    }
  }

  private def metaSync(spark: SparkSession, hoodieConfig: HoodieConfig, basePath: Path,
                       schema: StructType): Boolean = {
    val hiveSyncEnabled = hoodieConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_SYNC_ENABLED).toBoolean
    var metaSyncEnabled = hoodieConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_ENABLED).toBoolean
    val syncClientToolClassSet = scala.collection.mutable.Set[String]()
    hoodieConfig.getString(META_SYNC_CLIENT_TOOL_CLASS_NAME).split(",").foreach(syncClass => syncClientToolClassSet += syncClass)

    // for backward compatibility
    if (hiveSyncEnabled) metaSyncEnabled = true

    // for AWS glue compatibility
    if (hoodieConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE) == HiveSyncMode.GLUE.name()) {
      syncClientToolClassSet += "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
    }

    if (metaSyncEnabled) {
      val fs = basePath.getFileSystem(spark.sessionState.newHadoopConf())
      val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT);
      val properties = TypedProperties.fromMap(hoodieConfig.getProps)
      properties.put(HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key, spark.sessionState.conf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD).toString)
      properties.put(HoodieSyncConfig.META_SYNC_SPARK_VERSION.key, SPARK_VERSION)
      properties.put(HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA.key, hoodieConfig.getBoolean(HoodieMetadataConfig.ENABLE))
      if ((fs.getConf.get(HiveConf.ConfVars.METASTOREPWD.varname) == null || fs.getConf.get(HiveConf.ConfVars.METASTOREPWD.varname).isEmpty) &&
        (properties.get(HiveSyncConfigHolder.HIVE_PASS.key()) == null || properties.get(HiveSyncConfigHolder.HIVE_PASS.key()).toString.isEmpty || properties.get(HiveSyncConfigHolder.HIVE_PASS.key()).toString.equalsIgnoreCase(HiveSyncConfigHolder.HIVE_PASS.defaultValue()))) {
        try {
          val passwd = ShimLoader.getHadoopShims.getPassword(spark.sparkContext.hadoopConfiguration, HiveConf.ConfVars.METASTOREPWD.varname)
          if (passwd != null && !passwd.isEmpty) {
            fs.getConf.set(HiveConf.ConfVars.METASTOREPWD.varname, passwd)
            properties.put(HiveSyncConfigHolder.HIVE_PASS.key(), passwd)
          }
        } catch {
          case e: Exception =>
            log.info("Exception while trying to get Meta Sync password from hadoop credential store", e)
        }
      }
      // Collect exceptions in list because we want all sync to run. Then we can throw
      val failedMetaSyncs = new mutable.HashMap[String, HoodieException]()
      syncClientToolClassSet.foreach(impl => {
        try {
          SyncUtilHelpers.runHoodieMetaSync(impl.trim, properties, fs.getConf, fs, basePath.toString, baseFileFormat)
        } catch {
          case e: HoodieException =>
            log.info("SyncTool class " + impl.trim + " failed with exception", e)
            failedMetaSyncs.put(impl, e)
        }
      })
      if (failedMetaSyncs.nonEmpty) {
        throw getHoodieMetaSyncException(failedMetaSyncs.asJava)
      }
    }

    // Since Hive tables are now synced as Spark data source tables which are cached after Spark SQL queries
    // we must invalidate this table in the cache so writes are reflected in later queries
    if (metaSyncEnabled) {
      getHiveTableNames(hoodieConfig).foreach(name => {
        val syncDb = hoodieConfig.getStringOrDefault(HIVE_DATABASE)
        val qualifiedTableName = String.join(".", syncDb, name)
        if (spark.catalog.databaseExists(syncDb) && spark.catalog.tableExists(qualifiedTableName)) {
          spark.catalog.refreshTable(qualifiedTableName)
        }
      })
    }
    true
  }

  private def getHiveTableNames(hoodieConfig: HoodieConfig): List[String] = {
    val tableName = hoodieConfig.getStringOrDefault(HIVE_TABLE)
    val tableType = hoodieConfig.getStringOrDefault(TABLE_TYPE)

    if (tableType.equals(COW_TABLE_TYPE_OPT_VAL)) {
      List(tableName)
    } else {
      val roSuffix = if (hoodieConfig.getBooleanOrDefault(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE)) {
        ""
      } else {
        HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE
      }
      List(tableName + roSuffix,
        tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE)
    }
  }

  /**
   * Group all table/action specific information into a case class.
   */
  case class TableInstantInfo(basePath: Path, instantTime: String, commitActionType: String, operation: WriteOperationType)

  private def commitAndPerformPostOperations(spark: SparkSession,
                                             schema: StructType,
                                             writeResult: HoodieWriteResult,
                                             parameters: Map[String, String],
                                             client: SparkRDDWriteClient[_],
                                             tableConfig: HoodieTableConfig,
                                             jsc: JavaSparkContext,
                                             tableInstantInfo: TableInstantInfo,
                                             extraPreCommitFn: Option[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]]
                                            ): (Boolean, HOption[java.lang.String], HOption[java.lang.String]) = {
    val hasErrors = new AtomicBoolean(false)
    log.info("Proceeding to commit the write.")
    // get extra metadata from props
    // 1. properties starting with commit metadata key prefix
    // 2. properties related to checkpoint in spark streaming
    val extraMetadataOpt = common.util.Option.of(DataSourceUtils.getExtraMetadata(parameters.asJava))
    val commitSuccess =
      client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
        extraMetadataOpt,
        tableInstantInfo.commitActionType,
        writeResult.getPartitionToReplaceFileIds,
        common.util.Option.ofNullable(extraPreCommitFn.orNull),
        org.apache.hudi.common.util.Option.of(new SparkDataSourceWriteStatusValidator(tableInstantInfo.operation, hasErrors)))

    if (!hasErrors.get()) {
      if (commitSuccess) {
        log.info("Commit " + tableInstantInfo.instantTime + " successful!")
      }
      else {
        log.info("Commit " + tableInstantInfo.instantTime + " failed!")
      }

      val asyncCompactionEnabled = isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())
      val compactionInstant: common.util.Option[java.lang.String] =
        if (asyncCompactionEnabled) {
          client.scheduleCompaction(extraMetadataOpt)
        } else {
          common.util.Option.empty()
        }

      log.info(s"Compaction Scheduled is $compactionInstant")

      val asyncClusteringEnabled = isAsyncClusteringEnabled(client, parameters)
      val clusteringInstant: common.util.Option[java.lang.String] =
        if (asyncClusteringEnabled) {
          client.scheduleClustering(extraMetadataOpt)
        } else {
          common.util.Option.empty()
        }

      log.info(s"Clustering Scheduled is $clusteringInstant")

      val metaSyncSuccess = metaSync(spark, HoodieWriterUtils.convertMapToHoodieConfig(parameters),
        tableInstantInfo.basePath, schema)

      log.info(s"Is Async Compaction Enabled ? $asyncCompactionEnabled")
      (commitSuccess && metaSyncSuccess, compactionInstant, clusteringInstant)
    } else {
      (false, common.util.Option.empty(), common.util.Option.empty())
    }
  }

  private def isAsyncCompactionEnabled(client: SparkRDDWriteClient[_],
                                       tableConfig: HoodieTableConfig,
                                       parameters: Map[String, String], configuration: Configuration): Boolean = {
    log.info(s"Config.inlineCompactionEnabled ? ${client.getConfig.inlineCompactionEnabled}")
    (asyncCompactionTriggerFnDefined && !client.getConfig.inlineCompactionEnabled
      && parameters.get(ASYNC_COMPACT_ENABLE.key).exists(r => r.toBoolean)
      && tableConfig.getTableType == MERGE_ON_READ)
  }

  private def isAsyncClusteringEnabled(client: SparkRDDWriteClient[_],
                                       parameters: Map[String, String]): Boolean = {
    log.info(s"Config.asyncClusteringEnabled ? ${client.getConfig.isAsyncClusteringEnabled}")
    (asyncClusteringTriggerFnDefined && !client.getConfig.inlineClusteringEnabled
      && client.getConfig.isAsyncClusteringEnabled)
  }

  /**
   * Fetch table config for an already existing table and if save mode is not Overwrite.
   *
   * @param sparkContext         instance of {@link SparkContext} to use.
   * @param tablePath            table base path.
   * @param mode                 save mode in use.
   * @param hoodieTableConfigOpt return table config from this Option if present. else poll from a new metaClient.
   * @return {@link HoodieTableConfig} is conditions match. if not, returns null.
   */
  private def getHoodieTableConfig(sparkContext: SparkContext,
                                   tablePath: String,
                                   mode: SaveMode,
                                   hoodieTableConfigOpt: Option[HoodieTableConfig]): HoodieTableConfig = {
    if (tableExists && mode != SaveMode.Overwrite) {
      hoodieTableConfigOpt.getOrElse(
        HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration))
          .setBasePath(tablePath)
          .build().getTableConfig)
    } else {
      null
    }
  }

  private def mergeParamsAndGetHoodieConfig(optParams: Map[String, String],
                                            tableConfig: HoodieTableConfig, mode: SaveMode,
                                            isStreamingWrite: Boolean): (Map[String, String], HoodieConfig) = {
    val translatedOptions = DataSourceWriteOptions.mayBeDerivePartitionPath(optParams)
    var translatedOptsWithMappedTableConfig = mutable.Map.empty ++ translatedOptions.toMap
    if (tableConfig != null && mode != SaveMode.Overwrite) {
      // for missing write configs corresponding to table configs, fill them up.
      fetchMissingWriteConfigsFromTableConfig(tableConfig, optParams).foreach((kv) => translatedOptsWithMappedTableConfig += (kv._1 -> kv._2))
    }
    if (null != tableConfig && mode != SaveMode.Overwrite) {
      // over-ride only if not explicitly set by the user.
      tableConfig.getProps.asScala.filter(kv => !optParams.contains(kv._1))
        .foreach { case (key, value) =>
          translatedOptsWithMappedTableConfig += (key -> value)
        }
    }
    val mergedParams = mutable.Map.empty ++ HoodieWriterUtils.parametersWithWriteDefaults(translatedOptsWithMappedTableConfig.toMap)
    if (mergedParams.contains(KEYGENERATOR_CLASS_NAME.key) && !mergedParams.contains(HoodieTableConfig.KEY_GENERATOR_TYPE.key)) {
      mergedParams(HoodieTableConfig.KEY_GENERATOR_TYPE.key) = KeyGeneratorType.fromClassName(mergedParams(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key)).name
    }
    // use preCombineField to fill in PAYLOAD_ORDERING_FIELD_PROP_KEY
    if (mergedParams.contains(PRECOMBINE_FIELD.key())) {
      mergedParams.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, mergedParams(PRECOMBINE_FIELD.key()))
    }
    if (mergedParams.get(OPERATION.key()).get == INSERT_OPERATION_OPT_VAL && mergedParams.contains(DataSourceWriteOptions.INSERT_DUP_POLICY.key())
      && mergedParams.get(DataSourceWriteOptions.INSERT_DUP_POLICY.key()).get != FAIL_INSERT_DUP_POLICY) {
      // enable merge allow duplicates when operation type is insert
      mergedParams.put(HoodieWriteConfig.MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key(), "true")
    }
    // enable inline compaction for batch writes if applicable
    if (!isStreamingWrite
      && mergedParams.getOrElse(DataSourceWriteOptions.TABLE_TYPE.key(), COPY_ON_WRITE.name()) == MERGE_ON_READ.name()
      && !optParams.contains(HoodieCompactionConfig.INLINE_COMPACT.key())
      && !optParams.contains(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key)) {
      mergedParams.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
    }
    // disable drop partition columns when upsert MOR table
    if (mergedParams.get(OPERATION.key).get == UPSERT_OPERATION_OPT_VAL
      && mergedParams.getOrElse(DataSourceWriteOptions.TABLE_TYPE.key, COPY_ON_WRITE.name) == MERGE_ON_READ.name) {
      mergedParams.put(HoodieTableConfig.DROP_PARTITION_COLUMNS.key, "false")
    }

    val tableVersion = if (tableConfig != null) {
      tableConfig.getTableVersion
    } else {
      HoodieTableVersion.fromVersionCode(
        SparkConfigUtils.getStringWithAltKeys(mergedParams, WRITE_TABLE_VERSION).toInt)
    }
    if (!mergedParams.contains(DataSourceWriteOptions.RECORD_MERGE_MODE.key())
      || !mergedParams.contains(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key())
      || !mergedParams.contains(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key())) {
      val inferredMergeConfigs = HoodieTableConfig.inferCorrectMergingBehavior(
        RecordMergeMode.getValue(mergedParams.getOrElse(DataSourceWriteOptions.RECORD_MERGE_MODE.key(), null)),
        mergedParams.getOrElse(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key(), ""),
        mergedParams.getOrElse(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key(), ""),
        optParams.getOrElse(PRECOMBINE_FIELD.key(), null),
        tableVersion)
      mergedParams.put(DataSourceWriteOptions.RECORD_MERGE_MODE.key(), inferredMergeConfigs.getLeft.name())
      mergedParams.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), inferredMergeConfigs.getLeft.name())
      mergedParams.put(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key(), inferredMergeConfigs.getMiddle)
      mergedParams.put(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), inferredMergeConfigs.getMiddle)
      mergedParams.put(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key(), inferredMergeConfigs.getRight)
      mergedParams.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), inferredMergeConfigs.getRight)
    } else {
      mergedParams.put(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), mergedParams(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key()))
      mergedParams.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), mergedParams(HoodieWriteConfig.RECORD_MERGE_MODE.key()))
      mergedParams.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), mergedParams(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key()))
    }

    val params = mergedParams.toMap
    (params, HoodieWriterUtils.convertMapToHoodieConfig(params))
  }

  private def extractConfigsRelatedToTimestampBasedKeyGenerator(keyGenerator: String,
                                                                params: Map[String, String]): Map[String, String] = {
    if (classOf[TimestampBasedKeyGenerator].getCanonicalName.equals(keyGenerator) ||
      classOf[TimestampBasedAvroKeyGenerator].getCanonicalName.equals(keyGenerator)) {
      val allKeys = getAllConfigKeys(HoodieTableConfig.PERSISTED_CONFIG_LIST)
      params.filterKeys(allKeys.contains).toMap
    } else {
      Map.empty
    }
  }

  private def getExecutionId(context: SparkContext, newQueryExecution: QueryExecution): Option[Long] = {
    // If the `QueryExecution` does not match the current execution ID, it means the execution ID
    // belongs to another plan job, so we couldn't update UI in this plan job.
    Option(context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong).filter(SQLExecution.getQueryExecution(_) eq newQueryExecution)
  }
}

object HoodieSparkSqlWriterInternal {
  // Check if duplicates should be dropped.
  def shouldDropDuplicatesForInserts(hoodieConfig: HoodieConfig): Boolean = {
    hoodieConfig.contains(INSERT_DUP_POLICY) &&
      hoodieConfig.getString(INSERT_DUP_POLICY).equalsIgnoreCase(DROP_INSERT_DUP_POLICY)
  }

  // Check if we should fail if duplicates are found.
  def shouldFailWhenDuplicatesFound(hoodieConfig: HoodieConfig): Boolean = {
    hoodieConfig.contains(INSERT_DUP_POLICY) &&
      hoodieConfig.getString(INSERT_DUP_POLICY).equalsIgnoreCase(FAIL_INSERT_DUP_POLICY)
  }

  // Check if deduplication is required.
  def isDeduplicationRequired(hoodieConfig: HoodieConfig): Boolean = {
    hoodieConfig.getBoolean(INSERT_DROP_DUPS) ||
      shouldFailWhenDuplicatesFound(hoodieConfig) ||
      shouldDropDuplicatesForInserts(hoodieConfig)
  }

  // Check if deduplication is needed.
  def isDeduplicationNeeded(operation: WriteOperationType): Boolean = {
    operation == WriteOperationType.INSERT
  }

  def handleInsertDuplicates(incomingRecords: JavaRDD[HoodieRecord[_]],
                             hoodieConfig: HoodieConfig,
                             operation: WriteOperationType,
                             jsc: JavaSparkContext,
                             parameters: Map[String, String]): JavaRDD[HoodieRecord[_]] = {
    // If no deduplication is needed, return the incoming records as is
    if (!isDeduplicationRequired(hoodieConfig) || !isDeduplicationNeeded(operation)) {
      incomingRecords
    } else {
      // Perform deduplication
      DataSourceUtils.resolveDuplicates(
        jsc, incomingRecords, parameters.asJava, shouldFailWhenDuplicatesFound(hoodieConfig))
    }
  }
}
