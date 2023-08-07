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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.AutoRecordKeyGenerationUtils.{isAutoGenerateRecordKeys, mayBeValidateParamsForAutoGenerationOfRecordKeys}
import org.apache.hudi.AvroConversionUtils.{convertAvroSchemaToStructType, convertStructTypeToAvroSchema, getAvroRecordNameAndNamespace}
import org.apache.hudi.DataSourceOptionsHelper.fetchMissingWriteConfigsFromTableConfig
import org.apache.hudi.DataSourceUtils.tryOverrideParquetWriteLegacyFormatProperty
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.{toProperties, toScalaOption}
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.avro.AvroSchemaUtils.{canProject, isCompatibleProjectionOf, isSchemaCompatible, resolveNullableSchema}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.HoodieAvroUtils.removeMetadataFields
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.commit.{DatasetBulkInsertCommitActionExecutor, DatasetBulkInsertOverwriteCommitActionExecutor, DatasetBulkInsertOverwriteTableCommitActionExecutor}
import org.apache.hudi.common.config._
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstantTimeGenerator}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{CommitUtils, StringUtils, Option => HOption}
import org.apache.hudi.config.HoodieBootstrapConfig.{BASE_PATH, INDEX_CLASS_NAME}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY
import org.apache.hudi.exception.{HoodieException, SchemaCompatibilityException}
import org.apache.hudi.hive.{HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils.reconcileNullability
import org.apache.hudi.internal.schema.utils.{AvroSchemaEvolutionUtils, SerDeHelper}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName
import org.apache.hudi.keygen.{BaseKeyGenerator, KeyGenUtils, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.metrics.Metrics
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.sync.common.util.SyncUtilHelpers
import org.apache.hudi.sync.common.util.SyncUtilHelpers.getHoodieMetaSyncException
import org.apache.hudi.util.SparkKeyGenUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.slf4j.LoggerFactory

import java.util.function.BiConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.collection.mutable

object HoodieSparkSqlWriter {

  /**
   * Controls whether incoming batch's schema's nullability constraints should be canonicalized
   * relative to the table's schema. For ex, in case field A is marked as null-able in table's schema, but is marked
   * as non-null in the incoming batch, w/o canonicalization such write might fail as we won't be able to read existing
   * null records from the table (for updating, for ex). Note, that this config has only effect when
   * 'hoodie.datasource.write.reconcile.schema' is set to false
   *
   * NOTE: This is an internal config that is not exposed to the public
   */
  val CANONICALIZE_NULLABLE: ConfigProperty[Boolean] =
    ConfigProperty.key("hoodie.internal.write.schema.canonicalize.nullable")
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

  private val log = LoggerFactory.getLogger(getClass)
  private var tableExists: Boolean = false
  private var asyncCompactionTriggerFnDefined: Boolean = false
  private var asyncClusteringTriggerFnDefined: Boolean = false

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            optParams: Map[String, String],
            sourceDf: DataFrame,
            hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
            hoodieWriteClient: Option[SparkRDDWriteClient[_]] = Option.empty,
            asyncCompactionTriggerFn: Option[SparkRDDWriteClient[_] => Unit] = Option.empty,
            asyncClusteringTriggerFn: Option[SparkRDDWriteClient[_] => Unit] = Option.empty,
            extraPreCommitFn: Option[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]] = Option.empty):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)

    val spark = sqlContext.sparkSession
    val sparkContext = sqlContext.sparkContext

    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    var tableConfig = getHoodieTableConfig(sparkContext, path, mode, hoodieTableConfigOpt)
    // get params w/o injecting default and validate
    val paramsWithoutDefaults = HoodieWriterUtils.getParamsWithAlternatives(optParams)
    val originKeyGeneratorClassName = HoodieWriterUtils.getOriginKeyGenerator(paramsWithoutDefaults)
    val timestampKeyGeneratorConfigs = extractConfigsRelatedToTimestampBasedKeyGenerator(
      originKeyGeneratorClassName, paramsWithoutDefaults)

    // Validate datasource and tableconfig keygen are the same
    validateKeyGeneratorConfig(originKeyGeneratorClassName, tableConfig);
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite);

    // re-use table configs and inject defaults.
    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode)
    val databaseName = hoodieConfig.getStringOrDefault(HoodieTableConfig.DATABASE_NAME, "")
    val tblName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TBL_NAME,
      s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.").trim
    val tableIdentifier = TableIdentifier(tblName, if (databaseName.isEmpty) None else Some(databaseName))

    assert(!StringUtils.isNullOrEmpty(hoodieConfig.getString(HoodieWriteConfig.TBL_NAME)),
      s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.")

    asyncCompactionTriggerFnDefined = asyncCompactionTriggerFn.isDefined
    asyncClusteringTriggerFnDefined = asyncClusteringTriggerFn.isDefined
    sparkContext.getConf.getOption("spark.serializer") match {
      case Some(ser) if ser.equals("org.apache.spark.serializer.KryoSerializer") =>
      case _ => throw new HoodieException("hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer")
    }
    val tableType = HoodieTableType.valueOf(hoodieConfig.getString(TABLE_TYPE))
    val operation = deduceOperation(hoodieConfig, paramsWithoutDefaults, sourceDf)

    val preppedSparkSqlMergeInto = parameters.getOrDefault(SPARK_SQL_MERGE_INTO_PREPPED_KEY, "false").toBoolean
    val preppedSparkSqlWrites = parameters.getOrDefault(SPARK_SQL_WRITES_PREPPED_KEY, "false").toBoolean
    val preppedWriteOperation = canDoPreppedWrites(hoodieConfig, parameters, operation, sourceDf)

    val jsc = new JavaSparkContext(sparkContext)
    if (asyncCompactionTriggerFn.isDefined) {
      if (jsc.getConf.getOption(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY).isDefined) {
        jsc.setLocalProperty("spark.scheduler.pool", SparkConfigs.SPARK_DATASOURCE_WRITER_POOL_NAME)
      }
    }

    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      (false, common.util.Option.empty(), common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
    } else {
      // Handle various save modes
      handleSaveModes(sqlContext.sparkSession, mode, basePath, tableConfig, tblName, operation, fs)
      val partitionColumns = SparkKeyGenUtils.getPartitionColumns(getKeyGeneratorClassName(new TypedProperties(hoodieConfig.getProps)),
        toProperties(parameters))
      val timelineTimeZone = HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE))
      val tableMetaClient = if (tableExists) {
        HoodieInstantTimeGenerator.setCommitTimeZone(timelineTimeZone)
        HoodieTableMetaClient.builder
          .setConf(sparkContext.hadoopConfiguration)
          .setBasePath(path)
          .build()
      } else {
        val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.BASE_FILE_FORMAT)
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.ARCHIVELOG_FOLDER)
        val populateMetaFields = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)
        val useBaseFormatMetaFile = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT);
        HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(tableType)
          .setDatabaseName(databaseName)
          .setTableName(tblName)
          .setBaseFileFormat(baseFileFormat)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(hoodieConfig.getString(PAYLOAD_CLASS_NAME))
          // we can't fetch preCombine field from hoodieConfig object, since it falls back to "ts" as default value,
          // but we are interested in what user has set, hence fetching from optParams.
          .setPreCombineField(optParams.getOrElse(PRECOMBINE_FIELD.key(), null))
          .setPartitionFields(partitionColumns)
          .setPopulateMetaFields(populateMetaFields)
          .setRecordKeyFields(hoodieConfig.getString(RECORDKEY_FIELD))
          .setCDCEnabled(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED))
          .setCDCSupplementalLoggingMode(hoodieConfig.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE))
          .setKeyGeneratorClassProp(hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key))
          .set(timestampKeyGeneratorConfigs)
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .setShouldDropPartitionColumns(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.DROP_PARTITION_COLUMNS))
          .setCommitTimezone(timelineTimeZone)
          .initTable(sparkContext.hadoopConfiguration, path)
      }
      val instantTime = HoodieActiveTimeline.createNewInstantTime()
      tableConfig = tableMetaClient.getTableConfig

      val commitActionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)

      // Register Avro classes ([[Schema]], [[GenericData]]) w/ Kryo
      sparkContext.getConf.registerKryoClasses(
        Array(classOf[GenericData],
          classOf[Schema]))

      val shouldReconcileSchema = parameters(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
      val latestTableSchemaOpt = getLatestTableSchema(spark, tableIdentifier, tableMetaClient)
      val df = if (preppedWriteOperation || preppedSparkSqlWrites || preppedSparkSqlMergeInto) {
        sourceDf
      } else {
        sourceDf.drop(HoodieRecord.HOODIE_META_COLUMNS: _*)
      }
      // NOTE: We need to make sure that upon conversion of the schemas b/w Catalyst's [[StructType]] and
      //       Avro's [[Schema]] we're preserving corresponding "record-name" and "record-namespace" that
      //       play crucial role in establishing compatibility b/w schemas
      val (avroRecordName, avroRecordNamespace) = latestTableSchemaOpt.map(s => (s.getName, s.getNamespace))
        .getOrElse(getAvroRecordNameAndNamespace(tblName))

      val sourceSchema = convertStructTypeToAvroSchema(df.schema, avroRecordName, avroRecordNamespace)
      val internalSchemaOpt = getLatestTableInternalSchema(hoodieConfig, tableMetaClient).orElse {
        // In case we need to reconcile the schema and schema evolution is enabled,
        // we will force-apply schema evolution to the writer's schema
        if (shouldReconcileSchema && hoodieConfig.getBooleanOrDefault(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
          val allowOperationMetaDataField = parameters.getOrElse(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key(), "false").toBoolean
          Some(AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(latestTableSchemaOpt.getOrElse(sourceSchema), allowOperationMetaDataField)))
        } else {
          None
        }
      }

      val (writeResult, writeClient: SparkRDDWriteClient[_]) =
        operation match {
          case WriteOperationType.DELETE | WriteOperationType.DELETE_PREPPED =>
            mayBeValidateParamsForAutoGenerationOfRecordKeys(parameters, hoodieConfig)
            val genericRecords = HoodieSparkUtils.createRdd(df, avroRecordName, avroRecordNamespace)
            // Convert to RDD[HoodieKey]
            val hoodieKeysAndLocationsToDelete = genericRecords.mapPartitions(it => {
              val keyGenerator: Option[BaseKeyGenerator] = if (preppedSparkSqlWrites || preppedWriteOperation) {
                None
              } else {
                Some(HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(hoodieConfig.getProps))
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
            val internalSchemaOpt = getLatestTableInternalSchema(hoodieConfig, tableMetaClient)
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
              null, path, tblName,
              mapAsJavaMap(addSchemaEvolutionParameters(parameters, internalSchemaOpt) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key)))
              .asInstanceOf[SparkRDDWriteClient[_]]

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              asyncCompactionTriggerFn.get.apply(client)
            }
            if (isAsyncClusteringEnabled(client, parameters)) {
              asyncClusteringTriggerFn.get.apply(client)
            }

            // Issue deletes
            client.startCommitWithTime(instantTime, commitActionType)
            val writeStatuses = DataSourceUtils.doDeleteOperation(client, hoodieKeysAndLocationsToDelete, instantTime, preppedSparkSqlWrites || preppedWriteOperation)
            (writeStatuses, client)

          case WriteOperationType.DELETE_PARTITION =>
            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(hoodieConfig.getProps))
            // Get list of partitions to delete
            val partitionsToDelete = if (parameters.containsKey(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key())) {
              val partitionColsToDelete = parameters(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key()).split(",")
              java.util.Arrays.asList(resolvePartitionWildcards(java.util.Arrays.asList(partitionColsToDelete: _*).toList, jsc,
                hoodieConfig, basePath.toString): _*)
            } else {
              val genericRecords = HoodieSparkUtils.createRdd(df, avroRecordName, avroRecordNamespace)
              genericRecords.map(gr => keyGenerator.getKey(gr).getPartitionPath).toJavaRDD().distinct().collect()
            }

            // Create a HoodieWriteClient & issue the delete.
            val tableMetaClient = HoodieTableMetaClient.builder
              .setConf(sparkContext.hadoopConfiguration).setBasePath(basePath.toString).build()
            val schemaStr = new TableSchemaResolver(tableMetaClient).getTableAvroSchema.toString
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
              schemaStr, path, tblName,
              mapAsJavaMap(parameters - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key)))
              .asInstanceOf[SparkRDDWriteClient[_]]
            // Issue delete partitions
            client.startCommitWithTime(instantTime, commitActionType)
            val writeStatuses = DataSourceUtils.doDeletePartitionsOperation(client, partitionsToDelete, instantTime)
            (writeStatuses, client)

          // Here all other (than DELETE, DELETE_PARTITION) write operations are handled
          case _ =>
            // NOTE: Target writer's schema is deduced based on
            //         - Source's schema
            //         - Existing table's schema (including its Hudi's [[InternalSchema]] representation)
            val writerSchema = deduceWriterSchema(sourceSchema, latestTableSchemaOpt, internalSchemaOpt, parameters)

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
              val finalOpts = addSchemaEvolutionParameters(parameters, internalSchemaOpt, Some(writerSchema)) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key
              // TODO(HUDI-4772) proper writer-schema has to be specified here
              DataSourceUtils.createHoodieClient(jsc, processedDataSchema.toString, path, tblName, mapAsJavaMap(finalOpts))
            }

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              asyncCompactionTriggerFn.get.apply(client)
            }

            if (isAsyncClusteringEnabled(client, parameters)) {
              asyncClusteringTriggerFn.get.apply(client)
            }

            // Short-circuit if bulk_insert via row is enabled.
            // scalastyle:off
            if (hoodieConfig.getBoolean(ENABLE_ROW_WRITER) && operation == WriteOperationType.BULK_INSERT && !isConsistentHashingBucketIndex(client)) {
              return bulkInsertAsRow(client, parameters, hoodieConfig, df, mode, tblName, basePath,
                instantTime, writerSchema, tableConfig)
            }
            // scalastyle:on

            val writeConfig = client.getConfig
            if (writeConfig.getRecordMerger.getRecordType == HoodieRecordType.SPARK && tableType == HoodieTableType.MERGE_ON_READ && writeConfig.getLogDataBlockFormat.orElse(HoodieLogBlockType.AVRO_DATA_BLOCK) != HoodieLogBlockType.PARQUET_DATA_BLOCK) {
              throw new UnsupportedOperationException(s"${writeConfig.getRecordMerger.getClass.getName} only support parquet log.")
            }
            // Convert to RDD[HoodieRecord]
            val hoodieRecords =
              HoodieCreateRecordUtils.createHoodieRecordRdd(HoodieCreateRecordUtils.createHoodieRecordRddArgs(df,
                writeConfig, parameters, avroRecordName, avroRecordNamespace, writerSchema,
                processedDataSchema, operation, instantTime, preppedSparkSqlWrites, preppedSparkSqlMergeInto, preppedWriteOperation))

            val dedupedHoodieRecords =
              if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
                DataSourceUtils.dropDuplicates(jsc, hoodieRecords, mapAsJavaMap(parameters))
              } else {
                hoodieRecords
              }
            client.startCommitWithTime(instantTime, commitActionType)
            val writeResult = DataSourceUtils.doWriteOperation(client, dedupedHoodieRecords, instantTime, operation,
              preppedSparkSqlWrites || preppedWriteOperation)
            (writeResult, client)
        }

      // Check for errors and commit the write.
      try {
        val (writeSuccessful, compactionInstant, clusteringInstant) =
          commitAndPerformPostOperations(sqlContext.sparkSession, df.schema,
            writeResult, parameters, writeClient, tableConfig, jsc,
            TableInstantInfo(basePath, instantTime, commitActionType, operation), extraPreCommitFn)

        (writeSuccessful, common.util.Option.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
      } finally {
        // close the write client in all cases
        val asyncCompactionEnabled = isAsyncCompactionEnabled(writeClient, tableConfig, parameters, jsc.hadoopConfiguration())
        val asyncClusteringEnabled = isAsyncClusteringEnabled(writeClient, parameters)
        if (!asyncCompactionEnabled && !asyncClusteringEnabled) {
          log.info("Closing write client")
          writeClient.close()
        }
      }
    }
  }

  def deduceOperation(hoodieConfig: HoodieConfig, paramsWithoutDefaults : Map[String, String], df: Dataset[Row]): WriteOperationType = {
    var operation = WriteOperationType.fromValue(hoodieConfig.getString(OPERATION))
    // TODO clean up
    // It does not make sense to allow upsert() operation if INSERT_DROP_DUPS is true
    // Auto-correct the operation to "insert" if OPERATION is set to "upsert" wrongly
    // or not set (in which case it will be set as "upsert" by parametersWithWriteDefaults()) .
    if (hoodieConfig.getBoolean(INSERT_DROP_DUPS) &&
      operation == WriteOperationType.UPSERT) {

      log.warn(s"$UPSERT_OPERATION_OPT_VAL is not applicable " +
        s"when $INSERT_DROP_DUPS is set to be true, " +
        s"overriding the $OPERATION to be $INSERT_OPERATION_OPT_VAL")

      operation = WriteOperationType.INSERT
      operation
    } else {
      // if no record key, and no meta fields, we should treat it as append only workload and make bulk_insert as operation type.
      if (!paramsWithoutDefaults.containsKey(DataSourceWriteOptions.RECORDKEY_FIELD.key())
        && !paramsWithoutDefaults.containsKey(OPERATION.key()) && !df.schema.fieldNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
        log.warn(s"Choosing BULK_INSERT as the operation type since auto record key generation is applicable")
        operation = WriteOperationType.BULK_INSERT
      }
      operation
    }
  }

  /**
   * Deduces writer's schema based on
   * <ul>
   *   <li>Source's schema</li>
   *   <li>Target table's schema (including Hudi's [[InternalSchema]] representation)</li>
   * </ul>
   */
  def deduceWriterSchema(sourceSchema: Schema,
                         latestTableSchemaOpt: Option[Schema],
                         internalSchemaOpt: Option[InternalSchema],
                         opts: Map[String, String]): Schema = {
    val shouldReconcileSchema = opts(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
    val shouldValidateSchemasCompatibility = opts.getOrDefault(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key,
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue).toBoolean

    latestTableSchemaOpt match {
      // In case table schema is empty we're just going to use the source schema as a
      // writer's schema. No additional handling is required
      case None => sourceSchema
      // Otherwise, we need to make sure we reconcile incoming and latest table schemas
      case Some(latestTableSchemaWithMetaFields) =>
        // NOTE: Meta-fields will be unconditionally injected by Hudi writing handles, for the sake of
        //       deducing proper writer schema we're stripping them to make sure we can perform proper
        //       analysis
        val latestTableSchema = removeMetadataFields(latestTableSchemaWithMetaFields)
        // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
        // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
        // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
        // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
        val shouldCanonicalizeNullable = opts.getOrDefault(CANONICALIZE_NULLABLE.key,
          CANONICALIZE_NULLABLE.defaultValue.toString).toBoolean
        val mergeIntoWrites = opts.getOrDefault(SQL_MERGE_INTO_WRITES.key(),
          SQL_MERGE_INTO_WRITES.defaultValue.toString).toBoolean

        val canonicalizedSourceSchema = if (shouldCanonicalizeNullable) {
          canonicalizeSchema(sourceSchema, latestTableSchema, opts)
        } else {
          sourceSchema
        }

        val allowAutoEvolutionColumnDrop = opts.getOrDefault(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key,
          HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.defaultValue).toBoolean

        if (shouldReconcileSchema) {
          internalSchemaOpt match {
            case Some(internalSchema) =>
              // Apply schema evolution, by auto-merging write schema and read schema
              val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, internalSchema)
              val evolvedSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, latestTableSchema.getFullName)
              val shouldRemoveMetaDataFromInternalSchema = sourceSchema.getFields().filter(f => f.name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)).isEmpty
              if (shouldRemoveMetaDataFromInternalSchema) HoodieAvroUtils.removeMetadataFields(evolvedSchema) else evolvedSchema
            case None =>
              // In case schema reconciliation is enabled we will employ (legacy) reconciliation
              // strategy to produce target writer's schema (see definition below)
              val (reconciledSchema, isCompatible) =
                reconcileSchemasLegacy(latestTableSchema, canonicalizedSourceSchema)

              // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
              //       w/ the table's one and allow schemas to diverge. This is required in cases where
              //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
              //       only incoming dataset's projection has to match the table's schema, and not the whole one
              if (!shouldValidateSchemasCompatibility || isCompatible) {
                reconciledSchema
              } else {
                log.error(
                  s"""Failed to reconcile incoming batch schema with the table's one.
                     |Incoming schema ${sourceSchema.toString(true)}
                     |Incoming schema (canonicalized) ${canonicalizedSourceSchema.toString(true)}
                     |Table's schema ${latestTableSchema.toString(true)}
                     |""".stripMargin)
                throw new SchemaCompatibilityException("Failed to reconcile incoming schema with the table's one")
              }
          }
        } else {
          // In case reconciliation is disabled, we have to validate that the source's schema
          // is compatible w/ the table's latest schema, such that we're able to read existing table's
          // records using [[sourceSchema]].
          //
          // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
          //       w/ the table's one and allow schemas to diverge. This is required in cases where
          //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
          //       only incoming dataset's projection has to match the table's schema, and not the whole one

          if (mergeIntoWrites) {
            // if its merge into writes, do not check for projection nor schema compatibility. Writers down the line will
            // take care of it.
            canonicalizedSourceSchema
          } else {
            if (!shouldValidateSchemasCompatibility) {
              // if no validation is enabled, check for col drop
              // if col drop is allowed, go ahead. if not, check for projection, so that we do not allow dropping cols
              if (allowAutoEvolutionColumnDrop || canProject(latestTableSchema, canonicalizedSourceSchema)) {
                canonicalizedSourceSchema
              } else {
                log.error(
                  s"""Incoming batch schema is not compatible with the table's one.
                   |Incoming schema ${sourceSchema.toString(true)}
                   |Incoming schema (canonicalized) ${canonicalizedSourceSchema.toString(true)}
                   |Table's schema ${latestTableSchema.toString(true)}
                   |""".stripMargin)
                throw new SchemaCompatibilityException("Incoming batch schema is not compatible with the table's one")
              }
            } else if (isSchemaCompatible(latestTableSchema, canonicalizedSourceSchema, allowAutoEvolutionColumnDrop)) {
                canonicalizedSourceSchema
            } else {
                log.error(
                s"""Incoming batch schema is not compatible with the table's one.
                   |Incoming schema ${sourceSchema.toString(true)}
                   |Incoming schema (canonicalized) ${canonicalizedSourceSchema.toString(true)}
                   |Table's schema ${latestTableSchema.toString(true)}
                   |""".stripMargin)
                throw new SchemaCompatibilityException("Incoming batch schema is not compatible with the table's one")
              }
            }
        }
    }
  }

  /**
   * Resolve wildcards in partitions
   *
   * @param partitions   list of partitions that may contain wildcards
   * @param jsc          instance of java spark context
   * @param cfg          hoodie config
   * @param basePath     base path
   * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
   */
  private def resolvePartitionWildcards(partitions: List[String], jsc: JavaSparkContext, cfg: HoodieConfig, basePath: String): List[String] = {
    //find out if any of the input partitions have wildcards
    var (wildcardPartitions, fullPartitions) = partitions.partition(partition => partition.contains("*"))

    if (wildcardPartitions.nonEmpty) {
      //get list of all partitions
      val allPartitions = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc): HoodieEngineContext,
        HoodieMetadataConfig.newBuilder().fromProperties(cfg.getProps).build(), basePath)
      //go through list of partitions with wildcards and add all partitions that match to val fullPartitions
      wildcardPartitions.foreach(partition => {
        //turn wildcard into regex
        //regex for start of line is ^ and end of line is $
        //If the partitionpath was just alphanumeric we would just replace * with .*
        //Since there could be characters that could be considered regex in the partitionpath we must
        //prevent that from happening. Any text inbetween \\Q and \\E is considered literal
        //So we start the string with \\Q and end with \\E and then whenever we find a * we add \\E before
        //and \\Q after so all other characters besides .* will be enclosed between a set of \\Q \\E
        val regexPartition = "^\\Q" + partition.replace("*", "\\E.*\\Q") + "\\E$"

        //filter all partitions with the regex and append the result to the list of full partitions
        fullPartitions = List.concat(fullPartitions,allPartitions.filter(_.matches(regexPartition)))
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

    val schemaValidateEnable = if (schemaEvolutionEnable.toBoolean && parameters.getOrDefault(DataSourceWriteOptions.RECONCILE_SCHEMA.key(), "false").toBoolean) {
      // force disable schema validate, now we support schema evolution, no need to do validate
      "false"
    } else  {
      parameters.getOrDefault(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key(), "true")
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
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key()  -> schemaValidateEnable)
  }

  private def reconcileSchemasLegacy(tableSchema: Schema, newSchema: Schema): (Schema, Boolean) = {
    // Legacy reconciliation implements following semantic
    //    - In case new-schema is a "compatible" projection of the existing table's one (projection allowing
    //      permitted type promotions), table's schema would be picked as (reconciled) writer's schema;
    //    - Otherwise, we'd fall back to picking new (batch's) schema as a writer's schema;
    //
    // Philosophically, such semantic aims at always choosing a "wider" schema, ie the one containing
    // the other one (schema A contains schema B, if schema B is a projection of A). This enables us,
    // to always "extend" the schema during schema evolution and hence never lose the data (when, for ex
    // existing column is being dropped in a new batch)
    //
    // NOTE: By default Hudi doesn't allow automatic schema evolution to drop the columns from the target
    //       table. However, when schema reconciliation is turned on, we would allow columns to be dropped
    //       in the incoming batch (as these would be reconciled in anyway)
    if (isCompatibleProjectionOf(tableSchema, newSchema)) {
      // Picking table schema as a writer schema we need to validate that we'd be able to
      // rewrite incoming batch's data (written in new schema) into it
      (tableSchema, isSchemaCompatible(newSchema, tableSchema))
    } else {
      // Picking new schema as a writer schema we need to validate that we'd be able to
      // rewrite table's data into it
      (newSchema, isSchemaCompatible(tableSchema, newSchema))
    }
  }

  /**
   * Canonicalizes [[sourceSchema]] by reconciling it w/ [[latestTableSchema]] in following
   *
   * <ol>
   *  <li>Nullability: making sure that nullability of the fields in the source schema is matching
   *  that of the latest table's ones</li>
   * </ol>
   *
   * TODO support casing reconciliation
   */
  private def canonicalizeSchema(sourceSchema: Schema, latestTableSchema: Schema, opts : Map[String, String]): Schema = {
    reconcileNullability(sourceSchema, latestTableSchema, opts)
  }


  /**
   * get latest internalSchema from table
   *
   * @param config instance of {@link HoodieConfig}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
   */
  def getLatestTableInternalSchema(config: HoodieConfig,
                                   tableMetaClient: HoodieTableMetaClient): Option[InternalSchema] = {
    if (!config.getBooleanOrDefault(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
      Option.empty[InternalSchema]
    } else {
      try {
        val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
        val internalSchemaOpt = tableSchemaResolver.getTableInternalSchemaFromCommitMetadata
        if (internalSchemaOpt.isPresent) Some(internalSchemaOpt.get()) else None
      } catch {
        case _: Exception => None
      }
    }
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

    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode)
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
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.ARCHIVELOG_FOLDER)
        val partitionColumns = HoodieWriterUtils.getPartitionColumns(parameters)
        val recordKeyFields = hoodieConfig.getString(DataSourceWriteOptions.RECORDKEY_FIELD)
        val keyGenProp =
          if (StringUtils.nonEmpty(hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)))
            hoodieConfig.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)
          else hoodieConfig.getString(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)
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

        HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.valueOf(tableType))
          .setTableName(tableName)
          .setRecordKeyFields(recordKeyFields)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(hoodieConfig.getStringOrDefault(PAYLOAD_CLASS_NAME))
          .setPreCombineField(hoodieConfig.getStringOrDefault(PRECOMBINE_FIELD, null))
          .setBootstrapIndexClass(bootstrapIndexClass)
          .setBaseFileFormat(baseFileFormat)
          .setBootstrapBasePath(bootstrapBasePath)
          .setPartitionFields(partitionColumns)
          .setCDCEnabled(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED))
          .setCDCSupplementalLoggingMode(hoodieConfig.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE))
          .setPopulateMetaFields(populateMetaFields)
          .setKeyGeneratorClassProp(keyGenProp)
          .set(timestampKeyGeneratorConfigs)
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setCommitTimezone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE)))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .initTable(sparkContext.hadoopConfiguration, path)
      }

      val jsc = new JavaSparkContext(sqlContext.sparkContext)
      val writeClient = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
        schema, path, tableName, mapAsJavaMap(parameters)))
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
                      instantTime: String,
                      writerSchema: Schema,
                      tableConfig: HoodieTableConfig):
  (Boolean, HOption[String], HOption[String], HOption[String], SparkRDDWriteClient[_], HoodieTableConfig) = {
    if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet")
    }
    val sqlContext = writeClient.getEngineContext.asInstanceOf[HoodieSparkEngineContext].getSqlContext
    val jsc = writeClient.getEngineContext.asInstanceOf[HoodieSparkEngineContext].getJavaSparkContext

    val writerSchemaStr = writerSchema.toString

    // Make opts mutable since it could be modified by tryOverrideParquetWriteLegacyFormatProperty
    val opts = mutable.Map() ++ hoodieConfig.getProps.toMap ++
      Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key -> writerSchemaStr)

    // Auto set the value of "hoodie.parquet.writelegacyformat.enabled"
    tryOverrideParquetWriteLegacyFormatProperty(opts, convertAvroSchemaToStructType(writerSchema))
    val writeConfig = DataSourceUtils.createHoodieConfig(writerSchemaStr, basePath.toString, tblName, opts)
    val overwriteOperationType = Option(hoodieConfig.getString(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE))
      .map(WriteOperationType.fromValue)
      .orNull
    val executor = mode match {
      case _ if overwriteOperationType == null =>
        // Don't need to overwrite
        new DatasetBulkInsertCommitActionExecutor(writeConfig, writeClient, instantTime)
      case SaveMode.Append if overwriteOperationType == WriteOperationType.INSERT_OVERWRITE =>
        // INSERT OVERWRITE PARTITION uses Append mode
        new DatasetBulkInsertOverwriteCommitActionExecutor(writeConfig, writeClient, instantTime)
      case SaveMode.Overwrite if overwriteOperationType == WriteOperationType.INSERT_OVERWRITE_TABLE =>
        new DatasetBulkInsertOverwriteTableCommitActionExecutor(writeConfig, writeClient, instantTime)
      case _ =>
        throw new HoodieException(s"$mode with bulk_insert in row writer path is not supported yet");
    }

    val writeResult = executor.execute(df, tableConfig.isTablePartitioned)

    try {
      val (writeSuccessful, compactionInstant, clusteringInstant) = mode match {
        case _ if overwriteOperationType == null =>
          val syncHiveSuccess = metaSync(sqlContext.sparkSession, writeConfig, basePath, df.schema)
          (syncHiveSuccess, HOption.empty().asInstanceOf[HOption[String]], HOption.empty().asInstanceOf[HOption[String]])
        case _ =>
          try {
            commitAndPerformPostOperations(sqlContext.sparkSession, df.schema, writeResult, parameters, writeClient, tableConfig, jsc,
                TableInstantInfo(basePath, instantTime, executor.getCommitActionType, executor.getWriteOperationType), Option.empty)

          }
      }
      (writeSuccessful, HOption.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
    } finally {
      // close the write client in all cases
      val asyncCompactionEnabled = isAsyncCompactionEnabled(writeClient, tableConfig, parameters, jsc.hadoopConfiguration())
      val asyncClusteringEnabled = isAsyncClusteringEnabled(writeClient, parameters)
      if (!asyncCompactionEnabled && !asyncClusteringEnabled) {
        log.info("Closing write client")
        writeClient.close()
      }
    }
  }

  def cleanup() : Unit = {
    Metrics.shutdownAllMetrics()
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
    var syncClientToolClassSet = scala.collection.mutable.Set[String]()
    hoodieConfig.getString(META_SYNC_CLIENT_TOOL_CLASS_NAME).split(",").foreach(syncClass => syncClientToolClassSet += syncClass)

    // for backward compatibility
    if (hiveSyncEnabled) metaSyncEnabled = true


    if (metaSyncEnabled) {
      val fs = basePath.getFileSystem(spark.sessionState.newHadoopConf())
      val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT);
      val properties = TypedProperties.fromMap(hoodieConfig.getProps)
      properties.put(HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key, spark.sessionState.conf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD).toString)
      properties.put(HoodieSyncConfig.META_SYNC_SPARK_VERSION.key, SPARK_VERSION)
      properties.put(HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA.key, hoodieConfig.getBoolean(HoodieMetadataConfig.ENABLE))

      // Collect exceptions in list because we want all sync to run. Then we can throw
      val failedMetaSyncs = new mutable.HashMap[String,HoodieException]()
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
        throw getHoodieMetaSyncException(failedMetaSyncs)
      }
    }

    // Since Hive tables are now synced as Spark data source tables which are cached after Spark SQL queries
    // we must invalidate this table in the cache so writes are reflected in later queries
    if (metaSyncEnabled) {
      getHiveTableNames(hoodieConfig).foreach(name => {
        val qualifiedTableName = String.join(".", hoodieConfig.getStringOrDefault(HIVE_DATABASE), name)
        if (spark.catalog.tableExists(qualifiedTableName)) {
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
    if (writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).count() == 0) {
      log.info("Proceeding to commit the write.")
      val metaMap = parameters.filter(kv =>
        kv._1.startsWith(parameters(COMMIT_METADATA_KEYPREFIX.key)))
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          common.util.Option.of(new java.util.HashMap[String, String](mapAsJavaMap(metaMap))),
          tableInstantInfo.commitActionType,
          writeResult.getPartitionToReplaceFileIds,
          common.util.Option.ofNullable(extraPreCommitFn.orNull))

      if (commitSuccess) {
        log.info("Commit " + tableInstantInfo.instantTime + " successful!")
      }
      else {
        log.info("Commit " + tableInstantInfo.instantTime + " failed!")
      }

      val asyncCompactionEnabled = isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())
      val compactionInstant: common.util.Option[java.lang.String] =
        if (asyncCompactionEnabled) {
          client.scheduleCompaction(common.util.Option.of(new java.util.HashMap[String, String](mapAsJavaMap(metaMap))))
        } else {
          common.util.Option.empty()
        }

      log.info(s"Compaction Scheduled is $compactionInstant")

      val asyncClusteringEnabled = isAsyncClusteringEnabled(client, parameters)
      val clusteringInstant: common.util.Option[java.lang.String] =
        if (asyncClusteringEnabled) {
          client.scheduleClustering(common.util.Option.of(new java.util.HashMap[String, String](mapAsJavaMap(metaMap))))
        } else {
          common.util.Option.empty()
        }

      log.info(s"Clustering Scheduled is $clusteringInstant")

      val metaSyncSuccess = metaSync(spark, HoodieWriterUtils.convertMapToHoodieConfig(parameters),
        tableInstantInfo.basePath, schema)

      log.info(s"Is Async Compaction Enabled ? $asyncCompactionEnabled")
      (commitSuccess && metaSyncSuccess, compactionInstant, clusteringInstant)
    } else {
      log.error(s"${tableInstantInfo.operation} failed with errors")
      if (log.isTraceEnabled) {
        log.trace("Printing out the top 100 errors")
        writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors)
          .take(100)
          .foreach(ws => {
            log.trace("Global error :", ws.getGlobalError)
            if (ws.getErrors.size() > 0) {
              ws.getErrors.foreach(kt =>
                log.trace(s"Error for key: ${kt._1}", kt._2))
            }
          })
      }
      (false, common.util.Option.empty(), common.util.Option.empty())
    }
  }

  private def isAsyncCompactionEnabled(client: SparkRDDWriteClient[_],
                                       tableConfig: HoodieTableConfig,
                                       parameters: Map[String, String], configuration: Configuration): Boolean = {
    log.info(s"Config.inlineCompactionEnabled ? ${client.getConfig.inlineCompactionEnabled}")
    (asyncCompactionTriggerFnDefined && !client.getConfig.inlineCompactionEnabled
      && parameters.get(ASYNC_COMPACT_ENABLE.key).exists(r => r.toBoolean)
      && tableConfig.getTableType == HoodieTableType.MERGE_ON_READ)
  }

  private def isAsyncClusteringEnabled(client: SparkRDDWriteClient[_],
                                       parameters: Map[String, String]): Boolean = {
    log.info(s"Config.asyncClusteringEnabled ? ${client.getConfig.isAsyncClusteringEnabled}")
    (asyncClusteringTriggerFnDefined && !client.getConfig.inlineClusteringEnabled
      && client.getConfig.isAsyncClusteringEnabled)
  }

  private def isConsistentHashingBucketIndex(client: SparkRDDWriteClient[_]): Boolean = {
    client.getConfig.getIndexType == IndexType.BUCKET && client.getConfig.getBucketIndexEngineType == HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING
  }

  /**
   * Fetch table config for an already existing table and if save mode is not Overwrite.
   * @param sparkContext instance of {@link SparkContext} to use.
   * @param tablePath table base path.
   * @param mode save mode in use.
   * @param hoodieTableConfigOpt return table config from this Option if present. else poll from a new metaClient.
   * @return {@link HoodieTableConfig} is conditions match. if not, returns null.
   */
  private def getHoodieTableConfig(sparkContext: SparkContext,
                                   tablePath: String,
                                   mode: SaveMode,
                                   hoodieTableConfigOpt: Option[HoodieTableConfig]): HoodieTableConfig = {
    if (tableExists && mode != SaveMode.Overwrite) {
      hoodieTableConfigOpt.getOrElse(
        HoodieTableMetaClient.builder().setConf(sparkContext.hadoopConfiguration).setBasePath(tablePath)
          .build().getTableConfig)
    } else {
      null
    }
  }

  private def mergeParamsAndGetHoodieConfig(optParams: Map[String, String],
                                            tableConfig: HoodieTableConfig, mode: SaveMode): (Map[String, String], HoodieConfig) = {
    val translatedOptions = DataSourceWriteOptions.mayBeDerivePartitionPath(optParams)
    var translatedOptsWithMappedTableConfig = mutable.Map.empty ++ translatedOptions.toMap
    if (tableConfig != null && mode != SaveMode.Overwrite) {
      // for missing write configs corresponding to table configs, fill them up.
      fetchMissingWriteConfigsFromTableConfig(tableConfig, optParams).foreach((kv) => translatedOptsWithMappedTableConfig += (kv._1 -> kv._2))
    }
    if (null != tableConfig && mode != SaveMode.Overwrite) {
      // over-ride only if not explicitly set by the user.
      tableConfig.getProps.filter(kv => !optParams.contains(kv._1))
        .foreach { case (key, value) =>
          translatedOptsWithMappedTableConfig +=  (key -> value)
        }
    }
    val mergedParams = mutable.Map.empty ++ HoodieWriterUtils.parametersWithWriteDefaults(translatedOptsWithMappedTableConfig.toMap)
    if (!mergedParams.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)
      && mergedParams.contains(KEYGENERATOR_CLASS_NAME.key)) {
      mergedParams(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) = mergedParams(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key)
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
    val params = mergedParams.toMap
    (params, HoodieWriterUtils.convertMapToHoodieConfig(params))
  }

  private def extractConfigsRelatedToTimestampBasedKeyGenerator(keyGenerator: String,
                                                                params: Map[String, String]): Map[String, String] = {
    if (classOf[TimestampBasedKeyGenerator].getCanonicalName.equals(keyGenerator) ||
      classOf[TimestampBasedAvroKeyGenerator].getCanonicalName.equals(keyGenerator)) {
      params.filterKeys(HoodieTableConfig.PERSISTED_CONFIG_LIST.contains)
    } else {
      Map.empty
    }
  }
}
