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
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.AvroConversionUtils.{convertStructTypeToAvroSchema, getAvroRecordNameAndNamespace}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.{toProperties, toScalaOption}
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.avro.AvroSchemaUtils.{isCompatibleProjectionOf, isSchemaCompatible}
import org.apache.hudi.avro.{AvroSchemaUtils, HoodieAvroUtils}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieConfig, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{CommitUtils, StringUtils}
import org.apache.hudi.config.HoodieBootstrapConfig.{BASE_PATH, INDEX_CLASS_NAME, KEYGEN_CLASS_NAME}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieException, SchemaCompatibilityException}
import org.apache.hudi.execution.bulkinsert.{BulkInsertInternalPartitionerWithRowsFactory, NonSortPartitionerWithRows}
import org.apache.hudi.hive.{HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.internal.DataSourceInternalWriterHelper
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{AvroSchemaEvolutionUtils, SerDeHelper}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.{SparkKeyGeneratorInterface, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.metrics.Metrics
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.sync.common.util.SyncUtilHelpers
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.hudi.util.SparkKeyGenUtils
import org.apache.log4j.LogManager
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SPARK_VERSION, SparkContext}

import java.util.function.BiConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HoodieSparkSqlWriter {

  private val log = LogManager.getLogger(getClass)
  private var tableExists: Boolean = false
  private var asyncCompactionTriggerFnDefined: Boolean = false
  private var asyncClusteringTriggerFnDefined: Boolean = false

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            optParams: Map[String, String],
            df: DataFrame,
            hoodieTableConfigOpt: Option[HoodieTableConfig] = Option.empty,
            hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty,
            asyncCompactionTriggerFn: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]] => Unit] = Option.empty,
            asyncClusteringTriggerFn: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]] => Unit] = Option.empty,
            extraPreCommitFn: Option[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]] = Option.empty)
  : (Boolean, common.util.Option[String], common.util.Option[String], common.util.Option[String],
    SparkRDDWriteClient[HoodieRecordPayload[Nothing]], HoodieTableConfig) = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)

    val spark = sqlContext.sparkSession
    val sparkContext = sqlContext.sparkContext

    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    var tableConfig = getHoodieTableConfig(sparkContext, path, hoodieTableConfigOpt)
    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode)
    val originKeyGeneratorClassName = HoodieWriterUtils.getOriginKeyGenerator(parameters)
    val timestampKeyGeneratorConfigs = extractConfigsRelatedToTimestampBasedKeyGenerator(
      originKeyGeneratorClassName, parameters)

    // Validate datasource and tableconfig keygen are the same
    validateKeyGeneratorConfig(originKeyGeneratorClassName, tableConfig);
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite);

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
    }

    val jsc = new JavaSparkContext(sparkContext)
    if (asyncCompactionTriggerFn.isDefined) {
      if (jsc.getConf.getOption(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY).isDefined) {
        jsc.setLocalProperty("spark.scheduler.pool", SparkConfigs.SPARK_DATASOURCE_WRITER_POOL_NAME)
      }
    }
    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(hoodieConfig.getProps))

    if (mode == SaveMode.Ignore && tableExists) {
      log.warn(s"hoodie table at $basePath already exists. Ignoring & not performing actual writes.")
      (false, common.util.Option.empty(), common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
    } else {
      // Handle various save modes
      handleSaveModes(sqlContext.sparkSession, mode, basePath, tableConfig, tblName, operation, fs)
      val partitionColumns = SparkKeyGenUtils.getPartitionColumns(keyGenerator, toProperties(parameters))
      val tableMetaClient = if (tableExists) {
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
          .setKeyGeneratorClassProp(originKeyGeneratorClassName)
          .set(timestampKeyGeneratorConfigs)
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .setShouldDropPartitionColumns(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.DROP_PARTITION_COLUMNS))
          .setCommitTimezone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE)))
          .initTable(sparkContext.hadoopConfiguration, path)
      }
      tableConfig = tableMetaClient.getTableConfig

      val commitActionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)

      // Register Avro classes ([[Schema]], [[GenericData]]) w/ Kryo
      sparkContext.getConf.registerKryoClasses(
        Array(classOf[GenericData],
          classOf[Schema]))

      val shouldReconcileSchema = parameters(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
      val latestTableSchemaOpt = getLatestTableSchema(spark, tableIdentifier, tableMetaClient)
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

      // NOTE: Target writer's schema is deduced based on
      //         - Source's schema
      //         - Existing table's schema (including its Hudi's [[InternalSchema]] representation)
      val writerSchema = deduceWriterSchema(sourceSchema, latestTableSchemaOpt, internalSchemaOpt, parameters)

      validateSchemaForHoodieIsDeleted(writerSchema)

      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING THIS
      //       We have to register w/ Kryo all of the Avro schemas that might potentially be used to decode
      //       records into Avro format. Otherwise, Kryo wouldn't be able to apply an optimization allowing
      //       it to avoid the need to ser/de the whole schema along _every_ Avro record
      val targetAvroSchemas = sourceSchema +: writerSchema +: latestTableSchemaOpt.toSeq
      registerAvroSchemasWithKryo(sparkContext, targetAvroSchemas: _*)

      log.info(s"Registered Avro schemas: ${targetAvroSchemas.map(_.toString(true)).mkString("\n")}")

      // Short-circuit if bulk_insert via row is enabled.
      // scalastyle:off
      if (hoodieConfig.getBoolean(ENABLE_ROW_WRITER) && operation == WriteOperationType.BULK_INSERT) {
        val (success, commitTime: common.util.Option[String]) = bulkInsertAsRow(sqlContext, hoodieConfig, df, tblName,
          basePath, path, instantTime, writerSchema, tableConfig.isTablePartitioned)
        return (success, commitTime, common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
      }
      // scalastyle:on

      val (writeResult, writeClient: SparkRDDWriteClient[HoodieRecordPayload[Nothing]]) =
        operation match {
          case WriteOperationType.DELETE =>
            val genericRecords = HoodieSparkUtils.createRdd(df, avroRecordName, avroRecordNamespace)
            // Convert to RDD[HoodieKey]
            val hoodieKeysToDelete = genericRecords.map(gr => keyGenerator.getKey(gr)).toJavaRDD()

            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            // Create a HoodieWriteClient & issue the delete.
            val internalSchemaOpt = getLatestTableInternalSchema(hoodieConfig, tableMetaClient)
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
              null, path, tblName,
              mapAsJavaMap(addSchemaEvolutionParameters(parameters, internalSchemaOpt) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key)))
              .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              asyncCompactionTriggerFn.get.apply(client)
            }
            if (isAsyncClusteringEnabled(client, parameters)) {
              asyncClusteringTriggerFn.get.apply(client)
            }

            // Issue deletes
            client.startCommitWithTime(instantTime, commitActionType)
            val writeStatuses = DataSourceUtils.doDeleteOperation(client, hoodieKeysToDelete, instantTime)
            (writeStatuses, client)

          case WriteOperationType.DELETE_PARTITION =>
            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

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
              .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
            // Issue delete partitions
            client.startCommitWithTime(instantTime, commitActionType)
            val writeStatuses = DataSourceUtils.doDeletePartitionsOperation(client, partitionsToDelete, instantTime)
            (writeStatuses, client)
          case _ =>
            // Here all other (than DELETE, DELETE_PARTITION) write operations are handled

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

            // NOTE: Avro's [[Schema]] can't be effectively serialized by JVM native serialization framework
            //       (due to containing cyclic refs), therefore we have to convert it to string before
            //       passing onto the Executor
            val dataFileSchemaStr = dataFileSchema.toString

            // Create a HoodieWriteClient & issue the write.
            val client = hoodieWriteClient.getOrElse {
              val finalOpts = addSchemaEvolutionParameters(parameters, internalSchemaOpt, Some(writerSchema)) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key
              // TODO(HUDI-4772) proper writer-schema has to be specified here
              DataSourceUtils.createHoodieClient(jsc, dataFileSchemaStr, path, tblName, mapAsJavaMap(finalOpts))
            }.asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
            val writeConfig = client.getConfig
            if (writeConfig.getRecordMerger.getRecordType == HoodieRecordType.SPARK && tableType == HoodieTableType.MERGE_ON_READ && writeConfig.getLogDataBlockFormat.orElse(HoodieLogBlockType.AVRO_DATA_BLOCK) != HoodieLogBlockType.PARQUET_DATA_BLOCK) {
              throw new UnsupportedOperationException(s"${writeConfig.getRecordMerger.getClass.getName} only support parquet log.")
            }
            // Convert to RDD[HoodieRecord]
            val hoodieRecords = createHoodieRecordRdd(
              df,
              writeConfig,
              parameters,
              avroRecordName,
              avroRecordNamespace,
              writerSchema,
              dataFileSchemaStr,
              operation)

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              asyncCompactionTriggerFn.get.apply(client)
            }

            if (isAsyncClusteringEnabled(client, parameters)) {
              asyncClusteringTriggerFn.get.apply(client)
            }

            val dedupedHoodieRecords =
              if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
                DataSourceUtils.dropDuplicates(jsc, hoodieRecords, mapAsJavaMap(parameters))
              } else {
                hoodieRecords
              }
            client.startCommitWithTime(instantTime, commitActionType)
            val writeResult = DataSourceUtils.doWriteOperation(client, dedupedHoodieRecords, instantTime, operation)
            (writeResult, client)
        }

      // Check for errors and commit the write.
      val (writeSuccessful, compactionInstant, clusteringInstant) =
        commitAndPerformPostOperations(sqlContext.sparkSession, df.schema,
          writeResult, parameters, writeClient, tableConfig, jsc,
          TableInstantInfo(basePath, instantTime, commitActionType, operation), extraPreCommitFn)

      (writeSuccessful, common.util.Option.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
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
      case Some(latestTableSchema) =>
        // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
        // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
        // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
        // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
        val shouldCanonicalizeSchema = opts.getOrDefault(DataSourceWriteOptions.CANONICALIZE_SCHEMA.key,
          DataSourceWriteOptions.CANONICALIZE_SCHEMA.defaultValue.toString).toBoolean
        val canonicalizedSourceSchema = if (shouldCanonicalizeSchema) {
          AvroSchemaEvolutionUtils.canonicalizeColumnNullability(sourceSchema, latestTableSchema)
        } else {
          sourceSchema
        }

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
              val (reconciledSchema, isCompatible) = reconcileSchemasLegacy(latestTableSchema, canonicalizedSourceSchema)

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
          if (!shouldValidateSchemasCompatibility || AvroSchemaUtils.isSchemaCompatible(latestTableSchema, canonicalizedSourceSchema)) {
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

  def generateSparkSchemaWithoutPartitionColumns(partitionParam: String, schema: StructType): StructType = {
    val partitionColumns = getPartitionColumns(partitionParam)
    HoodieInternalRowUtils.removeFields(schema, partitionColumns)
  }

  def getAvroProcessedRecord(partitionParam: String, record: GenericRecord,
                             dropPartitionColumns: Boolean): GenericRecord = {
    var processedRecord = record
    if (dropPartitionColumns) {
      val writeSchema = generateSchemaWithoutPartitionColumns(partitionParam, record.getSchema)
      processedRecord = HoodieAvroUtils.rewriteRecord(record, writeSchema)
    }
    processedRecord
  }

  def getProcessedRecord(partitionParam: String, record: GenericRecord,
                         dropPartitionColumns: Boolean): GenericRecord = {
    var processedRecord = record
    if (dropPartitionColumns) {
      val writeSchema = generateSchemaWithoutPartitionColumns(partitionParam, record.getSchema)
      processedRecord = HoodieAvroUtils.rewriteRecord(record, writeSchema)
    }
    processedRecord
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
                hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty): Boolean = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    val tableConfig = getHoodieTableConfig(sparkContext, path, hoodieTableConfigOpt)
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite)

    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode)
    val tableName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TBL_NAME, s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.")
    val tableType = hoodieConfig.getStringOrDefault(TABLE_TYPE)
    val bootstrapBasePath = hoodieConfig.getStringOrThrow(BASE_PATH,
      s"'${BASE_PATH.key}' is required for '${BOOTSTRAP_OPERATION_OPT_VAL}'" +
        " operation'")
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
          if (StringUtils.nonEmpty(hoodieConfig.getString(KEYGEN_CLASS_NAME))) hoodieConfig.getString(KEYGEN_CLASS_NAME)
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
      AvroConversionUtils.resolveAvroTypeNullability(schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).schema())._2.getType != Schema.Type.BOOLEAN) {
      throw new HoodieException(HoodieRecord.HOODIE_IS_DELETED_FIELD + " has to be BOOLEAN type. Passed in dataframe's schema has type "
        + schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).schema().getType)
    }
  }

  def bulkInsertAsRow(sqlContext: SQLContext,
                      hoodieConfig: HoodieConfig,
                      df: DataFrame,
                      tblName: String,
                      basePath: Path,
                      path: String,
                      instantTime: String,
                      writerSchema: Schema,
                      isTablePartitioned: Boolean): (Boolean, common.util.Option[String]) = {
    if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet")
    }

    val writerSchemaStr = writerSchema.toString

    val opts = hoodieConfig.getProps.toMap ++
      Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key -> writerSchemaStr)

    val writeConfig = DataSourceUtils.createHoodieConfig(writerSchemaStr, path, tblName, mapAsJavaMap(opts))
    val populateMetaFields = hoodieConfig.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS)

    val bulkInsertPartitionerRows: BulkInsertPartitioner[Dataset[Row]] = if (populateMetaFields) {
      val userDefinedBulkInsertPartitionerOpt = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(writeConfig)
      if (userDefinedBulkInsertPartitionerOpt.isPresent) {
        userDefinedBulkInsertPartitionerOpt.get
      } else {
        BulkInsertInternalPartitionerWithRowsFactory.get(writeConfig, isTablePartitioned)
      }
    } else {
      // Sort modes are not yet supported when meta fields are disabled
      new NonSortPartitionerWithRows()
    }

    val shouldDropPartitionColumns = hoodieConfig.getBoolean(DataSourceWriteOptions.DROP_PARTITION_COLUMNS)
    val hoodieDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(df, writeConfig, bulkInsertPartitionerRows, shouldDropPartitionColumns)

    val optsOverrides = Map(
      HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED ->
        bulkInsertPartitionerRows.arePartitionRecordsSorted().toString
    )

    val (targetFormat, customOpts) = if (HoodieSparkUtils.isSpark2) {
      ("org.apache.hudi.internal", Map())
    } else if (HoodieSparkUtils.isSpark3) {
      ("org.apache.hudi.spark3.internal", Map(
        HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key -> hoodieDF.schema.json
      ))
    } else {
      throw new HoodieException("Bulk insert using row writer is not supported with current Spark version."
        + " To use row writer please switch to spark 2 or spark 3")
    }

    hoodieDF.write.format(targetFormat)
      .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
      .options(opts ++ customOpts ++ optsOverrides)
      .mode(SaveMode.Append)
      .save()

    val syncHiveSuccess = metaSync(sqlContext.sparkSession, writeConfig, basePath, df.schema)
    (syncHiveSuccess, common.util.Option.ofNullable(instantTime))
  }

  def cleanup() : Unit = {
    Metrics.shutdown()
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

    if (operation != WriteOperationType.DELETE) {
      if (mode == SaveMode.ErrorIfExists && tableExists) {
        throw new HoodieException(s"hoodie table at $tablePath already exists.")
      } else if (mode == SaveMode.Overwrite && tableExists && operation != WriteOperationType.INSERT_OVERWRITE_TABLE) {
        // When user set operation as INSERT_OVERWRITE_TABLE,
        // overwrite will use INSERT_OVERWRITE_TABLE operator in doWriteOperation
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
    if (hiveSyncEnabled) {
      metaSyncEnabled = true
      syncClientToolClassSet += classOf[HiveSyncTool].getName
    }

    if (metaSyncEnabled) {
      val fs = basePath.getFileSystem(spark.sessionState.newHadoopConf())
      val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT);
      val properties = new TypedProperties()
      properties.putAll(hoodieConfig.getProps)
      properties.put(HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key, spark.sessionState.conf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD).toString)
      properties.put(HoodieSyncConfig.META_SYNC_SPARK_VERSION.key, SPARK_VERSION)
      properties.put(HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA.key, hoodieConfig.getBoolean(HoodieMetadataConfig.ENABLE))

      // Collect exceptions in list because we want all sync to run. Then we can throw
      val metaSyncExceptions = new ListBuffer[HoodieException]()
      syncClientToolClassSet.foreach(impl => {
        try {
          SyncUtilHelpers.runHoodieMetaSync(impl.trim, properties, fs.getConf, fs, basePath.toString, baseFileFormat)
        } catch {
          case e: HoodieException =>
            log.info("SyncTool class " + impl.trim + " failed with exception", e)
            metaSyncExceptions.add(e)
        }
      })
      if (metaSyncExceptions.nonEmpty) {
        throw SyncUtilHelpers.getExceptionFromList(metaSyncExceptions)
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
                                             client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                             tableConfig: HoodieTableConfig,
                                             jsc: JavaSparkContext,
                                             tableInstantInfo: TableInstantInfo,
                                             extraPreCommitFn: Option[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]]
                                            ): (Boolean, common.util.Option[java.lang.String], common.util.Option[java.lang.String]) = {
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
      if (!asyncCompactionEnabled && !asyncClusteringEnabled) {
        client.close()
      }
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

  private def isAsyncCompactionEnabled(client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                       tableConfig: HoodieTableConfig,
                                       parameters: Map[String, String], configuration: Configuration): Boolean = {
    log.info(s"Config.inlineCompactionEnabled ? ${client.getConfig.inlineCompactionEnabled}")
    if (asyncCompactionTriggerFnDefined && !client.getConfig.inlineCompactionEnabled
      && parameters.get(ASYNC_COMPACT_ENABLE.key).exists(r => r.toBoolean)) {
      tableConfig.getTableType == HoodieTableType.MERGE_ON_READ
    } else {
      false
    }
  }

  private def isAsyncClusteringEnabled(client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
                                       parameters: Map[String, String]): Boolean = {
    log.info(s"Config.asyncClusteringEnabled ? ${client.getConfig.isAsyncClusteringEnabled}")
    asyncClusteringTriggerFnDefined && client.getConfig.isAsyncClusteringEnabled
  }

  private def getHoodieTableConfig(sparkContext: SparkContext,
                                   tablePath: String,
                                   hoodieTableConfigOpt: Option[HoodieTableConfig]): HoodieTableConfig = {
    if (tableExists) {
      hoodieTableConfigOpt.getOrElse(
        HoodieTableMetaClient.builder().setConf(sparkContext.hadoopConfiguration).setBasePath(tablePath)
          .build().getTableConfig)
    } else {
      null
    }
  }

  private def mergeParamsAndGetHoodieConfig(optParams: Map[String, String],
                                            tableConfig: HoodieTableConfig, mode: SaveMode): (Map[String, String], HoodieConfig) = {
    val translatedOptions = DataSourceWriteOptions.translateSqlOptions(optParams)
    val mergedParams = mutable.Map.empty ++ HoodieWriterUtils.parametersWithWriteDefaults(translatedOptions)
    if (!mergedParams.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)
      && mergedParams.contains(KEYGENERATOR_CLASS_NAME.key)) {
      mergedParams(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) = mergedParams(KEYGENERATOR_CLASS_NAME.key)
    }
    if (null != tableConfig && mode != SaveMode.Overwrite) {
      tableConfig.getProps.foreach { case (key, value) =>
        mergedParams(key) = value
      }
    }

    // use preCombineField to fill in PAYLOAD_ORDERING_FIELD_PROP_KEY
    if (mergedParams.contains(PRECOMBINE_FIELD.key())) {
      mergedParams.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, mergedParams(PRECOMBINE_FIELD.key()))
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

  private def createHoodieRecordRdd(df: DataFrame,
                                    config: HoodieWriteConfig,
                                    parameters: Map[String, String],
                                    recordName: String,
                                    recordNameSpace: String,
                                    writerSchema: Schema,
                                    dataFileSchemaStr: String,
                                    operation: WriteOperationType) = {
    val shouldDropPartitionColumns = config.getBoolean(DataSourceWriteOptions.DROP_PARTITION_COLUMNS)
    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps))
    val recordType = config.getRecordMerger.getRecordType

    val shouldCombine = parameters(INSERT_DROP_DUPS.key()).toBoolean ||
      operation.equals(WriteOperationType.UPSERT) ||
      parameters.getOrElse(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key(),
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.defaultValue()).toBoolean

    log.debug(s"Use $recordType")
    recordType match {
      case HoodieRecord.HoodieRecordType.AVRO =>
        val avroRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, recordName, recordNameSpace,
          Some(writerSchema))

        avroRecords.mapPartitions(it => {
          val dataFileSchema = new Schema.Parser().parse(dataFileSchemaStr)
          val consistentLogicalTimestampEnabled = parameters.getOrElse(
            DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
            DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()).toBoolean

          it.map { avroRecord =>
            val processedRecord = if (shouldDropPartitionColumns) {
              HoodieAvroUtils.rewriteRecord(avroRecord, dataFileSchema)
            } else {
              avroRecord
            }
            val hoodieRecord = if (shouldCombine) {
              val orderingVal = HoodieAvroUtils.getNestedFieldVal(avroRecord, config.getString(PRECOMBINE_FIELD),
                false, consistentLogicalTimestampEnabled).asInstanceOf[Comparable[_]]
              DataSourceUtils.createHoodieRecord(processedRecord, orderingVal, keyGenerator.getKey(avroRecord),
                config.getString(PAYLOAD_CLASS_NAME))
            } else {
              DataSourceUtils.createHoodieRecord(processedRecord, keyGenerator.getKey(avroRecord),
                config.getString(PAYLOAD_CLASS_NAME))
            }
            hoodieRecord
          }
        }).toJavaRDD()
      case HoodieRecord.HoodieRecordType.SPARK =>
        // ut will use AvroKeyGenerator, so we need to cast it in spark record
        val sparkKeyGenerator = keyGenerator.asInstanceOf[SparkKeyGeneratorInterface]
        val dataFileSchema = new Schema.Parser().parse(dataFileSchemaStr)
        val dataFileStructType = HoodieInternalRowUtils.getCachedSchema(dataFileSchema)
        val writerStructType = HoodieInternalRowUtils.getCachedSchema(writerSchema)
        val sourceStructType = df.schema
        df.queryExecution.toRdd.mapPartitions { iter =>

          iter.map { internalRow =>
            val recordKey = sparkKeyGenerator.getRecordKey(internalRow, sourceStructType)
            val partitionPath = sparkKeyGenerator.getPartitionPath(internalRow, sourceStructType)
            val key = new HoodieKey(recordKey.toString, partitionPath.toString)

            if (shouldDropPartitionColumns) {
              val row = HoodieInternalRowUtils.getCachedUnsafeProjection(dataFileStructType, dataFileStructType)
                .apply(HoodieInternalRowUtils.rewriteRecord(internalRow, sourceStructType, dataFileStructType))
              new HoodieSparkRecord(key, row, dataFileStructType, false)
            } else {
              val row = HoodieInternalRowUtils.getCachedUnsafeProjection(writerStructType, writerStructType)
                .apply(HoodieInternalRowUtils.rewriteRecord(internalRow, sourceStructType, writerStructType))
              new HoodieSparkRecord(key, row, writerStructType, false)
            }
          }
        }.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[_]]]
    }
  }
}
