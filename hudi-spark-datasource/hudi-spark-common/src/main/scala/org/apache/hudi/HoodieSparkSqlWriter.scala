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
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieConfig, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{CommitUtils, StringUtils}
import org.apache.hudi.config.HoodieBootstrapConfig.{BASE_PATH, INDEX_CLASS_NAME}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.{BulkInsertInternalPartitionerWithRowsFactory, NonSortPartitionerWithRows}
import org.apache.hudi.hive.{HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.index.SparkHoodieIndexFactory
import org.apache.hudi.internal.DataSourceInternalWriterHelper
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{AvroSchemaEvolutionUtils, SerDeHelper}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.sync.common.util.SyncUtilHelpers
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SPARK_VERSION, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

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
            asyncClusteringTriggerFn: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]] => Unit] = Option.empty
           )
  : (Boolean, common.util.Option[String], common.util.Option[String], common.util.Option[String],
    SparkRDDWriteClient[HoodieRecordPayload[Nothing]], HoodieTableConfig) = {

    assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
    var tableConfig = getHoodieTableConfig(sparkContext, path, hoodieTableConfigOpt)
    validateTableConfig(sqlContext.sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite)

    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode)
    val originKeyGeneratorClassName = HoodieWriterUtils.getOriginKeyGenerator(parameters)
    val timestampKeyGeneratorConfigs = extractConfigsRelatedToTimestampBasedKeyGenerator(
      originKeyGeneratorClassName, parameters)
    //validate datasource and tableconfig keygen are the same
    validateKeyGeneratorConfig(originKeyGeneratorClassName, tableConfig);
    val databaseName = hoodieConfig.getStringOrDefault(HoodieTableConfig.DATABASE_NAME, "")
    val tblName = hoodieConfig.getStringOrThrow(HoodieWriteConfig.TBL_NAME,
      s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.").trim
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
      val partitionColumns = HoodieSparkUtils.getPartitionColumns(keyGenerator, toProperties(parameters))
      // Create the table if not present
      if (!tableExists) {
        val baseFileFormat = hoodieConfig.getStringOrDefault(HoodieTableConfig.BASE_FILE_FORMAT)
        val archiveLogFolder = hoodieConfig.getStringOrDefault(HoodieTableConfig.ARCHIVELOG_FOLDER)
        val recordKeyFields = hoodieConfig.getString(DataSourceWriteOptions.RECORDKEY_FIELD)
        val populateMetaFields = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)
        val useBaseFormatMetaFile = hoodieConfig.getBooleanOrDefault(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT);

        val tableMetaClient = HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(tableType)
          .setDatabaseName(databaseName)
          .setTableName(tblName)
          .setRecordKeyFields(recordKeyFields)
          .setBaseFileFormat(baseFileFormat)
          .setArchiveLogFolder(archiveLogFolder)
          .setPayloadClassName(hoodieConfig.getString(PAYLOAD_CLASS_NAME))
          // we can't fetch preCombine field from hoodieConfig object, since it falls back to "ts" as default value,
          // but we are interested in what user has set, hence fetching from optParams.
          .setPreCombineField(optParams.getOrElse(PRECOMBINE_FIELD.key(), null))
          .setPartitionFields(partitionColumns)
          .setPopulateMetaFields(populateMetaFields)
          .setRecordKeyFields(hoodieConfig.getString(RECORDKEY_FIELD))
          .setKeyGeneratorClassProp(originKeyGeneratorClassName)
          .set(timestampKeyGeneratorConfigs)
          .setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(hoodieConfig.getBoolean(URL_ENCODE_PARTITIONING))
          .setPartitionMetafileUseBaseFormat(useBaseFormatMetaFile)
          .setShouldDropPartitionColumns(hoodieConfig.getBooleanOrDefault(HoodieTableConfig.DROP_PARTITION_COLUMNS))
          .setCommitTimezone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getStringOrDefault(HoodieTableConfig.TIMELINE_TIMEZONE)))
          .initTable(sparkContext.hadoopConfiguration, path)
        tableConfig = tableMetaClient.getTableConfig
      }

      val commitActionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)
      val dropPartitionColumns = hoodieConfig.getBoolean(DataSourceWriteOptions.DROP_PARTITION_COLUMNS)

      // short-circuit if bulk_insert via row is enabled.
      // scalastyle:off
      if (hoodieConfig.getBoolean(ENABLE_ROW_WRITER) &&
        operation == WriteOperationType.BULK_INSERT) {
        val (success, commitTime: common.util.Option[String]) = bulkInsertAsRow(sqlContext, parameters, df, tblName,
          basePath, path, instantTime, partitionColumns)
        return (success, commitTime, common.util.Option.empty(), common.util.Option.empty(), hoodieWriteClient.orNull, tableConfig)
      }
      // scalastyle:on

      val reconcileSchema = parameters(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
      val (writeResult, writeClient: SparkRDDWriteClient[HoodieRecordPayload[Nothing]]) =
        operation match {
          case WriteOperationType.DELETE => {
            val genericRecords = registerKryoClassesAndGetGenericRecords(tblName, sparkContext, df, reconcileSchema)
            // Convert to RDD[HoodieKey]
            val hoodieKeysToDelete = genericRecords.map(gr => keyGenerator.getKey(gr)).toJavaRDD()

            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            // Create a HoodieWriteClient & issue the delete.
            val internalSchemaOpt = getLatestTableInternalSchema(fs, basePath, sparkContext)
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
          }
          case WriteOperationType.DELETE_PARTITION => {
            if (!tableExists) {
              throw new HoodieException(s"hoodie table at $basePath does not exist")
            }

            // Get list of partitions to delete
            val partitionsToDelete = if (parameters.containsKey(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key())) {
              val partitionColsToDelete = parameters(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key()).split(",")
              java.util.Arrays.asList(partitionColsToDelete: _*)
            } else {
              val genericRecords = registerKryoClassesAndGetGenericRecords(tblName, sparkContext, df, reconcileSchema)
              genericRecords.map(gr => keyGenerator.getKey(gr).getPartitionPath).toJavaRDD().distinct().collect()
            }
            // Create a HoodieWriteClient & issue the delete.
            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc,
              null, path, tblName,
              mapAsJavaMap(parameters - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key)))
              .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
            // Issue delete partitions
            client.startCommitWithTime(instantTime, commitActionType)
            val writeStatuses = DataSourceUtils.doDeletePartitionsOperation(client, partitionsToDelete, instantTime)
            (writeStatuses, client)
          }
          case _ => { // any other operation
            // register classes & schemas
            val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
            sparkContext.getConf.registerKryoClasses(
              Array(classOf[org.apache.avro.generic.GenericData],
                classOf[org.apache.avro.Schema]))
            var schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
            val lastestSchema = getLatestTableSchema(fs, basePath, sparkContext, schema)
            var internalSchemaOpt = getLatestTableInternalSchema(fs, basePath, sparkContext)
            if (reconcileSchema && parameters(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key()).toBoolean && internalSchemaOpt.isEmpty) {
              // force apply full schema evolution.
              internalSchemaOpt = Some(AvroInternalSchemaConverter.convert(schema))
            }
            if (reconcileSchema) {
              schema = lastestSchema
            }
            if (internalSchemaOpt.isDefined) {
              // Apply schema evolution.
              val mergedSparkSchema = if (!reconcileSchema) {
                AvroConversionUtils.convertAvroSchemaToStructType(AvroSchemaEvolutionUtils.canonicalizeColumnNullability(schema, lastestSchema))
              } else {
                // Auto merge write schema and read schema.
                val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(schema, internalSchemaOpt.get)
                AvroConversionUtils.convertAvroSchemaToStructType(AvroInternalSchemaConverter.convert(mergedInternalSchema, lastestSchema.getName))
              }
              schema = AvroConversionUtils.convertStructTypeToAvroSchema(mergedSparkSchema, structName, nameSpace)
            }

            if (reconcileSchema && internalSchemaOpt.isEmpty) {
              schema = lastestSchema
            }
            validateSchemaForHoodieIsDeleted(schema)
            sparkContext.getConf.registerAvroSchemas(schema)
            log.info(s"Registered avro schema : ${schema.toString(true)}")

            // Convert to RDD[HoodieRecord]
            val genericRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, structName, nameSpace, reconcileSchema,
              org.apache.hudi.common.util.Option.of(schema))
            val shouldCombine = parameters(INSERT_DROP_DUPS.key()).toBoolean ||
              operation.equals(WriteOperationType.UPSERT) ||
              parameters.getOrElse(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key(),
                HoodieWriteConfig.COMBINE_BEFORE_INSERT.defaultValue()).toBoolean
            val hoodieAllIncomingRecords = genericRecords.map(gr => {
              val processedRecord = getProcessedRecord(partitionColumns, gr, dropPartitionColumns)
              val hoodieRecord = if (shouldCombine) {
                val orderingVal = HoodieAvroUtils.getNestedFieldVal(gr, hoodieConfig.getString(PRECOMBINE_FIELD), false, parameters.getOrElse(
                  DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                  DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()).toBoolean)
                  .asInstanceOf[Comparable[_]]
                DataSourceUtils.createHoodieRecord(processedRecord,
                  orderingVal,
                  keyGenerator.getKey(gr),
                  hoodieConfig.getString(PAYLOAD_CLASS_NAME))
              } else {
                DataSourceUtils.createHoodieRecord(processedRecord, keyGenerator.getKey(gr), hoodieConfig.getString(PAYLOAD_CLASS_NAME))
              }
              hoodieRecord
            }).toJavaRDD()

            val writeSchema = if (dropPartitionColumns) generateSchemaWithoutPartitionColumns(partitionColumns, schema) else schema
            // Create a HoodieWriteClient & issue the write.

            val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc, writeSchema.toString, path,
              tblName, mapAsJavaMap(addSchemaEvolutionParameters(parameters, internalSchemaOpt) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key)
            )).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

            if (isAsyncCompactionEnabled(client, tableConfig, parameters, jsc.hadoopConfiguration())) {
              asyncCompactionTriggerFn.get.apply(client)
            }

            if (isAsyncClusteringEnabled(client, parameters)) {
              asyncClusteringTriggerFn.get.apply(client)
            }

            val hoodieRecords =
              if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
                DataSourceUtils.dropDuplicates(jsc, hoodieAllIncomingRecords, mapAsJavaMap(parameters))
              } else {
                hoodieAllIncomingRecords
              }
            client.startCommitWithTime(instantTime, commitActionType)
            val writeResult = DataSourceUtils.doWriteOperation(client, hoodieRecords, instantTime, operation)
            (writeResult, client)
          }
        }

      // Check for errors and commit the write.
      val (writeSuccessful, compactionInstant, clusteringInstant) =
        commitAndPerformPostOperations(sqlContext.sparkSession, df.schema,
          writeResult, parameters, writeClient, tableConfig, jsc,
          TableInstantInfo(basePath, instantTime, commitActionType, operation))

      (writeSuccessful, common.util.Option.ofNullable(instantTime), compactionInstant, clusteringInstant, writeClient, tableConfig)
    }
  }

  def generateSchemaWithoutPartitionColumns(partitionParam: String, schema: Schema): Schema = {
    val fieldsToRemove = new java.util.ArrayList[String]()
    partitionParam.split(",").map(partitionField => partitionField.trim)
      .filter(s => !s.isEmpty).map(field => fieldsToRemove.add(field))
    HoodieAvroUtils.removeFields(schema, fieldsToRemove)
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

  def addSchemaEvolutionParameters(parameters: Map[String, String], internalSchemaOpt: Option[InternalSchema]): Map[String, String] = {
    val schemaEvolutionEnable = if (internalSchemaOpt.isDefined) "true" else "false"
    parameters ++ Map(HoodieWriteConfig.INTERNAL_SCHEMA_STRING.key() -> SerDeHelper.toJson(internalSchemaOpt.getOrElse(null)),
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key() -> schemaEvolutionEnable)
  }

  /**
    * get latest internalSchema from table
    *
    * @param fs           instance of FileSystem.
    * @param basePath     base path.
    * @param sparkContext instance of spark context.
    * @param schema       incoming record's schema.
    * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
    */
  def getLatestTableInternalSchema(fs: FileSystem, basePath: Path, sparkContext: SparkContext): Option[InternalSchema] = {
    try {
      if (FSUtils.isTableExists(basePath.toString, fs)) {
        val tableMetaClient = HoodieTableMetaClient.builder.setConf(sparkContext.hadoopConfiguration).setBasePath(basePath.toString).build()
        val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
        val internalSchemaOpt = tableSchemaResolver.getTableInternalSchemaFromCommitMetadata
        if (internalSchemaOpt.isPresent) Some(internalSchemaOpt.get()) else None
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Checks if schema needs upgrade (if incoming record's write schema is old while table schema got evolved).
   *
   * @param fs           instance of FileSystem.
   * @param basePath     base path.
   * @param sparkContext instance of spark context.
   * @param schema       incoming record's schema.
   * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
   */
  def getLatestTableSchema(fs: FileSystem, basePath: Path, sparkContext: SparkContext, schema: Schema): Schema = {
    var latestSchema: Schema = schema
    if (FSUtils.isTableExists(basePath.toString, fs)) {
      val tableMetaClient = HoodieTableMetaClient.builder.setConf(sparkContext.hadoopConfiguration).setBasePath(basePath.toString).build()
      val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
      latestSchema = tableSchemaResolver.getLatestSchema(schema, false, null)
    }
    latestSchema
  }

  def registerKryoClassesAndGetGenericRecords(tblName: String, sparkContext: SparkContext, df: Dataset[Row],
                                              reconcileSchema: Boolean): RDD[GenericRecord] = {
    val structName = s"${tblName}_record"
    val nameSpace = s"hoodie.${tblName}"
    sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    HoodieSparkUtils.createRdd(df, structName, nameSpace, reconcileSchema)
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
        val keyGenProp = hoodieConfig.getString(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)
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
          .setPopulateMetaFields(populateMetaFields)
          .setKeyGeneratorClassProp(keyGenProp)
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
    if (schema.getField(HoodieRecord.HOODIE_IS_DELETED) != null &&
      AvroConversionUtils.resolveAvroTypeNullability(schema.getField(HoodieRecord.HOODIE_IS_DELETED).schema())._2.getType != Schema.Type.BOOLEAN) {
      throw new HoodieException(HoodieRecord.HOODIE_IS_DELETED + " has to be BOOLEAN type. Passed in dataframe's schema has type "
        + schema.getField(HoodieRecord.HOODIE_IS_DELETED).schema().getType)
    }
  }

  def bulkInsertAsRow(sqlContext: SQLContext,
                      parameters: Map[String, String],
                      df: DataFrame,
                      tblName: String,
                      basePath: Path,
                      path: String,
                      instantTime: String,
                      partitionColumns: String): (Boolean, common.util.Option[String]) = {
    val sparkContext = sqlContext.sparkContext
    val populateMetaFields = java.lang.Boolean.parseBoolean((parameters.getOrElse(HoodieTableConfig.POPULATE_META_FIELDS.key(),
      String.valueOf(HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))))
    val dropPartitionColumns = parameters.get(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key()).map(_.toBoolean)
      .getOrElse(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.defaultValue())
    // register classes & schemas
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
    sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    var schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
    if (dropPartitionColumns) {
      schema = generateSchemaWithoutPartitionColumns(partitionColumns, schema)
    }
    validateSchemaForHoodieIsDeleted(schema)
    sparkContext.getConf.registerAvroSchemas(schema)
    log.info(s"Registered avro schema : ${schema.toString(true)}")
    if (parameters(INSERT_DROP_DUPS.key).toBoolean) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet")
    }
    val params: mutable.Map[String, String] = collection.mutable.Map(parameters.toSeq: _*)
    params(HoodieWriteConfig.AVRO_SCHEMA_STRING.key) = schema.toString
    val writeConfig = DataSourceUtils.createHoodieConfig(schema.toString, path, tblName, mapAsJavaMap(params))
    val bulkInsertPartitionerRows: BulkInsertPartitioner[Dataset[Row]] = if (populateMetaFields) {
      val userDefinedBulkInsertPartitionerOpt = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(writeConfig)
      if (userDefinedBulkInsertPartitionerOpt.isPresent) {
        userDefinedBulkInsertPartitionerOpt.get
      }
      else {
        BulkInsertInternalPartitionerWithRowsFactory.get(writeConfig.getBulkInsertSortMode)
      }
    } else {
      // Sort modes are not yet supported when meta fields are disabled
      new NonSortPartitionerWithRows()
    }
    val arePartitionRecordsSorted = bulkInsertPartitionerRows.arePartitionRecordsSorted()
    params(HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED) = arePartitionRecordsSorted.toString
    val isGlobalIndex = if (populateMetaFields) {
      SparkHoodieIndexFactory.isGlobalIndex(writeConfig)
    } else {
      false
    }
    val hoodieDF = if (populateMetaFields) {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, writeConfig, df, structName, nameSpace,
        bulkInsertPartitionerRows, isGlobalIndex, dropPartitionColumns)
    } else {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsertWithoutMetaFields(df)
    }
    if (HoodieSparkUtils.isSpark2) {
      hoodieDF.write.format("org.apache.hudi.internal")
        .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
        .options(params)
        .mode(SaveMode.Append)
        .save()
    } else if (HoodieSparkUtils.isSpark3) {
      hoodieDF.write.format("org.apache.hudi.spark3.internal")
        .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
        .option(HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key, hoodieDF.schema.toDDL)
        .options(params)
        .mode(SaveMode.Append)
        .save()
    } else {
      throw new HoodieException("Bulk insert using row writer is not supported with current Spark version."
        + " To use row writer please switch to spark 2 or spark 3")
    }
    val syncHiveSuccess = metaSync(sqlContext.sparkSession, writeConfig, basePath, df.schema)
    (syncHiveSuccess, common.util.Option.ofNullable(instantTime))
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

      syncClientToolClassSet.foreach(impl => {
        SyncUtilHelpers.runHoodieMetaSync(impl.trim, properties, fs.getConf, fs, basePath.toString, baseFileFormat)
      })
    }
    true
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
                                             tableInstantInfo: TableInstantInfo
                                            ): (Boolean, common.util.Option[java.lang.String], common.util.Option[java.lang.String]) = {
    if (writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).count() == 0) {
      log.info("Proceeding to commit the write.")
      val metaMap = parameters.filter(kv =>
        kv._1.startsWith(parameters(COMMIT_METADATA_KEYPREFIX.key)))
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          common.util.Option.of(new java.util.HashMap[String, String](mapAsJavaMap(metaMap))),
          tableInstantInfo.commitActionType,
          writeResult.getPartitionToReplaceFileIds)

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
    if (keyGenerator.equals(classOf[TimestampBasedKeyGenerator].getCanonicalName) ||
        keyGenerator.equals(classOf[TimestampBasedAvroKeyGenerator].getCanonicalName)) {
      params.filterKeys(HoodieTableConfig.PERSISTED_CONFIG_LIST.contains)
    } else {
      Map.empty
    }
  }
}
