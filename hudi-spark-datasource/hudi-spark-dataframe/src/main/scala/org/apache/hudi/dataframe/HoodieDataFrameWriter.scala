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

package org.apache.hudi.dataframe

import org.apache.hudi.{HoodieSchemaConversionUtils, HoodieSparkUtils}
import org.apache.hudi.AvroConversionUtils.getAvroRecordNameAndNamespace
import org.apache.hudi.client.{SparkRDDWriteClient, WriteStatus}
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{CommitUtils, ConfigUtils, HoodieDataUtils, Option => HOption, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.{HoodieIndex, SparkHoodieIndexFactory}
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.metadata.{HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.table.HoodieTable

import org.apache.spark.Partitioner
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, concat, hash, lit, row_number, udf, when}
import org.apache.spark.sql.hudi.execution.HoodieRoutingShuffle
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * DataFrame-native write path for Hudi on Spark (PoC).
 *
 * <p>Takes the input [[DataFrame]] end to end as Dataset/InternalRow operations: key generation
 * and meta-column stamping as a projection, index tagging as a join against the latest base-file
 * meta columns, routing as a repartition on a routing-key column, and the record write as
 * per-file-group tasks driving the row-native write handles ({@code HoodieRowCreateHandle} for
 * new files, {@code FileGroupReaderBasedMergeHandle} for merges). The commit goes through the
 * regular {@code SparkRDDWriteClient#commit} so timeline, metadata table, and table services
 * behave exactly as on the RDD path.
 *
 * <p>Scope: insert and upsert on COW tables with the simple index tagging strategy.
 */
object HoodieDataFrameWriter extends Logging {

  val DATAFRAME_WRITE_PATH_ENABLE = "hoodie.datasource.write.dataframe.path.enable"
  val INSERT_BUCKETS_PER_PARTITION = "hoodie.datasource.write.dataframe.insert.buckets"
  val WRITE_TASKS = "hoodie.datasource.write.dataframe.write.tasks"
  val PROFILER = "hoodie.datasource.write.dataframe.profiler"
  val PROFILER_METADATA = "metadata"
  val PROFILER_SHUFFLE_STATS = "shuffle_stats"
  val SHUFFLE_UPDATE_REDUCERS = "hoodie.datasource.write.dataframe.shuffle.update.reducers"
  val SHUFFLE_INSERT_REDUCERS = "hoodie.datasource.write.dataframe.shuffle.insert.reducers"
  val SHUFFLE_BYTES_METADATA_KEY = "hoodie.dataframe.shuffle.bytes"
  private val DEFAULT_SHUFFLE_SIZE_RATIO = 3.0d

  val OPERATION_KEY = "hoodie.datasource.write.operation"
  val RECORD_KEY_FIELD = "hoodie.datasource.write.recordkey.field"
  val PARTITION_PATH_FIELD = "hoodie.datasource.write.partitionpath.field"

  private val TAGGED_FILE_ID_COL = "_hoodie_tagged_file_id"
  private val LOCATED_PARTITION_COL = "_hoodie_located_partition"
  private val ROUTING_BUCKET_COL = "_hoodie_routing_bucket"
  val ROW_OP_COL = "_hoodie_row_op"
  val ROW_OP_UPSERT = "U"
  val ROW_OP_DELETE = "D"
  private val DELETE_MARKER_COL = "_hoodie_is_deleted"
  val UPDATE_BUCKET_PREFIX = "u:"
  val MERGE_BUCKET_PREFIX = "s:"
  val INSERT_BUCKET_PREFIX = "i:"

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            params: Map[String, String],
            df: DataFrame): Boolean = {
    val spark = sqlContext.sparkSession
    val basePath = params.getOrElse("path",
      throw new HoodieException("'path' must be set for the DataFrame write path"))
    val operation = WriteOperationType.fromValue(
      params.getOrElse(OPERATION_KEY, WriteOperationType.UPSERT.value()))
    if (operation != WriteOperationType.INSERT && operation != WriteOperationType.UPSERT) {
      throw new HoodieException(s"Operation $operation is not supported by the DataFrame write path yet")
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val storageConf = HadoopFSUtils.getStorageConfWithCopy(hadoopConf)
    // Meta columns of a Hudi-sourced input are regenerated by the prepare stage.
    val input = df.drop(HoodieRecord.HOODIE_META_COLUMNS.asScala: _*)
    val tableConfig = loadOrCreateTable(basePath, params, storageConf).getTableConfig
    val tableName = Option(tableConfig.getTableName).filter(!StringUtils.isNullOrEmpty(_))
      .orElse(params.get(HoodieWriteConfig.TBL_NAME.key()))
      .getOrElse(throw new HoodieException(s"'${HoodieWriteConfig.TBL_NAME.key()}' must be set"))
    val writeConfig = buildWriteConfig(basePath, tableName, operation, params, tableConfig, input)
    validateTableConfig(tableConfig, operation, writeConfig)
    val engineContext = new org.apache.hudi.client.common.HoodieSparkEngineContext(
      new JavaSparkContext(spark.sparkContext))
    val client = new SparkRDDWriteClient[Any](engineContext, writeConfig)
    try {
      val instantTime = client.startCommit()
      val table = client.initTable(operation, HOption.of(instantTime))
      if (operation == WriteOperationType.UPSERT) {
        table.validateUpsertSchema()
      } else {
        table.validateInsertSchema()
      }
      client.preWrite(instantTime, operation, table.getMetaClient)

      // Stage 1 + 2: key generation, meta columns, and upfront precombine as pure projections.
      val prepared = prepareInput(input, operation, writeConfig, table.getMetaClient.getTableConfig)

      // Stage 3: index tagging (record-level-index point lookup or simple-index join).
      val tagged = tagRecords(spark, prepared, operation, writeConfig, table)

      // Stage 4: routing on a bucket column; updates pin to their file group, inserts hash
      // across small-file padding slots and new-file buckets planned from table metadata.
      val mergeOnRead = table.getMetaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ
      val insertPlan = MetadataSizingProfiler.plan(table, writeConfig,
        params.get(INSERT_BUCKETS_PER_PARTITION).map(_.toInt).getOrElse(1),
        mergeOnRead,
        if (mergeOnRead) MERGE_BUCKET_PREFIX else UPDATE_BUCKET_PREFIX)
      val withBucket = withRoutingBucket(tagged, insertPlan)
      val bucketOrdinal = withBucket.schema.fieldIndex(ROUTING_BUCKET_COL)

      val actionType = CommitUtils.getCommitActionType(operation, table.getMetaClient.getTableConfig.getTableType)
      table.getActiveTimeline.transitionRequestedToInflight(
        table.getMetaClient.createNewInstant(HoodieInstant.State.REQUESTED, actionType, instantTime),
        HOption.empty())

      // Stage 5: per-bucket write tasks driving the row-native handles. The reader context
      // factory is created on the driver (the table's engine context is not usable on executors)
      // and shipped with the task.
      val writeSchemaLength = prepared.schema.length
      val sparkTable = table.asInstanceOf[HoodieTable[InternalRow, _, _, _]]
      val readerContextFactory = sparkTable.getReaderContextFactoryForWrite
      val task = new DataFrameWriteTask(
        writeConfig, sparkTable, readerContextFactory, instantTime, withBucket.schema,
        writeSchemaLength, bucketOrdinal, withBucket.schema.fieldIndex(ROW_OP_COL),
        mergeOnRead)

      val profilerMode = params.getOrElse(PROFILER, PROFILER_METADATA)
      val (statusRdd, extraMetadata) = if (profilerMode == PROFILER_SHUFFLE_STATS) {
        // The routing shuffle doubles as the profiling pass: only its map stage runs first, the
        // measured block sizes plan the write tasks, and the reduce side reads the shuffle files.
        val pairs = HoodieSparkUtils.injectSQLConf(
          withBucket.queryExecution.toRdd.map(row => (row.getString(bucketOrdinal), row.copy())),
          SQLConf.get)
        val sizeRatio = readShuffleSizeRatio(table.getMetaClient)
        val binSize = math.max(1L, (writeConfig.getParquetMaxFileSize * sizeRatio).toLong)
        val shuffle = HoodieRoutingShuffle.execute(pairs,
          params.getOrElse(SHUFFLE_UPDATE_REDUCERS, "50").toInt,
          params.getOrElse(SHUFFLE_INSERT_REDUCERS, "50").toInt,
          INSERT_BUCKET_PREFIX, binSize)
        (HoodieSparkUtils.injectSQLConf(
          shuffle.rows.mapPartitions(iter => task.execute(iter.map(_._2))), SQLConf.get),
          Map(SHUFFLE_BYTES_METADATA_KEY -> shuffle.totalShuffleBytes.toString))
      } else {
        val numWriteTasks = params.get(WRITE_TASKS).map(_.toInt)
          .getOrElse(spark.sparkContext.defaultParallelism)
        val routed = withBucket
          .repartition(numWriteTasks, col(ROUTING_BUCKET_COL))
          .sortWithinPartitions(col(ROUTING_BUCKET_COL))
        (HoodieSparkUtils.injectSQLConf(
          routed.queryExecution.toRdd.mapPartitions(iter => task.execute(iter)), SQLConf.get),
          Map.empty[String, String])
      }

      val javaRdd = new org.apache.spark.api.java.JavaRDD[WriteStatus](statusRdd)
      HoodieJavaRDD.of(javaRdd).persist(
        writeConfig.getString(HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE),
        engineContext, HoodieDataCacheKey.of(basePath, instantTime))

      val extraMetadataOpt = if (extraMetadata.isEmpty) {
        HOption.empty[java.util.Map[String, String]]()
      } else {
        val metadataMap: java.util.Map[String, String] = new java.util.HashMap[String, String](extraMetadata.asJava)
        HOption.of(metadataMap)
      }
      client.commit(instantTime, javaRdd, extraMetadataOpt, actionType,
        java.util.Collections.emptyMap[String, java.util.List[String]](), HOption.empty())
    } finally {
      client.close()
    }
    true
  }

  private def loadOrCreateTable(basePath: String,
                                params: Map[String, String],
                                storageConf: org.apache.hudi.storage.StorageConfiguration[_]): HoodieTableMetaClient = {
    val path = new org.apache.hadoop.fs.Path(basePath)
    val fs = path.getFileSystem(storageConf.unwrapAs(classOf[org.apache.hadoop.conf.Configuration]))
    val tableExists = fs.exists(new org.apache.hadoop.fs.Path(path, HoodieTableMetaClient.METAFOLDER_NAME))
    if (tableExists) {
      HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build()
    } else {
      val tableName = params.getOrElse(HoodieWriteConfig.TBL_NAME.key(),
        throw new HoodieException(s"'${HoodieWriteConfig.TBL_NAME.key()}' must be set to create a table"))
      val builder = HoodieTableMetaClient.newTableBuilder()
        .setTableType(params.getOrElse("hoodie.datasource.write.table.type",
          HoodieTableType.COPY_ON_WRITE.name()))
        .setTableName(tableName)
        .setRecordKeyFields(params.getOrElse(RECORD_KEY_FIELD, null))
        .setPartitionFields(params.getOrElse(PARTITION_PATH_FIELD, null))
        .setOrderingFields(ConfigUtils.getOrderingFieldsStrDuringWrite(params.asJava))
        .setKeyGeneratorClassProp(resolveKeyGeneratorClass(params))
      params.get(HoodieWriteConfig.WRITE_TABLE_VERSION.key()).foreach(v => builder.setTableVersion(v.toInt))
      builder.initTable(storageConf, basePath)
    }
  }

  private val SUPPORTED_INDEX_TYPES: Set[HoodieIndex.IndexType] = Set(
    HoodieIndex.IndexType.SIMPLE,
    HoodieIndex.IndexType.GLOBAL_SIMPLE,
    HoodieIndex.IndexType.BLOOM,
    HoodieIndex.IndexType.RECORD_INDEX,
    HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX,
    HoodieIndex.IndexType.RECORD_LEVEL_INDEX)

  /**
   * Rejects table configurations outside the current scope up front, so unsupported tables fail
   * cleanly instead of writing inconsistent data.
   */
  private def validateTableConfig(tableConfig: HoodieTableConfig,
                                  operation: WriteOperationType,
                                  writeConfig: HoodieWriteConfig): Unit = {
    val indexType = writeConfig.getIndexType
    if (!SUPPORTED_INDEX_TYPES.contains(indexType)) {
      throw new HoodieException(s"Index type $indexType is not supported by the DataFrame write path yet")
    }
    if (!tableConfig.populateMetaFields()) {
      throw new HoodieException("The DataFrame write path requires populated meta fields")
    }
    if (tableConfig.shouldDropPartitionColumns()) {
      throw new HoodieException("The DataFrame write path does not support dropping partition columns yet")
    }
    if (tableConfig.getRecordMergeMode == RecordMergeMode.CUSTOM) {
      throw new HoodieException("The DataFrame write path does not support custom merge modes yet")
    }
  }

  private def resolveKeyGeneratorClass(params: Map[String, String]): String = {
    params.getOrElse(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), {
      val recordKeyFields = params.getOrElse(RECORD_KEY_FIELD, "")
      val partitionFields = params.getOrElse(PARTITION_PATH_FIELD, "")
      if (StringUtils.isNullOrEmpty(partitionFields)) {
        classOf[NonpartitionedKeyGenerator].getName
      } else if (recordKeyFields.contains(",") || partitionFields.contains(",")) {
        classOf[ComplexKeyGenerator].getName
      } else {
        classOf[SimpleKeyGenerator].getName
      }
    })
  }

  private def buildWriteConfig(basePath: String,
                               tableName: String,
                               operation: WriteOperationType,
                               params: Map[String, String],
                               tableConfig: HoodieTableConfig,
                               input: DataFrame): HoodieWriteConfig = {
    val (name, namespace) = getAvroRecordNameAndNamespace(tableName)
    val schemaStr = HoodieSchemaConversionUtils
      .convertStructTypeToHoodieSchema(input.schema, name, namespace).toString
    val props = new TypedProperties()
    params.foreach { case (k, v) => props.setProperty(k, v) }
    // Table-config values fill in options the write did not carry, matching the legacy writer's
    // params backfill, so repeat writes only need the path and the enable flag.
    def backfill(key: String, value: String): Unit = {
      if (!props.containsKey(key) && !StringUtils.isNullOrEmpty(value)) {
        props.setProperty(key, value)
      }
    }
    backfill(RECORD_KEY_FIELD, tableConfig.getRecordKeyFieldProp)
    backfill(PARTITION_PATH_FIELD, tableConfig.getPartitionFieldProp)
    backfill("hoodie.datasource.write.hive_style_partitioning", tableConfig.getHiveStylePartitioningEnable)
    backfill("hoodie.datasource.write.partitionpath.urlencode", tableConfig.getUrlEncodePartitioning)
    backfill("hoodie.datasource.write.precombine.field",
      ConfigUtils.getOrderingFieldsStrDuringWrite(tableConfig.getProps))
    // The table's own key generator wins over anything inferred from the write options.
    val keyGenClass = Option(tableConfig.getKeyGeneratorClassName).filter(StringUtils.nonEmpty)
      .getOrElse(resolveKeyGeneratorClass(params))
    props.setProperty(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGenClass)
    // The engine-native record merger keeps the merge path on InternalRow end to end; an explicit
    // user-provided merger is left alone.
    if (!props.containsKey(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY)) {
      props.setProperty(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY,
        "org.apache.hudi.DefaultSparkRecordMerger")
    }
    // Avro log blocks cannot be read back as engine-native records; parquet blocks keep MOR
    // logs on InternalRow when the write version predates native logs.
    if (tableConfig.getTableType == HoodieTableType.MERGE_ON_READ
      && !props.containsKey("hoodie.logfile.data.block.format")) {
      props.setProperty("hoodie.logfile.data.block.format", "parquet")
    }
    HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .forTable(tableName)
      .withSchema(schemaStr)
      .withProps(props)
      .build()
  }

  /**
   * The prepare stage as projections: meta columns with the record key and partition path
   * computed by keygen expressions, then the upfront precombine as a window dedup keyed by the
   * ordering fields (or an arbitrary winner under commit-time ordering).
   */
  private def prepareInput(input: DataFrame,
                           operation: WriteOperationType,
                           writeConfig: HoodieWriteConfig,
                           tableConfig: HoodieTableConfig): DataFrame = {
    val props = writeConfig.getProps
    val kind = KeyGenExpressions.kindOf(
      writeConfig.getStringOrThrow(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME, "key generator is required"))
    val recordKeyFields = props.getString(RECORD_KEY_FIELD, "").split(",").map(_.trim).filter(_.nonEmpty).toSeq
    if (recordKeyFields.isEmpty) {
      throw new HoodieException("A record key field is required on the DataFrame write path")
    }
    val partitionFields = props.getString(PARTITION_PATH_FIELD, "").split(",").map(_.trim).filter(_.nonEmpty).toSeq
    val hiveStyle = props.getString("hoodie.datasource.write.hive_style_partitioning", "false").toBoolean
    val urlEncode = props.getString("hoodie.datasource.write.partitionpath.urlencode", "false").toBoolean

    val withMeta = input.select(
      (Seq(
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
        KeyGenExpressions.recordKeyExpr(kind, recordKeyFields).as(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        KeyGenExpressions.partitionPathExpr(kind, partitionFields, props, hiveStyle, urlEncode)
          .as(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.FILENAME_METADATA_FIELD))
        ++ input.columns.map(input(_))): _*)

    val shouldCombine = if (operation == WriteOperationType.UPSERT) {
      writeConfig.shouldCombineBeforeUpsert()
    } else {
      writeConfig.shouldCombineBeforeInsert()
    }
    if (!shouldCombine) {
      withMeta
    } else {
      val dedupKeys = if (SparkHoodieIndexFactory.isGlobalIndex(writeConfig)) {
        Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
      } else {
        Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD), col(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
      }
      val orderingFields = tableConfig.getOrderingFields.asScala.toSeq
      val ordering = if (orderingFields.nonEmpty) {
        orderingFields.map(f => col(f).desc)
      } else {
        // Commit-time ordering: no in-batch order is defined, keep an arbitrary single row per key.
        Seq(lit(1).asc)
      }
      val window = Window.partitionBy(dedupKeys: _*).orderBy(ordering: _*)
      withMeta.withColumn("_hoodie_dedup_rank", row_number().over(window))
        .filter(col("_hoodie_dedup_rank") === 1)
        .drop("_hoodie_dedup_rank")
    }
  }

  /**
   * Tags incoming rows with their target file group: the record-level-index strategy when the
   * index is configured and its metadata partition exists, else the simple-index-style join
   * against the latest base files' meta columns. Inserts come back with a null file id.
   */
  private def tagRecords(spark: org.apache.spark.sql.SparkSession,
                         prepared: Dataset[Row],
                         operation: WriteOperationType,
                         writeConfig: HoodieWriteConfig,
                         table: HoodieTable[_, _, _, _]): Dataset[Row] = {
    val indexType = writeConfig.getIndexType
    val recordIndexConfigured = indexType == HoodieIndex.IndexType.RECORD_INDEX ||
      indexType == HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX
    val recordIndexReady = recordIndexConfigured &&
      table.getMetaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)
    val globalIndex = recordIndexConfigured || indexType == HoodieIndex.IndexType.GLOBAL_SIMPLE
    val deleteMarker: Column = if (prepared.columns.contains(DELETE_MARKER_COL)) {
      coalesce(col(DELETE_MARKER_COL).cast("boolean"), lit(false))
    } else {
      lit(false)
    }

    if (operation != WriteOperationType.UPSERT) {
      // Inserts never tag, and a delete-marker row has nothing to delete.
      prepared.filter(!deleteMarker)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .withColumn(ROW_OP_COL, lit(ROW_OP_UPSERT))
    } else {
      val joined = if (recordIndexReady) {
        log.info("Tagging incoming records via the record-level index")
        tagViaRecordIndex(spark, prepared, table)
      } else {
        // A configured-but-uninitialized record index keeps global semantics through the
        // base-file join, mirroring the legacy fallback to the global simple index.
        tagViaSimpleJoin(spark, prepared, table, globalIndex)
      }
      val updatePartitionPath = globalIndex && (if (recordIndexConfigured) {
        writeConfig.getRecordIndexUpdatePartitionPath
      } else {
        writeConfig.getGlobalSimpleIndexUpdatePartitionPath
      })
      resolveTagged(joined, globalIndex, updatePartitionPath, deleteMarker)
    }
  }

  /**
   * Applies global-index semantics and the row-operation column on the tagged frame: no-op
   * deletes are dropped; with update-partition-path off, located rows route to their original
   * partition; with it on, a key that moved partitions fans out into a delete against the old
   * file group plus an insert into the new partition.
   */
  private def resolveTagged(joined: Dataset[Row],
                            globalIndex: Boolean,
                            updatePartitionPath: Boolean,
                            deleteMarker: Column): Dataset[Row] = {
    val base = joined
      .withColumn(ROW_OP_COL, when(deleteMarker, lit(ROW_OP_DELETE)).otherwise(lit(ROW_OP_UPSERT)))
      .filter(col(ROW_OP_COL) === ROW_OP_UPSERT || col(TAGGED_FILE_ID_COL).isNotNull)
    if (!globalIndex) {
      base
    } else if (!updatePartitionPath) {
      base.withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
          coalesce(col(LOCATED_PARTITION_COL), col(HoodieRecord.PARTITION_PATH_METADATA_FIELD)))
        .drop(LOCATED_PARTITION_COL)
    } else {
      val moved = col(TAGGED_FILE_ID_COL).isNotNull && col(LOCATED_PARTITION_COL).isNotNull &&
        col(LOCATED_PARTITION_COL) =!= col(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
      val staying = base.filter(!moved).drop(LOCATED_PARTITION_COL)
      val oldCopyDeletes = base.filter(moved)
        .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, col(LOCATED_PARTITION_COL))
        .withColumn(ROW_OP_COL, lit(ROW_OP_DELETE))
        .drop(LOCATED_PARTITION_COL)
      val newPartitionInserts = base.filter(moved && col(ROW_OP_COL) === ROW_OP_UPSERT)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .drop(LOCATED_PARTITION_COL)
      staying.unionByName(oldCopyDeletes).unionByName(newPartitionInserts)
    }
  }

  /**
   * Record-level-index tagging: only thin record keys shuffle, partitioned with the same hash
   * that shards the record index across its N file groups, so each Spark partition does point
   * lookups against exactly one index shard. Located keys join back to the input rows; the index
   * never shuffles. Located rows keep their original table partition (the update-partition-path
   * semantics of the global record index with the default config).
   */
  private def tagViaRecordIndex(spark: org.apache.spark.sql.SparkSession,
                                prepared: Dataset[Row],
                                table: HoodieTable[_, _, _, _]): Dataset[Row] = {
    val numFileGroups = table.getTableMetadata.getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX)
    val indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(
      MetadataPartitionType.RECORD_INDEX.getPartitionPath, table.getMetaClient)
    val mappingFunction = MetadataPartitionType
      .fromPartitionPath(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      .getFileGroupMappingFunction(indexVersion)
    val serTable = table

    val locationRows = prepared.select(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
      .queryExecution.toRdd
      .map(row => row.getString(0))
      .map(key => (mappingFunction.apply(key, numFileGroups).intValue(), key))
      .partitionBy(new ShardPartitioner(numFileGroups))
      .map(_._2)
      .mapPartitions { keys =>
        val keyList = new java.util.ArrayList[String]()
        keys.foreach(keyList.add)
        if (keyList.isEmpty) {
          Iterator.empty
        } else {
          val locations = serTable.getTableMetadata
            .readRecordIndexLocationsWithKeys(HoodieListData.eager[String](keyList))
          try {
            HoodieDataUtils.dedupeAndCollectAsList(locations).asScala
              .map(pair => Row(pair.getKey, pair.getValue.getPartitionPath, pair.getValue.getFileId))
              .iterator
          } finally {
            locations.unpersistWithDependencies()
          }
        }
      }
    val locationSchema = StructType(Seq(
      StructField("_hoodie_lookup_key", StringType),
      StructField("_hoodie_located_partition", StringType),
      StructField(TAGGED_FILE_ID_COL, StringType)))
    val locations = spark.createDataFrame(locationRows, locationSchema)
    prepared.join(locations,
        prepared(HoodieRecord.RECORD_KEY_METADATA_FIELD) === locations("_hoodie_lookup_key"),
        "left_outer")
      .drop("_hoodie_lookup_key")
  }

  /**
   * Simple-index-style tagging: read only the meta columns of the latest base files and left
   * join the input on (record key, partition path), producing the target file id column (null
   * for inserts).
   */
  private def tagViaSimpleJoin(spark: org.apache.spark.sql.SparkSession,
                               prepared: Dataset[Row],
                               table: HoodieTable[_, _, _, _],
                               globalIndex: Boolean): Dataset[Row] = {
    // The view only serves partitions already loaded into it; load them all on the same view
    // instance before asking for the latest base files across the table.
    val fsView = table.getHoodieView
    fsView.loadAllPartitions()
    val latestBaseFiles = fsView.getLatestBaseFiles.iterator().asScala.map(_.getPath).toSeq
    if (latestBaseFiles.isEmpty) {
      log.info("Tagging skipped: no latest base files in the table")
      val untagged = prepared.withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
      if (globalIndex) {
        untagged.withColumn(LOCATED_PARTITION_COL, lit(null).cast(StringType))
      } else {
        untagged
      }
    } else {
      log.info(s"Tagging incoming records against ${latestBaseFiles.size} latest base files"
        + s" (${if (globalIndex) "global" else "partition-local"} key matching)")
      val fileIdFromName = udf((fileName: String) => FSUtils.getFileId(fileName))
      val existing = spark.read.parquet(latestBaseFiles: _*)
        .select(
          col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as("_hoodie_existing_key"),
          col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(LOCATED_PARTITION_COL),
          fileIdFromName(col(HoodieRecord.FILENAME_METADATA_FIELD)).as(TAGGED_FILE_ID_COL))
      val keyMatches = prepared(HoodieRecord.RECORD_KEY_METADATA_FIELD) === existing("_hoodie_existing_key")
      val condition = if (globalIndex) {
        keyMatches
      } else {
        keyMatches && prepared(HoodieRecord.PARTITION_PATH_METADATA_FIELD) === existing(LOCATED_PARTITION_COL)
      }
      val joined = prepared.join(existing, condition, "left_outer").drop("_hoodie_existing_key")
      if (globalIndex) joined else joined.drop(LOCATED_PARTITION_COL)
    }
  }

  /**
   * Adds the routing-bucket column. Tagged updates pin to their file group; inserts hash across
   * their partition's slots, where the leading slots pad existing small file groups (routed as
   * update buckets, so they merge) and the remaining slots open new file groups. The profiler
   * mode decides how buckets then map to write tasks.
   */
  private def withRoutingBucket(tagged: Dataset[Row],
                                insertPlan: InsertRoutingPlan): Dataset[Row] = {
    val smallFileSlots = insertPlan.smallFileSlotsByPartition
    val newFileBuckets = insertPlan.newFileBuckets
    val insertBucket = udf { (partitionPath: String, keyHash: Int) =>
      val padding = smallFileSlots.getOrElse(partitionPath, Array.empty[String])
      // Padding-first: without a count pass there is no record budget per small file, so all of
      // a partition's inserts fill its small file groups before any new one opens. A very large
      // batch can overshoot the target file size; the measured-bytes budget of the shuffle-stats
      // profiler is the eventual fix.
      val totalSlots = if (padding.nonEmpty) padding.length else newFileBuckets
      val slot = ((keyHash % totalSlots) + totalSlots) % totalSlots
      if (padding.nonEmpty) {
        padding(slot)
      } else {
        INSERT_BUCKET_PREFIX + partitionPath + ":" + "%010d".format(slot)
      }
    }
    val bucketCol = when(col(TAGGED_FILE_ID_COL).isNotNull,
        concat(lit(UPDATE_BUCKET_PREFIX), col(TAGGED_FILE_ID_COL)))
      .otherwise(insertBucket(
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        hash(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))))
    tagged.withColumn(ROUTING_BUCKET_COL, bucketCol)
  }

  /**
   * Calibrates the shuffle-bytes-to-parquet-bytes ratio from recent commit metadata, so bin
   * sizes track this table's actual encoding and compression instead of a fixed guess.
   */
  private def readShuffleSizeRatio(metaClient: HoodieTableMetaClient): Double = {
    val timeline = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
    val ratio = timeline.getReverseOrderedInstants.iterator().asScala.take(5).flatMap { instant =>
      try {
        val metadata = metaClient.getActiveTimeline.readCommitMetadata(instant)
        val shuffleBytes = Option(metadata.getExtraMetadata.get(SHUFFLE_BYTES_METADATA_KEY))
          .map(_.toLong).getOrElse(0L)
        val bytesWritten = metadata.fetchTotalBytesWritten()
        if (shuffleBytes > 0 && bytesWritten > 0) {
          Some(shuffleBytes.toDouble / bytesWritten)
        } else {
          None
        }
      } catch {
        case _: Exception => None
      }
    }.toSeq.headOption.getOrElse(DEFAULT_SHUFFLE_SIZE_RATIO)
    log.info(s"Shuffle-to-parquet size ratio for bin sizing: $ratio")
    ratio
  }
}

/**
 * Passes through pre-computed shard ids as Spark partition ids, aligning the thin-key shuffle
 * with the record index's own file-group hashing (Spark's Murmur3 partitioning does not).
 */
private class ShardPartitioner(shards: Int) extends Partitioner {
  override def numPartitions: Int = shards
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
