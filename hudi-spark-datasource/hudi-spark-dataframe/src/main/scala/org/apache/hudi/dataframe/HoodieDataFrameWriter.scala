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

import org.apache.hudi.{DataFrameWriteResult, DataFrameWriteSpec, DataSourceUtils, DataSourceWriteOptions, HoodieDataFrameWritePath, HoodieSchemaConversionUtils, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient, WriteStatus}
import org.apache.hudi.client.clustering.update.strategy.{SparkAllowUpdateStrategy, SparkRejectUpdateStrategy}
import org.apache.hudi.client.utils.SparkValidatorUtils
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieBaseFile, HoodieFileFormat, HoodieFileGroupId, HoodieRecord, HoodieRecordGlobalLocation, HoodieRecordLocation, HoodieTableType, MetaFieldsMode, OverwriteWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{CommitUtils, ConfigUtils, HoodieDataUtils, HoodieRecordUtils, HoodieTimer, Option => HOption, StringUtils}
import org.apache.hudi.common.util.collection.{Pair => HoodiePair}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.exception.{HoodieClusteringUpdateException, HoodieDuplicateKeyException, HoodieUpsertException}
import org.apache.hudi.index.{HoodieIndex, PartitionedRecordIndexFileGroupLookupFunction, SparkHoodieIndexFactory}
import org.apache.hudi.io.HoodieKeyLocationFetchHandle
import org.apache.hudi.keygen.{BaseKeyGenerator, KeyGenUtils}
import org.apache.hudi.keygen.constant.{KeyGeneratorOptions, KeyGeneratorType}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.metadata.{BucketizedMetadataTableFileGroupIndexParser, HoodieIndexVersion, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.metrics.RecordIndexLookupMetrics
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.HoodieWriteMetadata
import org.apache.hudi.util.{CommonClientUtils, SparkKeyGenUtils}

import org.apache.spark.Partitioner
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, HoodieInternalRowUtils, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, hash, input_file_name, lit, monotonically_increasing_id, row_number, size, struct, udf, when}
import org.apache.spark.sql.hudi.command.SqlKeyGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType, StringType, StructField, StructType}

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

/**
 * The record write of a datasource write as DataFrame transformations, on Spark's internal rows
 * from the input to the file writers: key generation and meta columns as a projection, index
 * tagging as a join or an index lookup, routing as a shuffle on a routing-bucket column, and the
 * write as per-bucket tasks driving the row-based write handles. Table setup, the commit and
 * everything after it stay with the caller.
 */
class HoodieDataFrameWriter extends HoodieDataFrameWritePath with SparkAdapterSupport with Logging {

  import HoodieDataFrameWriter._

  override def unsupportedReason(spec: DataFrameWriteSpec): Option[String] = {
    val params = spec.parameters
    val tableConfig = spec.metaClient.getTableConfig
    val writeConfig = resolveWriteConfig(spec)
    val checks: Seq[(() => Boolean, String)] = Seq(
      (() => !SUPPORTED_OPERATIONS.contains(spec.operation), s"operation ${spec.operation.value}"),
      (() => StringUtils.nonEmpty(writeConfig.getIndexClass), "custom index classes"),
      (() => !SUPPORTED_INDEX_TYPES.contains(writeConfig.getIndexType), s"index type ${writeConfig.getIndexType}"),
      (() => tableConfig.getMetaFieldsMode != MetaFieldsMode.ALL || writeConfig.getMetaFieldsMode != MetaFieldsMode.ALL,
        "tables that do not populate all meta fields"),
      (() => tableConfig.isLSMTreeStorageLayout, "the LSM tree storage layout"),
      (() => tableConfig.getBaseFileFormat != HoodieFileFormat.PARQUET || tableConfig.isMultipleBaseFileFormatsEnabled,
        s"base file format ${tableConfig.getBaseFileFormat}"),
      (() => (isCustomMerge(writeConfig, tableConfig) || payloadDecides(writeConfig))
        && SparkHoodieIndexFactory.isGlobalIndex(writeConfig), "custom merging under a global index"),
      (() => combinesInTask(spec, writeConfig, tableConfig)
        && params.getOrElse(PROFILER, PROFILER_METADATA) == PROFILER_SHUFFLE_STATS,
        "custom merging that combines records with the shuffle-stats profiler"),
      (() => writeConfig.allowOperationMetadataField, "the operation meta field"),
      // Rejecting comes before any rollback of the clustering, so only allowing combines with one.
      (() => writeConfig.getClusteringUpdatesStrategyClass != classOf[SparkRejectUpdateStrategy[_]].getName
        && (writeConfig.isRollbackPendingClustering || !allowsUpdatesOfPendingClustering(writeConfig)),
        "clustering update strategies other than rejecting the update or leaving it to conflict resolution"))
    if (spec.operation == WriteOperationType.DELETE_PARTITION) {
      // Only the partition paths of the rows are derived, whatever the table is.
      None
    } else {
      checks.collectFirst { case (applies, what) if applies() => s"$what are not supported by it yet" }
    }
  }

  override def clientOptions(spec: DataFrameWriteSpec): Map[String, String] = {
    val params = spec.parameters
    if (params.contains(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY)
      || spec.operation == WriteOperationType.DELETE_PARTITION) {
      // A merger the user set decides the record type, and deleting partitions writes no record.
      Map.empty
    } else {
      val writeConfig = DataSourceUtils.createHoodieConfig(spec.writerSchema.map(_.toString).orNull,
        spec.metaClient.getBasePath.toString, params(HoodieWriteConfig.TBL_NAME.key), params.asJava)
      if (canWriteSparkRecords(spec, writeConfig)) {
        // The engine-native record merger keeps the merge on InternalRow.
        Map(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY -> "org.apache.hudi.DefaultSparkRecordMerger")
      } else {
        // The handles take Avro records, built in the write task.
        Map.empty
      }
    }
  }

  /**
   * Whether engine-native records write the table the way Avro records do. They do not where the
   * merging goes through a payload class or a custom merger, where log blocks are Avro, where the
   * column stats index predates the version that covers every type for them, where rows cannot
   * be rewritten to the writer schema as rows, where a merge would read a variant column of a
   * base file with schema-on-read, which the row-based reader does not serve, and where a merge
   * would read a bootstrapped base file, whose rows it cannot rewrite to the writer schema in
   * every partitioning.
   */
  private def canWriteSparkRecords(spec: DataFrameWriteSpec, writeConfig: HoodieWriteConfig): Boolean = {
    val tableConfig = spec.metaClient.getTableConfig
    val avroLogBlocks = tableConfig.getTableType == HoodieTableType.MERGE_ON_READ &&
      CommonClientUtils.getLogBlockType(writeConfig, tableConfig) != HoodieLogBlockType.PARQUET_DATA_BLOCK
    val columnStatsByRecordType = HoodieTableMetadataUtil.existingIndexVersionOrDefault(
      MetadataPartitionType.COLUMN_STATS.getPartitionPath, spec.metaClient).lowerThan(HoodieIndexVersion.V2)
    val variantUnderSchemaOnRead =
      spec.parameters.getOrElse(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, "false").toBoolean &&
        containsVariant(spec.writerSchema.map(HoodieInternalRowUtils.getCachedSchema).getOrElse(spec.inputSchema))
    !isCustomMerge(writeConfig, tableConfig) && !payloadDecides(writeConfig) && !avroLogBlocks &&
      !columnStatsByRecordType && !variantUnderSchemaOnRead && !tableConfig.getBootstrapBasePath.isPresent &&
      rowsConformToWriterSchema(spec)
  }

  private def containsVariant(dataType: DataType): Boolean = dataType match {
    case struct: StructType => struct.fields.exists(field => containsVariant(field.dataType))
    case array: ArrayType => containsVariant(array.elementType)
    case map: MapType => containsVariant(map.keyType) || containsVariant(map.valueType)
    case other => other.typeName == "variant"
  }

  /** Whether the input rows can be rewritten to the writer schema without leaving InternalRow. */
  private def rowsConformToWriterSchema(spec: DataFrameWriteSpec): Boolean = spec.writerSchema.forall { schema =>
    val metaColumns = HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.asScala.toSet
    val source = StructType(spec.inputSchema.fields.filterNot(field => metaColumns.contains(field.name)))
    val target = HoodieInternalRowUtils.getCachedSchema(
      org.apache.hudi.common.schema.HoodieSchemaUtils.removeMetadataFields(schema))
    DataFrameWriteTask.canConform(source, target)
  }

  override def write(spec: DataFrameWriteSpec,
                     client: SparkRDDWriteClient[_],
                     instantTime: String,
                     df: DataFrame): DataFrameWriteResult = {
    if (spec.operation == WriteOperationType.DELETE_PARTITION) {
      deletePartitions(spec, client, instantTime, df)
    } else {
      try {
        writeRows(spec, client, instantTime, df)
      } catch {
        case e: HoodieUpsertException => throw e
        // What fails before the write statuses are handed over fails the way the write client's
        // own write does.
        case e: Throwable => throw new HoodieUpsertException(s"Failed to upsert for commit time $instantTime", e)
      }
    }
  }

  /** Deletes the partitions the rows of `df` are in, by the partition paths the key generator derives for them. */
  private def deletePartitions(spec: DataFrameWriteSpec,
                               client: SparkRDDWriteClient[_],
                               instantTime: String,
                               df: DataFrame): DataFrameWriteResult = {
    val writeConfig = client.getConfig
    val metaClient = spec.metaClient
    val input = executable(df)
    val keys = HoodieKeyGeneratorExpression.column(input, keyGeneratorProps(writeConfig, metaClient), instantTime,
      !metaClient.getTableConfig.getRecordKeyFields.isPresent, generatesKeysFromAvro(input, spec, writeConfig, metaClient))
    val partitions = input.select(keys.getField(HoodieKeyGeneratorExpression.PARTITION_PATH_FIELD))
      .distinct().collect().map(_.getString(0)).toSeq
    writeCount.incrementAndGet()
    DataFrameWriteResult(client.deletePartitions(partitions.asJava, instantTime))
  }

  /**
   * A micro-batch of the streaming sink only allows its physical plan to be executed, so a plan
   * over its rows is built from that.
   */
  private def executable(df: DataFrame): DataFrame = {
    if (df.isStreaming) {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(df.sparkSession, df.queryExecution.toRdd, df.schema)
    } else {
      df
    }
  }

  private def writeRows(spec: DataFrameWriteSpec,
                        client: SparkRDDWriteClient[_],
                        instantTime: String,
                        df: DataFrame): DataFrameWriteResult = {
    val spark = df.sparkSession
    val operation = spec.operation
    val params = spec.parameters
    val writeConfig = client.getConfig
    val engineContext = client.getEngineContext
    // Prepped upserts and deletes are booked under their own operation types, as the write client does.
    val clientOperation = (operation, spec.keysFromMetaColumns) match {
      case (WriteOperationType.UPSERT, true) => WriteOperationType.UPSERT_PREPPED
      case (WriteOperationType.DELETE, true) => WriteOperationType.DELETE_PREPPED
      case _ => operation
    }
    val table = client.initTable(clientOperation, HOption.of(instantTime))
    operation match {
      case WriteOperationType.UPSERT => table.validateUpsertSchema()
      case WriteOperationType.DELETE =>
      case _ => table.validateInsertSchema()
    }
    client.preWrite(instantTime, clientOperation, table.getMetaClient)
    writeCount.incrementAndGet()
    val tableConfig = table.getMetaClient.getTableConfig
    val mergeOnRead = tableConfig.getTableType == HoodieTableType.MERGE_ON_READ
    val overwrite = operation == WriteOperationType.INSERT_OVERWRITE || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
    // A delete writes no rows, so its schema is the table's, which the client sets for deletes.
    val droppedColumns = droppedPartitionColumns(spec)
    val writerSchema = org.apache.hudi.common.schema.HoodieSchemaUtils.removeFields(
      org.apache.hudi.common.schema.HoodieSchemaUtils.removeMetadataFields(
        spec.writerSchema.getOrElse(HoodieSchema.parse(writeConfig.getSchema))), droppedColumns.asJava)

    val input = executable(df)

    // Stage 1 + 2: key generation, meta columns, and upfront precombine as pure projections.
    val prepared = prepareInput(input, spec, writeConfig, table.getMetaClient, instantTime)

    // Stage 3: index tagging (record-level-index point lookup or simple-index join), unless
    // the rows already carry their file group.
    val tagged = tagRecords(spark, prepared.rows, spec, writeConfig, table, engineContext)

    // Stage 4: routing on a bucket column; updates pin to their file group, inserts hash
    // across small-file padding slots and new-file buckets planned from table metadata. An
    // overwrite replaces the file groups it would pad, so it only opens new ones.
    val newFileBuckets = params.get(INSERT_BUCKETS_PER_PARTITION).map(_.toInt).getOrElse(1)
    // Read from the timeline by a view of its own: the plan and the write tasks have to agree on
    // the file groups a pending clustering is rewriting.
    val pendingClusteringFileGroups: Set[HoodieFileGroupId] =
      table.getFileSystemView.getFileGroupsInPendingClustering.iterator().asScala.map(_.getKey).toSet
    val insertPlan = if (overwrite) {
      InsertRoutingPlan(Map.empty, newFileBuckets)
    } else {
      MetadataSizingProfiler.plan(table, writeConfig, newFileBuckets, mergeOnRead,
        if (mergeOnRead) MERGE_BUCKET_PREFIX else UPDATE_BUCKET_PREFIX, pendingClusteringFileGroups)
    }
    val profilerMode = params.getOrElse(PROFILER, PROFILER_METADATA)
    // The tagged updates of a small file group that takes inserts join them in one bucket, so
    // one handle writes the file group, when the write task can rely on the order of rows
    // within a bucket.
    val colocateUpdatesWithPadding = profilerMode != PROFILER_SHUFFLE_STATS
    // The file groups a write may not update or replace; none where that is left to conflict resolution.
    val protectedFileGroups = if (allowsUpdatesOfPendingClustering(writeConfig)) Set.empty[HoodieFileGroupId] else pendingClusteringFileGroups
    rejectUpdatesOfPendingClustering(tagged, protectedFileGroups)
    val routedToBuckets = withRoutingBucket(tagged, insertPlan, colocateUpdatesWithPadding)
    // A delete-marker row that opens a new file has no record to remove.
    val withBucket = deleteMarkerOf(tagged, writeConfig, tableConfig)
      .map(marker => routedToBuckets.filter(!(col(ROUTING_BUCKET_COL).startsWith(INSERT_BUCKET_PREFIX) && marker)))
      .getOrElse(routedToBuckets)
    val bucketOrdinal = withBucket.schema.fieldIndex(ROUTING_BUCKET_COL)

    val actionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)
    table.getActiveTimeline.transitionRequestedToInflight(
      table.getMetaClient.createNewInstant(HoodieInstant.State.REQUESTED, actionType, instantTime),
      HOption.empty())

    // Stage 5: per-bucket write tasks driving the write handles. The reader context factory is
    // created on the driver (the table's engine context is not usable on executors) and shipped
    // with the task.
    val sparkTable = table.asInstanceOf[HoodieTable[InternalRow, _, _, _]]
    val readerContextFactory = sparkTable.getReaderContextFactoryForWrite
    val task = new DataFrameWriteTask(
      writeConfig, sparkTable, readerContextFactory, instantTime, operation, withBucket.schema,
      prepared.dataSchema.length, HoodieInternalRowUtils.getCachedSchema(writerSchema), bucketOrdinal,
      withBucket.schema.fieldIndex(ROW_OP_COL), withBucket.schema.fieldIndex(TAGGED_FILE_ID_COL),
      withBucket.schema.fieldIndex(TAGGED_INSTANT_COL), withBucket.schema.fieldIndex(TAGGED_POSITION_COL), mergeOnRead,
      protectedFileGroups,
      avroRecordSpec(spec, prepared.dataSchema, writerSchema, writeConfig, tableConfig),
      // The row-based create handle takes rows in the writer schema and neither consults a
      // payload class nor tracks the event-time watermark.
      recordBasedInserts = payloadDecides(writeConfig) || !rowsConformToWriterSchema(spec) ||
        (ConfigUtils.isTrackingEventTimeWatermark(writeConfig.getProps) && ConfigUtils.getEventTimeFieldName(writeConfig.getProps) != null),
      combineRecords = combinesInTask(spec, writeConfig, tableConfig),
      sqlConfOverrides = writeTaskConfOverrides(spec, writeConfig))

    val (statusRdd, extraMetadata) = if (profilerMode == PROFILER_SHUFFLE_STATS) {
      // The routing shuffle doubles as the profiling pass and lives inside a physical-plan
      // node over the catalyst plan (the Delta optimized-writer shape): only its map stage
      // runs first, measured block sizes plan the write tasks, and the reduce side reads the
      // shuffle files. The execution id makes the plan visible in the SQL tab and propagates
      // the SQL conf to its tasks.
      val sizeRatio = readShuffleSizeRatio(table.getMetaClient)
      val binSize = math.max(1L, (writeConfig.getParquetMaxFileSize * sizeRatio).toLong)
      val shuffleExec = org.apache.spark.sql.hudi.execution.HoodieRoutingShuffleExec(
        withBucket.queryExecution.executedPlan, bucketOrdinal,
        params.getOrElse(SHUFFLE_UPDATE_REDUCERS, "50").toInt,
        params.getOrElse(SHUFFLE_INSERT_REDUCERS, "50").toInt,
        INSERT_BUCKET_PREFIX, binSize)
      val routedRows = org.apache.spark.sql.hudi.execution.HoodieSqlExecution.withExecutionId(
        withBucket.queryExecution, s"hudi dataframe write: prepare, tag and profile ($operation)") {
        spark.sparkContext.setJobDescription(
          s"hudi dataframe write: routing shuffle map stage with sizing profile ($operation)")
        try {
          shuffleExec.execute()
        } finally {
          spark.sparkContext.setJobDescription(null)
        }
      }
      val shuffle = shuffleExec.shuffleStats
      spark.sparkContext.setJobDescription(
        s"hudi dataframe write: ${shuffle.binCount} write tasks and commit ($operation)")
      (HoodieSparkUtils.injectSQLConf(
        routedRows.mapPartitions(iter => task.execute(iter)), SQLConf.get),
        Map(SHUFFLE_BYTES_METADATA_KEY -> shuffle.totalShuffleBytes.toString))
    } else {
      val numWriteTasks = params.get(WRITE_TASKS).map(_.toInt)
        .getOrElse(spark.sparkContext.defaultParallelism)
      // Within a bucket, the inserts that pad the file group come before its tagged updates, so
      // the write task sees from the first row whether the run pads. Combining in the write task
      // also needs the records of a key next to each other.
      val sortColumns = Seq(col(ROUTING_BUCKET_COL), col(TAGGED_FILE_ID_COL).isNotNull) ++
        (if (combinesInTask(spec, writeConfig, tableConfig)) Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD)) else Seq.empty)
      val routed = withBucket
        .repartition(numWriteTasks, col(ROUTING_BUCKET_COL))
        .sortWithinPartitions(sortColumns: _*)
      (HoodieSparkUtils.injectSQLConf(
        routed.queryExecution.toRdd.mapPartitions(iter => task.execute(iter)), SQLConf.get),
        Map.empty[String, String])
    }

    val javaRdd = new JavaRDD[WriteStatus](statusRdd)
    HoodieJavaRDD.of(javaRdd).persist(
      writeConfig.getString(HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE),
      engineContext, HoodieDataCacheKey.of(writeConfig.getBasePath, instantTime))
    val replacedFileIds = partitionToReplacedFileIds(operation, javaRdd, table, writeConfig, protectedFileGroups)
    if (StringUtils.nonEmpty(writeConfig.getPreCommitValidators)) {
      val writeMetadata = new HoodieWriteMetadata[HoodieData[WriteStatus]]()
      writeMetadata.setWriteStatuses(HoodieJavaRDD.of(javaRdd))
      writeMetadata.setPartitionToReplaceFileIds(replacedFileIds)
      SparkValidatorUtils.runValidators(writeConfig, writeMetadata, engineContext, table, instantTime)
    }
    DataFrameWriteResult(new HoodieWriteResult(javaRdd, replacedFileIds), extraMetadata)
  }

  /**
   * Fails the write, before anything is written, when it updates a file group that a pending
   * clustering is rewriting. The write tasks refuse such an update as well, but only once they
   * run, which is when the write is committed.
   */
  private def rejectUpdatesOfPendingClustering(tagged: Dataset[Row], pendingClusteringFileGroups: Set[HoodieFileGroupId]): Unit = {
    if (pendingClusteringFileGroups.nonEmpty) {
      val pendingFileIds = pendingClusteringFileGroups.map(_.getFileId).toSeq
      tagged.filter(col(TAGGED_FILE_ID_COL).isin(pendingFileIds: _*))
        .select(col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), col(TAGGED_FILE_ID_COL))
        .head(1)
        .foreach(row => throw DataFrameWriteTask.pendingClusteringUpdateError(new HoodieFileGroupId(row.getString(0), row.getString(1))))
    }
  }

  /**
   * SQL conf entries the write tasks set over the session's. Where the write is one of Avro
   * records without this path, dates and timestamps are written in the calendar they come in,
   * as an Avro record's are, unless the session says how to rebase them.
   */
  private def writeTaskConfOverrides(spec: DataFrameWriteSpec, writeConfig: HoodieWriteConfig): Map[String, String] = {
    if (avroRecordsWithoutThisPath(spec, writeConfig)) {
      Seq(PARQUET_REBASE_MODE_IN_WRITE, PARQUET_INT96_REBASE_MODE_IN_WRITE)
        .filterNot(SQLConf.get.contains).map(_ -> "CORRECTED").toMap
    } else {
      Map.empty
    }
  }

  /**
   * The file groups an overwrite replaces: every file group of the partitions it wrote to (or of
   * the statically named partitions), or of the whole table for a table overwrite.
   */
  private def partitionToReplacedFileIds(operation: WriteOperationType,
                                         writeStatuses: JavaRDD[WriteStatus],
                                         table: HoodieTable[_, _, _, _],
                                         writeConfig: HoodieWriteConfig,
                                         pendingClusteringFileGroups: Set[HoodieFileGroupId]): java.util.Map[String, java.util.List[String]] = {
    val partitions: Seq[String] = operation match {
      case WriteOperationType.INSERT_OVERWRITE =>
        val staticPartitions = writeConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS)
        if (StringUtils.nonEmpty(staticPartitions)) {
          staticPartitions.split(",").toSeq
        } else {
          writeStatuses.rdd.map(_.getStat.getPartitionPath).distinct().collect().toSeq
        }
      case WriteOperationType.INSERT_OVERWRITE_TABLE =>
        FSUtils.getAllPartitionPaths(table.getContext, table.getMetaClient, writeConfig.getMetadataConfig).asScala.toSeq
      case _ => Seq.empty
    }
    val replaced = new java.util.HashMap[String, java.util.List[String]]()
    partitions.foreach { partition =>
      val fileIds = table.getSliceView.getLatestFileSlices(partition).iterator().asScala.map(_.getFileId).toSeq.distinct
      fileIds.map(new HoodieFileGroupId(partition, _)).find(pendingClusteringFileGroups.contains).foreach { fileGroup =>
        throw new HoodieClusteringUpdateException(
          s"Not allowed to update the clustering file group $fileGroup. "
            + "For pending clustering operations, we are not going to support update for now.")
      }
      replaced.put(partition, fileIds.asJava)
    }
    replaced
  }

  /**
   * Whether a row is a delete by what it carries: the built-in marker column, or the field and
   * value the merge properties name for deletes, which the merging reads off the row where no
   * payload class decides.
   */
  private def deleteMarkerOf(rows: DataFrame, writeConfig: HoodieWriteConfig, tableConfig: HoodieTableConfig): Option[Column] = {
    val builtIn = if (rows.columns.contains(DELETE_MARKER_COL)) {
      Some(coalesce(col(DELETE_MARKER_COL), lit(false)))
    } else {
      None
    }
    val props = mergeProps(writeConfig, tableConfig)
    val custom = for {
      field <- Option(props.getProperty(DefaultHoodieRecordPayload.DELETE_KEY)).filter(_.nonEmpty)
      marker <- Option(props.getProperty(DefaultHoodieRecordPayload.DELETE_MARKER)).filter(_.nonEmpty)
      if !payloadDecides(writeConfig) && resolves(rows, field)
    } yield coalesce(col(field).cast(StringType) === lit(marker), lit(false))
    (builtIn ++ custom).reduceOption(_ || _)
  }

  private def allowsUpdatesOfPendingClustering(writeConfig: HoodieWriteConfig): Boolean =
    writeConfig.getClusteringUpdatesStrategyClass == classOf[SparkAllowUpdateStrategy[_]].getName

  private def resolveWriteConfig(spec: DataFrameWriteSpec): HoodieWriteConfig = {
    val params = spec.parameters ++ clientOptions(spec)
    DataSourceUtils.createHoodieConfig(spec.writerSchema.map(_.toString).orNull, spec.metaClient.getBasePath.toString,
      params(HoodieWriteConfig.TBL_NAME.key), params.asJava)
  }

  /** How the handles take records when the write config's record merger works on Avro records. */
  private def avroRecordSpec(spec: DataFrameWriteSpec,
                             inputSchema: StructType,
                             writerSchema: HoodieSchema,
                             writeConfig: HoodieWriteConfig,
                             tableConfig: HoodieTableConfig): Option[AvroRecordSpec] = {
    if (writeConfig.getRecordMerger.getRecordType != HoodieRecordType.AVRO) {
      None
    } else {
      val sourceSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        inputSchema, writerSchema.getName, writerSchema.getNamespace.orElse(null))
      val orderingFields = tableConfig.getOrderingFields
      val payloadClass = writeConfig.getPayloadClass
      Some(AvroRecordSpec(
        sourceSchema = sourceSchema.toString,
        writerSchema = writerSchema.toString,
        payloadClass = payloadClass,
        orderingFields = orderingFields,
        attachOrderingValue = shouldCombine(spec, writeConfig, tableConfig) && !orderingFields.isEmpty,
        requiresOrderingValue = !(tableConfig.getRecordMergeMode == RecordMergeMode.COMMIT_TIME_ORDERING
          || classOf[OverwriteWithLatestAvroPayload].getName == payloadClass),
        consistentLogicalTimestamp = spec.parameters.getOrElse(
          DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key,
          DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue).toBoolean))
    }
  }

  /** What an insert does with keys the table already has: nothing, drop them, or fail. */
  private def insertDuplicatePolicy(spec: DataFrameWriteSpec): String = {
    val params = spec.parameters
    if (params.getOrElse(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "false").toBoolean) {
      DataSourceWriteOptions.DROP_INSERT_DUP_POLICY
    } else {
      params.getOrElse(DataSourceWriteOptions.INSERT_DUP_POLICY.key, DataSourceWriteOptions.NONE_INSERT_DUP_POLICY)
        .toLowerCase(java.util.Locale.ROOT)
    }
  }

  /**
   * Whether merging two records of a key is more than picking one by ordering: a custom merge
   * mode, or a partial update mode, under which the fields of both records make up the result.
   */
  private def isCustomMerge(writeConfig: HoodieWriteConfig, tableConfig: HoodieTableConfig): Boolean =
    writeConfig.getRecordMergeMode == RecordMergeMode.CUSTOM || tableConfig.getRecordMergeMode == RecordMergeMode.CUSTOM ||
      tableConfig.getPartialUpdateMode.isPresent

  /** Whether the write's payload class has behavior of its own, beyond what the merge mode expresses. */
  private def payloadDecides(writeConfig: HoodieWriteConfig): Boolean = {
    val payloadClass = writeConfig.getPayloadClass
    StringUtils.nonEmpty(payloadClass) && !HoodieRecordUtils.isPayloadClassDeprecated(payloadClass)
  }

  /**
   * Whether the records of a key are combined in the write task with the table's record merger,
   * as custom mergers and payload classes need; otherwise combining is a window over the
   * ordering fields.
   */
  private def combinesInTask(spec: DataFrameWriteSpec,
                             writeConfig: HoodieWriteConfig,
                             tableConfig: HoodieTableConfig): Boolean =
    shouldCombine(spec, writeConfig, tableConfig) && (isCustomMerge(writeConfig, tableConfig) || payloadDecides(writeConfig))

  private def mergeProps(writeConfig: HoodieWriteConfig, tableConfig: HoodieTableConfig): TypedProperties =
    ConfigUtils.getMergeProps(writeConfig.getProps, tableConfig)

  /** Whether the write deduplicates the incoming records of a key before writing them. */
  private def shouldCombine(spec: DataFrameWriteSpec,
                            writeConfig: HoodieWriteConfig,
                            tableConfig: HoodieTableConfig): Boolean = {
    // Keys generated for this write are unique, and rows that bring their keys in the meta
    // columns are written as they are. A MERGE into a table without record keys does combine:
    // its rows are keyed by the stored rows they matched.
    val generatesKeys = !tableConfig.getRecordKeyFields.isPresent && !spec.locationFromMetaColumns
    !spec.keysFromMetaColumns && !generatesKeys && (spec.operation match {
      case WriteOperationType.UPSERT => writeConfig.shouldCombineBeforeUpsert()
      case WriteOperationType.DELETE => writeConfig.shouldCombineBeforeDelete()
      case _ => writeConfig.shouldCombineBeforeInsert()
    })
  }

  /**
   * Whether the key generator is given rows as Avro records. The built-in key generators answer
   * the same for rows and for Avro records, so they read rows. A key generator of the user's can
   * differ between the two, so it reads what the write's records are without this path, Avro by
   * default. A key field the input does not have also goes through the Avro side, which raises
   * the errors a write of Avro records raises for it.
   */
  private def generatesKeysFromAvro(input: DataFrame,
                                    spec: DataFrameWriteSpec,
                                    writeConfig: HoodieWriteConfig,
                                    metaClient: HoodieTableMetaClient): Boolean = {
    val props = keyGeneratorProps(writeConfig, metaClient)
    val configuredClass = HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props)
    val keyGeneratorClass = if (configuredClass == classOf[SqlKeyGenerator].getName) {
      props.getString(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, null)
    } else {
      configuredClass
    }
    val userProvided = keyGeneratorClass != null &&
      KeyGeneratorType.fromClassName(keyGeneratorClass) == KeyGeneratorType.USER_PROVIDED
    val keyFields = (props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "").split(",") ++
      props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "").split(",").map(_.split(":")(0)))
      .map(_.trim).filter(_.nonEmpty)
    (userProvided && avroRecordsWithoutThisPath(spec, writeConfig)) || !keyFields.forall(resolves(input, _))
  }

  /**
   * Whether the write is one of Avro records unless this path picks the record type, which it
   * does when the write names no record merger. Checks that a write of Avro records makes on
   * its input, and that a write of Spark records does not make, follow this.
   */
  private def avroRecordsWithoutThisPath(spec: DataFrameWriteSpec, writeConfig: HoodieWriteConfig): Boolean =
    !spec.parameters.contains(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY) ||
      writeConfig.getRecordMerger.getRecordType == HoodieRecordType.AVRO

  /**
   * Fails the write on a vector that does not have the dimension of its column. The file writer
   * checks this too, but there a mismatch is a failed record of the write status.
   */
  private def requireVectorDimensions(rows: DataFrame, spec: DataFrameWriteSpec): DataFrame = {
    val dimensions: Seq[(String, Int)] = spec.writerSchema.toSeq.flatMap(_.getFields.asScala).flatMap { field =>
      field.schema.getNonNullType match {
        case vector: HoodieSchema.Vector if resolves(rows, quoted(field.name)) => Some(field.name -> vector.getDimension)
        case _ => None
      }
    }
    if (dimensions.isEmpty) {
      rows
    } else {
      val requireDimension = udf((field: String, expected: Int, actual: Int) => {
        if (field != null) {
          throw new IllegalArgumentException(
            s"Vector dimension mismatch at $field: schema expects $expected elements but got $actual")
        }
        true
      }).asNondeterministic()
      dimensions.foldLeft(rows) { case (checked, (field, dimension)) =>
        val mismatched = col(quoted(field)).isNotNull && size(col(quoted(field))) =!= dimension
        // A null vector has no size to report, and a null argument would null the check itself.
        val actual = when(mismatched, size(col(quoted(field)))).otherwise(lit(dimension))
        checked.filter(requireDimension(when(mismatched, lit(field)), lit(dimension), actual))
      }
    }
  }

  private def resolves(frame: DataFrame, field: String): Boolean =
    scala.util.Try(frame.select(col(field))).isSuccess

  /** The partition columns the write keeps in the partition path only, as the write options name them. */
  private def droppedPartitionColumns(spec: DataFrameWriteSpec): Set[String] = {
    if (spec.parameters.getOrElse(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key, "false").toBoolean) {
      SparkKeyGenUtils.getPartitionColumns(TypedProperties.fromMap(spec.parameters.asJava))
        .split(",").map(_.trim).filter(_.nonEmpty).toSet
    } else {
      Set.empty
    }
  }

  private def keyGeneratorProps(writeConfig: HoodieWriteConfig, metaClient: HoodieTableMetaClient): TypedProperties =
    KeyGenUtils.withComplexKeyGenEncoding(TypedProperties.copy(writeConfig.getProps), metaClient.getTableConfig)

  /**
   * The prepare stage as projections: the record key and partition path from the table's key
   * generator (or from the meta columns of prepped rows), the remaining meta columns as
   * placeholders, the data columns, and for rows that carry their location the file group they
   * are in. Then the upfront precombine as a window dedup on the ordering fields and the input
   * order.
   */
  private def prepareInput(input: DataFrame,
                           spec: DataFrameWriteSpec,
                           writeConfig: HoodieWriteConfig,
                           metaClient: HoodieTableMetaClient,
                           instantTime: String): PreparedRows = {
    val tableConfig = metaClient.getTableConfig
    val isDelete = spec.operation == WriteOperationType.DELETE
    val metaColumns = HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.asScala.toSet
    val carriesMetaColumns = spec.keysFromMetaColumns || spec.locationFromMetaColumns
    // The keys are generated from every column of the input; the data files leave out the
    // partition columns the write drops.
    val leftOut = (if (carriesMetaColumns) metaColumns else Set.empty[String]) ++ droppedPartitionColumns(spec)
    val dataFields = if (isDelete) {
      Array.empty[StructField]
    } else {
      input.schema.fields.filterNot(field => leftOut.contains(field.name))
    }
    val dataColumns = dataFields.toSeq.map(field => col(quoted(field.name)))

    val keys = if (spec.keysFromMetaColumns) {
      struct(
        col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as(HoodieKeyGeneratorExpression.RECORD_KEY_FIELD),
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(HoodieKeyGeneratorExpression.PARTITION_PATH_FIELD))
    } else {
      HoodieKeyGeneratorExpression.column(input, keyGeneratorProps(writeConfig, metaClient), instantTime,
        !tableConfig.getRecordKeyFields.isPresent, generatesKeysFromAvro(input, spec, writeConfig, metaClient))
    }
    val location = if (spec.locationFromMetaColumns) {
      val fileIdFromName = udf((fileName: String) => FSUtils.getFileId(fileName))
      Seq(when(col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isNotNull && col(HoodieRecord.FILENAME_METADATA_FIELD).isNotNull,
        fileIdFromName(col(HoodieRecord.FILENAME_METADATA_FIELD))).as(TAGGED_FILE_ID_COL),
        col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).as(TAGGED_INSTANT_COL))
    } else {
      Seq.empty
    }
    val combineByWindow = shouldCombine(spec, writeConfig, tableConfig) && !combinesInTask(spec, writeConfig, tableConfig)
    val inputOrder = if (combineByWindow) Seq(monotonically_increasing_id().as(INPUT_ORDER_COL)) else Seq.empty
    val keyed = input.select((keys.as(KEYS_COL) +: (dataColumns ++ location ++ inputOrder)): _*)
    val withMeta = requireVectorDimensions(keyed, spec).select(
      (Seq(
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
        col(KEYS_COL).getField(HoodieKeyGeneratorExpression.RECORD_KEY_FIELD).as(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        col(KEYS_COL).getField(HoodieKeyGeneratorExpression.PARTITION_PATH_FIELD)
          .as(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.FILENAME_METADATA_FIELD))
        ++ dataColumns
        ++ (if (spec.locationFromMetaColumns) Seq(col(TAGGED_FILE_ID_COL), col(TAGGED_INSTANT_COL)) else Seq.empty)
        ++ inputOrder.map(_ => col(INPUT_ORDER_COL))): _*)

    val rows = if (!combineByWindow) {
      withMeta
    } else {
      val dedupKeys = if (SparkHoodieIndexFactory.isGlobalIndex(writeConfig)) {
        Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
      } else {
        Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD), col(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
      }
      // The greatest ordering value wins under event-time ordering. Under commit-time ordering,
      // for deletes, and among equal ordering values, the row that comes later in the input wins.
      val orderingFields = if (writeConfig.getRecordMergeMode == RecordMergeMode.EVENT_TIME_ORDERING && !isDelete) {
        tableConfig.getOrderingFields.asScala.toSeq
      } else {
        Seq.empty
      }
      // An ordering field the input does not have is a null ordering value on every row.
      val (presentOrderingFields, missingOrderingFields) = orderingFields.partition(f => resolves(withMeta, f))
      val ordering = presentOrderingFields.map(f => col(f).desc) :+ col(INPUT_ORDER_COL).desc
      val window = Window.partitionBy(dedupKeys: _*).orderBy(ordering: _*)
      // An Avro record that is combined on its ordering value has to have one. The check is not
      // deterministic to the optimizer, which would otherwise infer it for the other side of
      // the tagging join.
      val requireOrderingValue = udf((field: String, recordKey: String) => {
        if (field != null) {
          throw new IllegalArgumentException(
            s"Ordering field '$field' has null value for record key '$recordKey'. "
              + "Please ensure all records have non-null values for the ordering field, "
              + "or use a payload class that doesn't require ordering (e.g., OverwriteWithLatestAvroPayload).")
        }
        true
      }).asNondeterministic()
      val nullOrderingField = missingOrderingFields.headOption.map(lit(_)).getOrElse(
        presentOrderingFields.foldRight(lit(null).cast(StringType)) { (field, otherwise) =>
          when(col(field).isNull, lit(field)).otherwise(otherwise)
        })
      val checked = if (orderingFields.isEmpty || !avroRecordsWithoutThisPath(spec, writeConfig)) {
        withMeta
      } else {
        withMeta.filter(requireOrderingValue(nullOrderingField, col(HoodieRecord.RECORD_KEY_METADATA_FIELD)))
      }
      checked.withColumn(DEDUP_RANK_COL, row_number().over(window))
        .filter(col(DEDUP_RANK_COL) === 1)
        .drop(DEDUP_RANK_COL, INPUT_ORDER_COL)
    }
    PreparedRows(rows, StructType(dataFields))
  }

  /**
   * Tags incoming rows with their target file group: the record-level-index strategy when the
   * index is configured and its metadata partition exists, else the simple-index-style join
   * against the latest base files' meta columns. Inserts come back with a null file id. Rows
   * that carry their location are tagged already, and inserts and overwrites never tag.
   */
  private def tagRecords(spark: org.apache.spark.sql.SparkSession,
                         prepared: Dataset[Row],
                         spec: DataFrameWriteSpec,
                         writeConfig: HoodieWriteConfig,
                         table: HoodieTable[_, _, _, _],
                         engineContext: HoodieEngineContext): Dataset[Row] = {
    val operation = spec.operation
    val indexType = writeConfig.getIndexType
    val recordIndexConfigured = indexType == HoodieIndex.IndexType.RECORD_INDEX ||
      indexType == HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX
    // The record index of a table is either global or kept per data partition.
    val partitionedRecordIndex = indexType == HoodieIndex.IndexType.RECORD_LEVEL_INDEX
    val recordIndexReady = (recordIndexConfigured || partitionedRecordIndex) &&
      table.getMetaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)
    val globalIndex = recordIndexConfigured || indexType == HoodieIndex.IndexType.GLOBAL_SIMPLE ||
      indexType == HoodieIndex.IndexType.GLOBAL_BLOOM
    val deleteMarker: Column = if (operation == WriteOperationType.DELETE) {
      lit(true)
    } else {
      deleteMarkerOf(prepared, writeConfig, table.getMetaClient.getTableConfig).getOrElse(lit(false))
    }

    // Log blocks record the positions of the rows they update in the base file.
    val withPositions = table.getMetaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ &&
      writeConfig.shouldWriteRecordPositions
    def located: Dataset[Row] = if (recordIndexReady) {
      log.info("Tagging incoming records via the record-level index")
      tagViaRecordIndex(spark, prepared, table, engineContext, writeConfig, partitionedRecordIndex)
    } else {
      // A record index that is configured but not built yet tags through the base-file join,
      // with the semantics of the index: across the table for a global one.
      tagViaSimpleJoin(spark, prepared, table, writeConfig, globalIndex, withPositions)
    }
    val duplicatePolicy = insertDuplicatePolicy(spec)

    withLocationDetail(if (spec.locationFromMetaColumns) {
      resolveTagged(prepared, globalIndex = false, updatePartitionPath = false, deleteMarker,
        deletesByOperation = operation == WriteOperationType.DELETE)
    } else if (operation == WriteOperationType.INSERT && duplicatePolicy != DataSourceWriteOptions.NONE_INSERT_DUP_POLICY) {
      val candidates = located.filter(!deleteMarker)
      if (duplicatePolicy == DataSourceWriteOptions.FAIL_INSERT_DUP_POLICY) {
        // A key the table already has fails the insert, before anything is written. The check is
        // not deterministic to the optimizer, which would otherwise move it across the join.
        val failOnDuplicate = udf((recordKey: String) => {
          if (recordKey != null) {
            throw new HoodieDuplicateKeyException(recordKey)
          }
          true
        }).asNondeterministic()
        candidates.filter(failOnDuplicate(when(col(TAGGED_FILE_ID_COL).isNotNull, col(HoodieRecord.RECORD_KEY_METADATA_FIELD))))
          .foreach(_ => ())
      }
      // Keys the table already has are dropped from the insert.
      val kept = candidates.filter(col(TAGGED_FILE_ID_COL).isNull)
      (if (globalIndex) kept.drop(LOCATED_PARTITION_COL) else kept)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .withColumn(ROW_OP_COL, lit(ROW_OP_UPSERT))
    } else if (operation != WriteOperationType.UPSERT && operation != WriteOperationType.DELETE) {
      // Inserts never tag. Where the handles merge the inserts of a small file group by key, a
      // delete-marker row removes the record of its key there; anywhere else it has nothing to
      // delete.
      (if (writeConfig.allowDuplicateInserts) prepared.filter(!deleteMarker) else prepared)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .withColumn(ROW_OP_COL, lit(ROW_OP_UPSERT))
    } else {
      val joined = located
      // A delete goes to wherever the key is; only an upsert can move a key to a new partition.
      val updatePartitionPath = globalIndex && operation == WriteOperationType.UPSERT && (if (recordIndexConfigured) {
        writeConfig.getRecordIndexUpdatePartitionPath
      } else if (indexType == HoodieIndex.IndexType.GLOBAL_BLOOM) {
        writeConfig.getGlobalBloomIndexUpdatePartitionPath
      } else {
        writeConfig.getGlobalSimpleIndexUpdatePartitionPath
      })
      val storedOrderingFields = if (writeConfig.getRecordMergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
        table.getMetaClient.getTableConfig.getOrderingFields.asScala.toSeq
      } else {
        Seq.empty
      }
      // These indexes keep an entry per stored key, which a delete in a log file takes away.
      val settleDeletes = operation == WriteOperationType.UPSERT &&
        table.getMetaClient.getTableConfig.getTableType == HoodieTableType.MERGE_ON_READ &&
        (globalIndex || indexType == HoodieIndex.IndexType.RECORD_LEVEL_INDEX)
      resolveTagged(joined, globalIndex, updatePartitionPath, deleteMarker,
        deletesByOperation = operation == WriteOperationType.DELETE, settleDeletes, writeConfig.getBasePath, storedOrderingFields)
    })
  }

  /**
   * Makes sure the tagged frame carries the instant of the file slice a row is located in and
   * the row's position in its base file, null where the tagging does not know them.
   */
  private def withLocationDetail(tagged: Dataset[Row]): Dataset[Row] = {
    val withInstant = if (tagged.columns.contains(TAGGED_INSTANT_COL)) {
      tagged
    } else {
      tagged.withColumn(TAGGED_INSTANT_COL, lit(null).cast(StringType))
    }
    if (withInstant.columns.contains(TAGGED_POSITION_COL)) {
      withInstant
    } else {
      withInstant.withColumn(TAGGED_POSITION_COL, lit(null).cast(LongType))
    }
  }

  /**
   * Applies global-index semantics and the row-operation column on the tagged frame: deletes
   * of keys that are not located are dropped; with update-partition-path off, located rows route
   * to their original partition; with it on, a key that moved partitions fans out into a delete
   * against the old file group plus an insert into the new partition.
   *
   * <p>Under event-time ordering two kinds of rows are settled here against the stored row of
   * their key, which is read from the table, and are dropped when their ordering value is behind
   * it: a row that moves its key to another partition, and, with `settleDeletes`, a located row
   * that deletes by its content. A delete appended to a log is taken as one by the indexes that
   * are kept from the write, so it must not get there when the stored row outlives it.
   */
  private def resolveTagged(joined: Dataset[Row],
                            globalIndex: Boolean,
                            updatePartitionPath: Boolean,
                            deleteMarker: Column,
                            deletesByOperation: Boolean,
                            settleDeletes: Boolean = false,
                            basePath: String = null,
                            orderingFields: Seq[String] = Seq.empty): Dataset[Row] = {
    val located = col(TAGGED_FILE_ID_COL).isNotNull
    // A row that deletes by its content keeps the upsert operation: whether it deletes is for the
    // write handle to find out from the record.
    val base = joined
      .withColumn(DELETES_COL, deleteMarker)
      .withColumn(ROW_OP_COL, lit(if (deletesByOperation) ROW_OP_DELETE else ROW_OP_UPSERT))
      .filter(!col(DELETES_COL) || located)
    val movesPartitions = globalIndex && updatePartitionPath
    val settled = if (orderingFields.isEmpty || !(movesPartitions || settleDeletes)) {
      base
    } else {
      val moving = if (movesPartitions) located && movedPartitions else lit(false)
      val deleting = if (settleDeletes) located && col(DELETES_COL) else lit(false)
      val storedPartition = if (globalIndex) {
        coalesce(col(LOCATED_PARTITION_COL), col(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
      } else {
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
      }
      base.filter(!(moving || deleting))
        .unionByName(notBehindStored(base.filter(moving || deleting), storedPartition, basePath, orderingFields))
    }
    resolveLocated(settled, globalIndex, updatePartitionPath).drop(DELETES_COL)
  }

  /** Whether a row is located in a partition other than the one it comes with. */
  private def movedPartitions: Column =
    col(LOCATED_PARTITION_COL).isNotNull && col(LOCATED_PARTITION_COL) =!= col(HoodieRecord.PARTITION_PATH_METADATA_FIELD)

  /**
   * The rows whose ordering value is not behind that of the stored row of their key in
   * `storedPartition`. A row without a stored row stays, an equal ordering value lets the
   * incoming row win, and so does a delete that carries no ordering value.
   */
  private def notBehindStored(rows: Dataset[Row],
                              storedPartition: Column,
                              basePath: String,
                              orderingFields: Seq[String]): Dataset[Row] = {
    // A table without a completed commit reads back without columns and has no stored rows.
    lazy val storedTable = rows.sparkSession.read.format("hudi").load(basePath)
    val comparedFields = orderingFields.filter(resolves(rows, _))
      .filter(field => resolves(storedTable, HoodieRecord.RECORD_KEY_METADATA_FIELD) && resolves(storedTable, field))
    if (comparedFields.isEmpty) {
      rows
    } else {
      val stored = storedTable.select(
        (Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as(STORED_KEY_COL),
          col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(STORED_PARTITION_COL))
          ++ comparedFields.zipWithIndex.map { case (field, i) => col(field).as(STORED_ORDERING_COL_PREFIX + i) }): _*)
      // Lexicographic over the ordering fields.
      val notBehind = comparedFields.zipWithIndex.foldRight(lit(true)) { case ((field, i), rest) =>
        val storedValue = col(STORED_ORDERING_COL_PREFIX + i)
        col(field) > storedValue || (col(field) <=> storedValue && rest)
      }
      val deletesWithoutOrdering = col(DELETES_COL) && comparedFields.map(col(_).isNull).reduce(_ && _)
      rows
        .join(stored, col(HoodieRecord.RECORD_KEY_METADATA_FIELD) === col(STORED_KEY_COL)
          && storedPartition === col(STORED_PARTITION_COL), "left_outer")
        .filter(col(STORED_KEY_COL).isNull || deletesWithoutOrdering || notBehind)
        .drop((Seq(STORED_KEY_COL, STORED_PARTITION_COL)
          ++ comparedFields.indices.map(STORED_ORDERING_COL_PREFIX + _)): _*)
    }
  }

  private def resolveLocated(base: Dataset[Row], globalIndex: Boolean, updatePartitionPath: Boolean): Dataset[Row] = {
    if (!globalIndex) {
      base
    } else if (!updatePartitionPath) {
      base.withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
          coalesce(col(LOCATED_PARTITION_COL), col(HoodieRecord.PARTITION_PATH_METADATA_FIELD)))
        .drop(LOCATED_PARTITION_COL)
    } else {
      val moved = col(TAGGED_FILE_ID_COL).isNotNull && movedPartitions
      val staying = base.filter(!moved).drop(LOCATED_PARTITION_COL)
      val moving = base.filter(moved)
      val oldCopyDeletes = moving
        .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, col(LOCATED_PARTITION_COL))
        .withColumn(ROW_OP_COL, lit(ROW_OP_DELETE))
        .drop(LOCATED_PARTITION_COL)
      val newPartitionInserts = moving.filter(!col(DELETES_COL))
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .drop(LOCATED_PARTITION_COL)
      staying.unionByName(oldCopyDeletes).unionByName(newPartitionInserts)
    }
  }

  /**
   * Record-level-index tagging: only thin record keys shuffle, partitioned the way the record
   * index is sharded across its file groups, so each Spark partition does point lookups against
   * exactly one index shard. Located keys join back to the input rows; the index never
   * shuffles. A global index locates a key wherever it is stored and the row keeps that
   * partition (the update-partition-path semantics of the global record index with the default
   * config); an index kept per data partition locates a key in the partition the row comes with.
   */
  private def tagViaRecordIndex(spark: org.apache.spark.sql.SparkSession,
                                prepared: Dataset[Row],
                                table: HoodieTable[_, _, _, _],
                                engineContext: HoodieEngineContext,
                                writeConfig: HoodieWriteConfig,
                                partitioned: Boolean): Dataset[Row] = {
    val tableMetadata = table.getTableMetadata
    // Resolved on the driver, from the context of the client that commits, so the closure
    // carries the counters to executors and the commit publishes them.
    val lookupMetrics = RecordIndexLookupMetrics.resolveRegistry(engineContext, writeConfig)

    val located: RDD[(String, HoodieRecordGlobalLocation)] = if (partitioned) {
      val fileGroupCounts = new java.util.HashMap[String, Integer]()
      tableMetadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX).asScala
        .foreach { case (partition, fileGroups) => fileGroupCounts.put(partition, fileGroups.size()) }
      val shardOffsets = new java.util.HashMap[String, Integer](
        BucketizedMetadataTableFileGroupIndexParser.generatePartitionToBaseIndexOffsets(fileGroupCounts))
      val numShards = BucketizedMetadataTableFileGroupIndexParser.calculateNumberOfFileGroups(fileGroupCounts)
      val keysByShard = prepared
        .select(col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
        .queryExecution.toRdd
        .map(row => (row.getString(0), row.getString(1)))
        // A partition the index has no file groups for holds no located key.
        .filter { case (partition, _) => shardOffsets.containsKey(partition) }
        .map { case (partition, key) =>
          val shard = shardOffsets.get(partition) +
            HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, fileGroupCounts.get(partition))
          (shard, HoodiePair.of(partition, key))
        }
        .partitionBy(new ShardPartitioner(math.max(1, numShards)))
        .map(_._2)
      keysByShard.toJavaRDD()
        .mapPartitionsToPair(new PartitionedRecordIndexFileGroupLookupFunction(tableMetadata, lookupMetrics))
        .rdd
    } else {
      val numFileGroups = tableMetadata.getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX)
      val indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(
        MetadataPartitionType.RECORD_INDEX.getPartitionPath, table.getMetaClient)
      val mappingFunction = MetadataPartitionType
        .fromPartitionPath(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
        .getFileGroupMappingFunction(indexVersion)
      prepared.select(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
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
            val shardTimer = HoodieTimer.start()
            val locations = tableMetadata.readRecordIndexLocationsWithKeys(HoodieListData.eager[String](keyList))
            try {
              val found = HoodieDataUtils.dedupeAndCollectAsList(locations).asScala
              if (lookupMetrics.isPresent) {
                RecordIndexLookupMetrics.recordShardLookup(lookupMetrics.get, keyList,
                  found.map(_.getKey).toSet.asJava, shardTimer.endTimer())
              }
              found.map(pair => (pair.getKey, pair.getValue)).iterator
            } finally {
              locations.unpersistWithDependencies()
            }
          }
        }
    }
    val locationRows = located.map { case (key, location) =>
      val position: java.lang.Long =
        if (HoodieRecordLocation.isPositionValid(location.getPosition)) location.getPosition else null
      Row(key, location.getPartitionPath, location.getFileId, location.getInstantTime, position)
    }
    val locationSchema = StructType(Seq(
      StructField("_hoodie_lookup_key", StringType),
      StructField(LOCATED_PARTITION_COL, StringType),
      StructField(TAGGED_FILE_ID_COL, StringType),
      StructField(TAGGED_INSTANT_COL, StringType),
      StructField(TAGGED_POSITION_COL, LongType)))
    val locations = spark.createDataFrame(locationRows, locationSchema)
    val keyMatches = prepared(HoodieRecord.RECORD_KEY_METADATA_FIELD) === locations("_hoodie_lookup_key")
    if (partitioned) {
      prepared.join(locations,
          keyMatches && prepared(HoodieRecord.PARTITION_PATH_METADATA_FIELD) === locations(LOCATED_PARTITION_COL),
          "left_outer")
        .drop("_hoodie_lookup_key", LOCATED_PARTITION_COL)
    } else {
      prepared.join(locations, keyMatches, "left_outer").drop("_hoodie_lookup_key")
    }
  }

  /**
   * Simple-index-style tagging: read only the meta columns of the latest base files and left
   * join the input on (record key, partition path), producing the target file id column (null
   * for inserts).
   */
  private def tagViaSimpleJoin(spark: org.apache.spark.sql.SparkSession,
                               prepared: Dataset[Row],
                               table: HoodieTable[_, _, _, _],
                               writeConfig: HoodieWriteConfig,
                               globalIndex: Boolean,
                               withPositions: Boolean): Dataset[Row] = {
    // Asked partition by partition, so the view loads what it serves: a view behind the timeline
    // server drops the partitions it had loaded when another writer's commit makes it refresh.
    val fsView = table.getHoodieView
    val latestBaseFiles: Seq[(String, HoodieBaseFile)] = table.getTableMetadata.getAllPartitionPaths.asScala.toSeq
      .flatMap(partition => fsView.getLatestBaseFiles(partition).iterator().asScala.map(baseFile => (partition, baseFile)))
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
      val existing = if (withPositions && !HoodieSparkUtils.gteqSpark3_5) {
        keyLocationsOfBaseFiles(spark, table, writeConfig, latestBaseFiles)
      } else {
        keyLocationsFromFileSource(spark, latestBaseFiles.map(_._2.getPath), withPositions)
      }
      val keyMatches = prepared(HoodieRecord.RECORD_KEY_METADATA_FIELD) === existing(EXISTING_KEY_COL)
      val condition = if (globalIndex) {
        keyMatches
      } else {
        keyMatches && prepared(HoodieRecord.PARTITION_PATH_METADATA_FIELD) === existing(LOCATED_PARTITION_COL)
      }
      val joined = prepared.join(existing, condition, "left_outer").drop(EXISTING_KEY_COL)
      if (globalIndex) joined else joined.drop(LOCATED_PARTITION_COL)
    }
  }

  /** The keys of base files and where they are, read as the meta columns of the files. */
  private def keyLocationsFromFileSource(spark: org.apache.spark.sql.SparkSession,
                                         baseFilePaths: Seq[String],
                                         withPositions: Boolean): Dataset[Row] = {
    val fileIdFromName = udf((fileName: String) => FSUtils.getFileId(fileName))
    val instantFromPath = udf((filePath: String) => FSUtils.getCommitTime(new StoragePath(filePath).getName))
    // Only the meta columns are read, with their schema given: the other columns of a file need
    // not be ones the file source of every Spark version can infer.
    val baseFiles = spark.read.schema(KEY_LOCATION_SCHEMA).parquet(baseFilePaths: _*)
    // The row index is a hidden column of the file source, which not every read exposes.
    val position = if (withPositions && resolves(baseFiles, "_metadata.row_index")) {
      col("_metadata.row_index")
    } else {
      lit(null).cast(LongType)
    }
    baseFiles.select(
      col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as(EXISTING_KEY_COL),
      col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(LOCATED_PARTITION_COL),
      fileIdFromName(col(HoodieRecord.FILENAME_METADATA_FIELD)).as(TAGGED_FILE_ID_COL),
      instantFromPath(input_file_name()).as(TAGGED_INSTANT_COL),
      position.as(TAGGED_POSITION_COL))
  }

  /**
   * The keys of base files and where they are, with the position of each row in its file, read
   * the way the simple index reads them. Before Spark 3.5 the file source has no row index to
   * take the positions from.
   */
  private def keyLocationsOfBaseFiles(spark: org.apache.spark.sql.SparkSession,
                                      table: HoodieTable[_, _, _, _],
                                      writeConfig: HoodieWriteConfig,
                                      baseFiles: Seq[(String, HoodieBaseFile)]): Dataset[Row] = {
    val serTable = table.asInstanceOf[HoodieTable[AnyRef, AnyRef, AnyRef, AnyRef]]
    val parallelism = if (writeConfig.getSimpleIndexParallelism > 0) {
      math.min(baseFiles.size, writeConfig.getSimpleIndexParallelism)
    } else {
      baseFiles.size
    }
    val locationRows = spark.sparkContext.parallelize(baseFiles, parallelism).flatMap { case (partition, baseFile) =>
      val locations = new HoodieKeyLocationFetchHandle[AnyRef, AnyRef, AnyRef, AnyRef](
        writeConfig, serTable, HoodiePair.of(partition, baseFile), HOption.empty[BaseKeyGenerator]()).locations()
      new Iterator[Row] {
        override def hasNext: Boolean = {
          val more = locations.hasNext
          if (!more) {
            locations.close()
          }
          more
        }

        override def next(): Row = {
          val keyAndLocation = locations.next()
          val location = keyAndLocation.getRight
          val position: java.lang.Long =
            if (HoodieRecordLocation.isPositionValid(location.getPosition)) location.getPosition else null
          Row(keyAndLocation.getLeft.getRecordKey, keyAndLocation.getLeft.getPartitionPath, location.getFileId,
            location.getInstantTime, position)
        }
      }
    }
    spark.createDataFrame(locationRows, StructType(Seq(
      StructField(EXISTING_KEY_COL, StringType),
      StructField(LOCATED_PARTITION_COL, StringType),
      StructField(TAGGED_FILE_ID_COL, StringType),
      StructField(TAGGED_INSTANT_COL, StringType),
      StructField(TAGGED_POSITION_COL, LongType))))
  }

  /**
   * Adds the routing-bucket column. Tagged updates pin to their file group. Inserts hash across
   * their partition's slots: the small file groups that take inserts, whose buckets merge, or
   * else the new-file buckets. The tagged updates of a small file group that takes inserts go
   * to its padding bucket too. The profiler mode decides how buckets then map to write tasks.
   */
  private def withRoutingBucket(tagged: Dataset[Row],
                                insertPlan: InsertRoutingPlan,
                                colocateUpdatesWithPadding: Boolean): Dataset[Row] = {
    val smallFileSlots = insertPlan.smallFileSlotsByPartition
    val paddingBuckets: Set[String] = if (colocateUpdatesWithPadding) smallFileSlots.values.flatten.toSet else Set.empty
    val paddingPrefix = smallFileSlots.values.flatten.headOption.map(_.substring(0, 2)).getOrElse(UPDATE_BUCKET_PREFIX)
    val newFileBuckets = insertPlan.newFileBuckets
    val routingBucket = udf { (partitionPath: String, taggedFileId: String, keyHash: Int) =>
      if (taggedFileId != null) {
        val paddingBucket = paddingPrefix + taggedFileId
        if (paddingBuckets.contains(paddingBucket)) paddingBucket else UPDATE_BUCKET_PREFIX + taggedFileId
      } else {
        // Padding-first: without a count pass there is no record budget per small file group, so
        // a partition's inserts fill its small file groups before any new one opens.
        val padding = smallFileSlots.getOrElse(partitionPath, Array.empty[String])
        val totalSlots = if (padding.nonEmpty) padding.length else newFileBuckets
        val slot = ((keyHash % totalSlots) + totalSlots) % totalSlots
        if (padding.nonEmpty) {
          padding(slot)
        } else {
          INSERT_BUCKET_PREFIX + partitionPath + ":" + "%010d".format(slot)
        }
      }
    }
    tagged.withColumn(ROUTING_BUCKET_COL, routingBucket(
      col(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
      col(TAGGED_FILE_ID_COL),
      hash(col(HoodieRecord.RECORD_KEY_METADATA_FIELD))))
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

object HoodieDataFrameWriter {

  private val PARQUET_REBASE_MODE_IN_WRITE = "spark.sql.parquet.datetimeRebaseModeInWrite"
  private val PARQUET_INT96_REBASE_MODE_IN_WRITE = "spark.sql.parquet.int96RebaseModeInWrite"
  val INSERT_BUCKETS_PER_PARTITION = "hoodie.datasource.write.dataframe.insert.buckets"
  val WRITE_TASKS = "hoodie.datasource.write.dataframe.write.tasks"
  val PROFILER = "hoodie.datasource.write.dataframe.profiler"
  val PROFILER_METADATA = "metadata"
  val PROFILER_SHUFFLE_STATS = "shuffle_stats"
  val SHUFFLE_UPDATE_REDUCERS = "hoodie.datasource.write.dataframe.shuffle.update.reducers"
  val SHUFFLE_INSERT_REDUCERS = "hoodie.datasource.write.dataframe.shuffle.insert.reducers"
  val SHUFFLE_BYTES_METADATA_KEY = "hoodie.dataframe.shuffle.bytes"
  private val DEFAULT_SHUFFLE_SIZE_RATIO = 3.0d

  private val KEYS_COL = "_hoodie_keys"
  private val DEDUP_RANK_COL = "_hoodie_dedup_rank"
  private val INPUT_ORDER_COL = "_hoodie_input_order"
  private val DELETES_COL = "_hoodie_deletes"
  private val STORED_KEY_COL = "_hoodie_stored_key"
  private val STORED_PARTITION_COL = "_hoodie_stored_partition"
  private val STORED_ORDERING_COL_PREFIX = "_hoodie_stored_ordering_"
  private val TAGGED_FILE_ID_COL = "_hoodie_tagged_file_id"
  private val TAGGED_INSTANT_COL = "_hoodie_tagged_instant"
  private val TAGGED_POSITION_COL = "_hoodie_tagged_position"
  private val LOCATED_PARTITION_COL = "_hoodie_located_partition"
  private val EXISTING_KEY_COL = "_hoodie_existing_key"
  private val ROUTING_BUCKET_COL = "_hoodie_routing_bucket"
  val ROW_OP_COL = "_hoodie_row_op"
  val ROW_OP_UPSERT = "U"
  val ROW_OP_DELETE = "D"
  private val DELETE_MARKER_COL = HoodieRecord.HOODIE_IS_DELETED_FIELD
  private val KEY_LOCATION_SCHEMA = StructType(Seq(
    HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    .map(StructField(_, StringType)))
  val UPDATE_BUCKET_PREFIX = "u:"
  val MERGE_BUCKET_PREFIX = "s:"
  val INSERT_BUCKET_PREFIX = "i:"

  private val SUPPORTED_INDEX_TYPES: Set[HoodieIndex.IndexType] = Set(
    HoodieIndex.IndexType.SIMPLE,
    HoodieIndex.IndexType.GLOBAL_SIMPLE,
    HoodieIndex.IndexType.GLOBAL_BLOOM,
    HoodieIndex.IndexType.BLOOM,
    HoodieIndex.IndexType.RECORD_INDEX,
    HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX,
    HoodieIndex.IndexType.RECORD_LEVEL_INDEX)

  private val SUPPORTED_OPERATIONS: Set[WriteOperationType] = Set(
    WriteOperationType.INSERT,
    WriteOperationType.UPSERT,
    WriteOperationType.DELETE,
    WriteOperationType.DELETE_PARTITION,
    WriteOperationType.INSERT_OVERWRITE,
    WriteOperationType.INSERT_OVERWRITE_TABLE)

  /** The prepared rows, and the schema of the data columns they carry after the meta columns. */
  private case class PreparedRows(rows: DataFrame, dataSchema: StructType)

  /** Number of writes this JVM ran on the DataFrame write path. */
  val writeCount = new AtomicLong()

  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"
}

/**
 * Passes through pre-computed shard ids as Spark partition ids, aligning the thin-key shuffle
 * with the record index's own file-group hashing (Spark's Murmur3 partitioning does not).
 */
private class ShardPartitioner(shards: Int) extends Partitioner {
  override def numPartitions: Int = shards
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
