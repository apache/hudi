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
import org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy
import org.apache.hudi.client.utils.SparkValidatorUtils
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieFileFormat, HoodieFileGroupId, HoodieRecord, HoodieTableType, MetaFieldsMode, OverwriteWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{CommitUtils, ConfigUtils, HoodieDataUtils, HoodieRecordUtils, Option => HOption, StringUtils}
import org.apache.hudi.config.{HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.exception.{HoodieClusteringUpdateException, HoodieDuplicateKeyException}
import org.apache.hudi.index.{HoodieIndex, SparkHoodieIndexFactory}
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.keygen.constant.{KeyGeneratorOptions, KeyGeneratorType}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.metadata.{HoodieIndexVersion, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.HoodieWriteMetadata
import org.apache.hudi.util.CommonClientUtils

import org.apache.spark.Partitioner
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, HoodieInternalRowUtils, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, hash, lit, monotonically_increasing_id, row_number, struct, udf, when}
import org.apache.spark.sql.hudi.command.SqlKeyGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
    def enabled(key: String): Boolean = params.getOrElse(key, "false").toBoolean
    val checks: Seq[(() => Boolean, String)] = Seq(
      (() => !SUPPORTED_OPERATIONS.contains(spec.operation), s"operation ${spec.operation.value}"),
      (() => StringUtils.nonEmpty(writeConfig.getIndexClass), "custom index classes"),
      (() => !SUPPORTED_INDEX_TYPES.contains(writeConfig.getIndexType), s"index type ${writeConfig.getIndexType}"),
      (() => tableConfig.getMetaFieldsMode != MetaFieldsMode.ALL || writeConfig.getMetaFieldsMode != MetaFieldsMode.ALL,
        "tables that do not populate all meta fields"),
      (() => tableConfig.shouldDropPartitionColumns() || enabled(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key),
        "dropped partition columns"),
      (() => tableConfig.isCDCEnabled, "CDC tables"),
      (() => tableConfig.isLSMTreeStorageLayout, "the LSM tree storage layout"),
      (() => tableConfig.getBaseFileFormat != HoodieFileFormat.PARQUET || tableConfig.isMultipleBaseFileFormatsEnabled,
        s"base file format ${tableConfig.getBaseFileFormat}"),
      (() => tableConfig.getBootstrapBasePath.isPresent, "bootstrapped tables"),
      (() => (isCustomMerge(writeConfig, tableConfig) || payloadDecides(writeConfig))
        && SparkHoodieIndexFactory.isGlobalIndex(writeConfig), "custom merging under a global index"),
      (() => combinesInTask(spec, writeConfig, tableConfig)
        && params.getOrElse(PROFILER, PROFILER_METADATA) == PROFILER_SHUFFLE_STATS,
        "custom merging that combines records with the shuffle-stats profiler"),
      (() => writeConfig.allowOperationMetadataField, "the operation meta field"),
      (() => writeConfig.isRollbackPendingClustering
        || writeConfig.getClusteringUpdatesStrategyClass != classOf[SparkRejectUpdateStrategy[_]].getName,
        "clustering update strategies other than rejecting the update"),
      (() => writeConfig.getRecordMerger.getRecordType == HoodieRecordType.SPARK
        && mergeProps(writeConfig, tableConfig).containsKey(DefaultHoodieRecordPayload.DELETE_KEY),
        "custom delete markers on engine-native records"))
    checks.collectFirst { case (applies, what) if applies() => s"$what are not supported by it yet" }
  }

  override def clientOptions(spec: DataFrameWriteSpec): Map[String, String] = {
    val params = spec.parameters
    if (params.contains(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY)) {
      // A merger the user set decides the record type.
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
   * column stats index predates the version that covers every type for them, and where rows
   * cannot be rewritten to the writer schema as rows.
   */
  private def canWriteSparkRecords(spec: DataFrameWriteSpec, writeConfig: HoodieWriteConfig): Boolean = {
    val tableConfig = spec.metaClient.getTableConfig
    val avroLogBlocks = tableConfig.getTableType == HoodieTableType.MERGE_ON_READ &&
      CommonClientUtils.getLogBlockType(writeConfig, tableConfig) != HoodieLogBlockType.PARQUET_DATA_BLOCK
    val columnStatsByRecordType = HoodieTableMetadataUtil.existingIndexVersionOrDefault(
      MetadataPartitionType.COLUMN_STATS.getPartitionPath, spec.metaClient).lowerThan(HoodieIndexVersion.V2)
    !isCustomMerge(writeConfig, tableConfig) && !payloadDecides(writeConfig) && !avroLogBlocks &&
      !columnStatsByRecordType && rowsConformToWriterSchema(spec)
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
    val writerSchema = org.apache.hudi.common.schema.HoodieSchemaUtils.removeMetadataFields(
      spec.writerSchema.getOrElse(HoodieSchema.parse(writeConfig.getSchema)))

    // A micro-batch of the streaming sink only allows its physical plan to be executed, so the
    // plan below is built over its rows.
    val input = if (df.isStreaming) {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(spark, df.queryExecution.toRdd, df.schema)
    } else {
      df
    }

    // Stage 1 + 2: key generation, meta columns, and upfront precombine as pure projections.
    val prepared = prepareInput(input, spec, writeConfig, table.getMetaClient, instantTime)

    // Stage 3: index tagging (record-level-index point lookup or simple-index join), unless
    // the rows already carry their file group.
    val tagged = tagRecords(spark, prepared.rows, spec, writeConfig, table)

    // Stage 4: routing on a bucket column; updates pin to their file group, inserts go to the
    // small file group their partition pads or hash across new-file buckets, as planned from
    // table metadata. An overwrite replaces the file groups it would pad, so it only opens new ones.
    val newFileBuckets = params.get(INSERT_BUCKETS_PER_PARTITION).map(_.toInt).getOrElse(1)
    val insertPlan = MetadataSizingProfiler.plan(table, writeConfig, newFileBuckets, mergeOnRead,
      if (mergeOnRead) MERGE_BUCKET_PREFIX else UPDATE_BUCKET_PREFIX, padSmallFiles = !overwrite)
    val withBucket = withRoutingBucket(tagged, insertPlan)
    val bucketOrdinal = withBucket.schema.fieldIndex(ROUTING_BUCKET_COL)

    val actionType = CommitUtils.getCommitActionType(operation, tableConfig.getTableType)
    table.getActiveTimeline.transitionRequestedToInflight(
      table.getMetaClient.createNewInstant(HoodieInstant.State.REQUESTED, actionType, instantTime),
      HOption.empty())

    // Stage 5: per-bucket write tasks driving the write handles. The reader context factory is
    // created on the driver (the table's engine context is not usable on executors) and shipped
    // with the task.
    val profilerMode = params.getOrElse(PROFILER, PROFILER_METADATA)
    val sparkTable = table.asInstanceOf[HoodieTable[InternalRow, _, _, _]]
    val readerContextFactory = sparkTable.getReaderContextFactoryForWrite
    val pendingClusteringFileGroups: Set[HoodieFileGroupId] =
      table.getFileSystemView.getFileGroupsInPendingClustering.iterator().asScala.map(_.getKey).toSet
    val task = new DataFrameWriteTask(
      writeConfig, sparkTable, readerContextFactory, instantTime, operation, withBucket.schema,
      prepared.dataSchema.length, HoodieInternalRowUtils.getCachedSchema(writerSchema), bucketOrdinal,
      withBucket.schema.fieldIndex(ROW_OP_COL), withBucket.schema.fieldIndex(TAGGED_FILE_ID_COL), mergeOnRead,
      // The shuffle-stats profiler orders rows by bucket alone, so it pads without a budget.
      if (profilerMode == PROFILER_SHUFFLE_STATS) Map.empty else insertPlan.paddingByPartition.values.map(slot => slot.bucket -> slot.recordBudget).toMap,
      insertPlan.recordsPerNewFile, pendingClusteringFileGroups,
      avroRecordSpec(spec, prepared.dataSchema, writerSchema, writeConfig, tableConfig),
      // The row-based create handle takes rows in the writer schema and neither consults a
      // payload class nor tracks the event-time watermark.
      recordBasedInserts = payloadDecides(writeConfig) || !rowsConformToWriterSchema(spec) ||
        (ConfigUtils.isTrackingEventTimeWatermark(writeConfig.getProps) && ConfigUtils.getEventTimeFieldName(writeConfig.getProps) != null),
      combineRecords = combinesInTask(spec, writeConfig, tableConfig))

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
      // Within a bucket, tagged updates come before the inserts that pad the file group, so the
      // write task can stop taking inserts once the file group is full. Combining in the write
      // task also needs the records of a key next to each other.
      val sortColumns = Seq(col(ROUTING_BUCKET_COL), col(TAGGED_FILE_ID_COL).isNull) ++
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
    val replacedFileIds = partitionToReplacedFileIds(operation, javaRdd, table, writeConfig, pendingClusteringFileGroups)
    if (StringUtils.nonEmpty(writeConfig.getPreCommitValidators)) {
      val writeMetadata = new HoodieWriteMetadata[HoodieData[WriteStatus]]()
      writeMetadata.setWriteStatuses(HoodieJavaRDD.of(javaRdd))
      writeMetadata.setPartitionToReplaceFileIds(replacedFileIds)
      SparkValidatorUtils.runValidators(writeConfig, writeMetadata, engineContext, table, instantTime)
    }
    DataFrameWriteResult(new HoodieWriteResult(javaRdd, replacedFileIds), extraMetadata)
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
          DataSourceWriteOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue).toBoolean,
        mergeProps = mergeProps(writeConfig, tableConfig)))
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

  private def isCustomMerge(writeConfig: HoodieWriteConfig, tableConfig: HoodieTableConfig): Boolean =
    writeConfig.getRecordMergeMode == RecordMergeMode.CUSTOM || tableConfig.getRecordMergeMode == RecordMergeMode.CUSTOM

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
    !spec.keysFromMetaColumns && tableConfig.getRecordKeyFields.isPresent && (spec.operation match {
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
    val avroRecordsWithoutThisPath = !spec.parameters.contains(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY) ||
      writeConfig.getRecordMerger.getRecordType == HoodieRecordType.AVRO
    val keyFields = (props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "").split(",") ++
      props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "").split(",").map(_.split(":")(0)))
      .map(_.trim).filter(_.nonEmpty)
    (userProvided && avroRecordsWithoutThisPath) || !keyFields.forall(resolves(input, _))
  }

  private def resolves(frame: DataFrame, field: String): Boolean =
    scala.util.Try(frame.select(col(field))).isSuccess

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
    val dataFields = if (isDelete) {
      Array.empty[StructField]
    } else if (carriesMetaColumns) {
      input.schema.fields.filterNot(field => metaColumns.contains(field.name))
    } else {
      input.schema.fields
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
        fileIdFromName(col(HoodieRecord.FILENAME_METADATA_FIELD))).as(TAGGED_FILE_ID_COL))
    } else {
      Seq.empty
    }
    val combineByWindow = shouldCombine(spec, writeConfig, tableConfig) && !combinesInTask(spec, writeConfig, tableConfig)
    val inputOrder = if (combineByWindow) Seq(monotonically_increasing_id().as(INPUT_ORDER_COL)) else Seq.empty
    val keyed = input.select((keys.as(KEYS_COL) +: (dataColumns ++ location ++ inputOrder)): _*)
    val withMeta = keyed.select(
      (Seq(
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
        col(KEYS_COL).getField(HoodieKeyGeneratorExpression.RECORD_KEY_FIELD).as(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        col(KEYS_COL).getField(HoodieKeyGeneratorExpression.PARTITION_PATH_FIELD)
          .as(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        lit(null).cast(StringType).as(HoodieRecord.FILENAME_METADATA_FIELD))
        ++ dataColumns ++ location.map(_ => col(TAGGED_FILE_ID_COL)) ++ inputOrder.map(_ => col(INPUT_ORDER_COL))): _*)

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
      // A record that is combined on its ordering value has to have one.
      val requireOrderingValue = udf((field: String, recordKey: String) => {
        if (field != null) {
          throw new IllegalArgumentException(
            s"Ordering field '$field' has null value for record key '$recordKey'. "
              + "Please ensure all records have non-null values for the ordering field, "
              + "or use a payload class that doesn't require ordering (e.g., OverwriteWithLatestAvroPayload).")
        }
        true
      })
      val nullOrderingField = missingOrderingFields.headOption.map(lit(_)).getOrElse(
        presentOrderingFields.foldRight(lit(null).cast(StringType)) { (field, otherwise) =>
          when(col(field).isNull, lit(field)).otherwise(otherwise)
        })
      val checked = if (orderingFields.isEmpty) {
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
                         table: HoodieTable[_, _, _, _]): Dataset[Row] = {
    val operation = spec.operation
    val indexType = writeConfig.getIndexType
    val recordIndexConfigured = indexType == HoodieIndex.IndexType.RECORD_INDEX ||
      indexType == HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX
    val recordIndexReady = recordIndexConfigured &&
      table.getMetaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)
    val globalIndex = recordIndexConfigured || indexType == HoodieIndex.IndexType.GLOBAL_SIMPLE
    val deleteMarker: Column = if (operation == WriteOperationType.DELETE) {
      lit(true)
    } else if (prepared.columns.contains(DELETE_MARKER_COL)) {
      coalesce(col(DELETE_MARKER_COL), lit(false))
    } else {
      lit(false)
    }

    def located: Dataset[Row] = if (recordIndexReady) {
      log.info("Tagging incoming records via the record-level index")
      tagViaRecordIndex(spark, prepared, table)
    } else {
      // A configured-but-uninitialized record index keeps global semantics through the
      // base-file join, mirroring the legacy fallback to the global simple index.
      tagViaSimpleJoin(spark, prepared, table, globalIndex)
    }
    val duplicatePolicy = insertDuplicatePolicy(spec)

    if (spec.locationFromMetaColumns) {
      resolveTagged(prepared, globalIndex = false, updatePartitionPath = false, deleteMarker)
    } else if (operation == WriteOperationType.INSERT && duplicatePolicy != DataSourceWriteOptions.NONE_INSERT_DUP_POLICY) {
      // Keys the table already has are dropped from the insert, or fail it.
      val isNew: Column = if (duplicatePolicy == DataSourceWriteOptions.FAIL_INSERT_DUP_POLICY) {
        val failOnDuplicate = udf((recordKey: String) => {
          if (recordKey != null) {
            throw new HoodieDuplicateKeyException(recordKey)
          }
          true
        })
        failOnDuplicate(when(col(TAGGED_FILE_ID_COL).isNotNull, col(HoodieRecord.RECORD_KEY_METADATA_FIELD)))
      } else {
        col(TAGGED_FILE_ID_COL).isNull
      }
      val kept = located.filter(!deleteMarker).filter(isNew)
      (if (globalIndex) kept.drop(LOCATED_PARTITION_COL) else kept)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .withColumn(ROW_OP_COL, lit(ROW_OP_UPSERT))
    } else if (operation != WriteOperationType.UPSERT && operation != WriteOperationType.DELETE) {
      // Inserts never tag, and a delete-marker row has nothing to delete.
      prepared.filter(!deleteMarker)
        .withColumn(TAGGED_FILE_ID_COL, lit(null).cast(StringType))
        .withColumn(ROW_OP_COL, lit(ROW_OP_UPSERT))
    } else {
      val joined = located
      // A delete goes to wherever the key is; only an upsert can move a key to a new partition.
      val updatePartitionPath = globalIndex && operation == WriteOperationType.UPSERT && (if (recordIndexConfigured) {
        writeConfig.getRecordIndexUpdatePartitionPath
      } else {
        writeConfig.getGlobalSimpleIndexUpdatePartitionPath
      })
      val moveOrderingFields = if (writeConfig.getRecordMergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
        table.getMetaClient.getTableConfig.getOrderingFields.asScala.toSeq
      } else {
        Seq.empty
      }
      resolveTagged(joined, globalIndex, updatePartitionPath, deleteMarker, writeConfig.getBasePath, moveOrderingFields)
    }
  }

  /**
   * Applies global-index semantics and the row-operation column on the tagged frame: no-op
   * deletes are dropped; with update-partition-path off, located rows route to their original
   * partition; with it on, a key that moved partitions fans out into a delete against the old
   * file group plus an insert into the new partition. Under event-time ordering the move only
   * happens when the incoming row's ordering value is not behind the stored row's, which is
   * read from the table for the moved keys; otherwise the stored row stays where it is.
   */
  private def resolveTagged(joined: Dataset[Row],
                            globalIndex: Boolean,
                            updatePartitionPath: Boolean,
                            deleteMarker: Column,
                            basePath: String = null,
                            orderingFields: Seq[String] = Seq.empty): Dataset[Row] = {
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
      val comparedFields = orderingFields.filter(resolves(base, _))
      val moving = if (comparedFields.isEmpty) {
        base.filter(moved)
      } else {
        val stored = joined.sparkSession.read.format("hudi").load(basePath).select(
          (Seq(col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as(STORED_KEY_COL),
            col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as(STORED_PARTITION_COL))
            ++ comparedFields.zipWithIndex.map { case (field, i) => col(field).as(STORED_ORDERING_COL_PREFIX + i) }): _*)
        // Lexicographic over the ordering fields; an equal ordering value lets the incoming row win.
        val notBehind = comparedFields.zipWithIndex.foldRight(lit(true)) { case ((field, i), rest) =>
          val storedValue = col(STORED_ORDERING_COL_PREFIX + i)
          col(field) > storedValue || (col(field) <=> storedValue && rest)
        }
        base.filter(moved)
          .join(stored, col(HoodieRecord.RECORD_KEY_METADATA_FIELD) === col(STORED_KEY_COL)
            && col(LOCATED_PARTITION_COL) === col(STORED_PARTITION_COL), "left_outer")
          .filter(col(STORED_KEY_COL).isNull || notBehind)
          .drop((Seq(STORED_KEY_COL, STORED_PARTITION_COL)
            ++ comparedFields.indices.map(STORED_ORDERING_COL_PREFIX + _)): _*)
      }
      val oldCopyDeletes = moving
        .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, col(LOCATED_PARTITION_COL))
        .withColumn(ROW_OP_COL, lit(ROW_OP_DELETE))
        .drop(LOCATED_PARTITION_COL)
      val newPartitionInserts = moving.filter(col(ROW_OP_COL) === ROW_OP_UPSERT)
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
   * Adds the routing-bucket column. Tagged updates pin to their file group. Inserts of a
   * partition that pads a small file group go to that file group's bucket, which merges, and
   * tagged updates of the same file group join them there; other inserts hash across the
   * partition's new-file buckets. The profiler mode decides how buckets then map to write tasks.
   */
  private def withRoutingBucket(tagged: Dataset[Row],
                                insertPlan: InsertRoutingPlan): Dataset[Row] = {
    val padding = insertPlan.paddingByPartition
    val newFileBuckets = insertPlan.newFileBuckets
    val routingBucket = udf { (partitionPath: String, taggedFileId: String, keyHash: Int) =>
      val slot = padding.get(partitionPath)
      if (taggedFileId != null) {
        if (slot.exists(_.fileId == taggedFileId)) slot.get.bucket else UPDATE_BUCKET_PREFIX + taggedFileId
      } else if (slot.isDefined) {
        slot.get.bucket
      } else {
        val newFileBucket = ((keyHash % newFileBuckets) + newFileBuckets) % newFileBuckets
        INSERT_BUCKET_PREFIX + partitionPath + ":" + "%010d".format(newFileBucket)
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
  private val STORED_KEY_COL = "_hoodie_stored_key"
  private val STORED_PARTITION_COL = "_hoodie_stored_partition"
  private val STORED_ORDERING_COL_PREFIX = "_hoodie_stored_ordering_"
  private val TAGGED_FILE_ID_COL = "_hoodie_tagged_file_id"
  private val LOCATED_PARTITION_COL = "_hoodie_located_partition"
  private val ROUTING_BUCKET_COL = "_hoodie_routing_bucket"
  val ROW_OP_COL = "_hoodie_row_op"
  val ROW_OP_UPSERT = "U"
  val ROW_OP_DELETE = "D"
  private val DELETE_MARKER_COL = HoodieRecord.HOODIE_IS_DELETED_FIELD
  val UPDATE_BUCKET_PREFIX = "u:"
  val MERGE_BUCKET_PREFIX = "s:"
  val INSERT_BUCKET_PREFIX = "i:"

  private val SUPPORTED_INDEX_TYPES: Set[HoodieIndex.IndexType] = Set(
    HoodieIndex.IndexType.SIMPLE,
    HoodieIndex.IndexType.GLOBAL_SIMPLE,
    HoodieIndex.IndexType.BLOOM,
    HoodieIndex.IndexType.RECORD_INDEX,
    HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX,
    HoodieIndex.IndexType.RECORD_LEVEL_INDEX)

  private val SUPPORTED_OPERATIONS: Set[WriteOperationType] = Set(
    WriteOperationType.INSERT,
    WriteOperationType.UPSERT,
    WriteOperationType.DELETE,
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
