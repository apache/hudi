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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.HoodieBaseRelation.convertToAvroSchema
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.MergeOnReadSnapshotRelation.{createPartitionedFile, isProjectionCompatible}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieNotSupportedException
import org.apache.hudi.storage.StoragePath
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logFiles: List[HoodieLogFile]) extends HoodieFileSplit

case class MergeOnReadSnapshotRelation(override val sqlContext: SQLContext,
                                       override val optParams: Map[String, String],
                                       override val metaClient: HoodieTableMetaClient,
                                       private val globPaths: Seq[StoragePath],
                                       private val userSchema: Option[StructType],
                                       private val prunedDataSchema: Option[StructType] = None)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, globPaths, userSchema, prunedDataSchema) {

  override type Relation = MergeOnReadSnapshotRelation

  override def updatePrunedDataSchema(prunedSchema: StructType): MergeOnReadSnapshotRelation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override protected def shouldIncludeLogFiles(): Boolean = {
    true
  }

}

/**
 * Base implementation of the Merge-on-Read snapshot relation
 *
 * NOTE: Reason this is extracted as a standalone base class is such that both MOR
 *       Snapshot and Incremental relations could inherit from it while both being Scala
 *       case classes
 */
abstract class BaseMergeOnReadSnapshotRelation(sqlContext: SQLContext,
                                               optParams: Map[String, String],
                                               metaClient: HoodieTableMetaClient,
                                               globPaths: Seq[StoragePath],
                                               userSchema: Option[StructType],
                                               prunedDataSchema: Option[StructType])
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema) {

  override type FileSplit = HoodieMergeOnReadFileSplit
  override type Partition = HoodieDefaultFilePartition

  /**
   * NOTE: These are the fields that are required to properly fulfil Merge-on-Read (MOR)
   *       semantic:
   *
   *       <ol>
   *         <li>Primary key is required to make sure we're able to correlate records from the base
   *         file with the updated records from the delta-log file</li>
   *         <li>Pre-combine key is required to properly perform the combining (or merging) of the
   *         existing and updated records</li>
   *       </ol>
   *
   *       However, in cases when merging is NOT performed (for ex, if file-group only contains base
   *       files but no delta-log files, or if the query-type is equal to [["skip_merge"]]) neither
   *       of primary-key or pre-combine-key are required to be fetched from storage (unless requested
   *       by the query), therefore saving on throughput
   */
  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  /**
   * Determines whether relation's schema could be pruned by Spark's Optimizer
   */
  override def canPruneRelationSchema: Boolean =
    super.canPruneRelationSchema && isProjectionCompatible(tableState)

  protected override def composeRDD(partitions: Seq[HoodieDefaultFilePartition],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val requiredFilters = Seq.empty
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      config = jobConf,
      fileReaders = readers,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      tableState = tableState,
      mergeType = mergeType,
      partitions = partitions)
  }

  protected def createBaseFileReaders(tableSchema: HoodieTableSchema,
                                      requiredSchema: HoodieTableSchema,
                                      requestedColumns: Array[String],
                                      requiredFilters: Seq[Filter],
                                      optionalFilters: Seq[Filter] = Seq.empty): HoodieMergeOnReadBaseFileReaders = {
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumns(tableSchema, requiredSchema)

    val fullSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = dataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), internalSchemaOpt)
    )

    val requiredSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = requiredDataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
    )

    // Check whether fields required for merging were also requested to be fetched
    // by the query:
    //    - In case they were, there's no optimization we could apply here (we will have
    //    to fetch such fields)
    //    - In case they were not, we will provide 2 separate file-readers
    //        a) One which would be applied to file-groups w/ delta-logs (merging)
    //        b) One which would be applied to file-groups w/ no delta-logs or
    //           in case query-mode is skipping merging
    val mandatoryColumns = mandatoryFieldsForMerging.map(HoodieAvroUtils.getRootLevelFieldName)
    if (mandatoryColumns.forall(requestedColumns.contains)) {
      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReader
      )
    } else {
      val prunedRequiredSchema = {
        val unusedMandatoryColumnNames = mandatoryColumns.filterNot(requestedColumns.contains)
        val prunedStructSchema =
          StructType(requiredDataSchema.structTypeSchema.fields
            .filterNot(f => unusedMandatoryColumnNames.contains(f.name)))

        HoodieTableSchema(prunedStructSchema, convertToAvroSchema(prunedStructSchema, tableName).toString)
      }

      val requiredSchemaReaderSkipMerging = createBaseFileReader(
        spark = sqlContext.sparkSession,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        requiredDataSchema = prunedRequiredSchema,
        // This file-reader is only used in cases when no merging is performed, therefore it's safe to push
        // down these filters to the base file readers
        filters = requiredFilters ++ optionalFilters,
        options = optParams,
        // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
        //       to configure Parquet reader appropriately
        hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
      )

      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReaderSkipMerging
      )
    }
  }

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)

    if (globPaths.isEmpty) {
      val fileSlices = fileIndex.filterFileSlices(dataFilters, convertedPartitionFilters).flatMap(s => s._2)
      buildSplits(fileSlices)
    } else {
      val fileSlices = listLatestFileSlices(globPaths, partitionFilters, dataFilters)
      buildSplits(fileSlices)
    }
  }

  protected override def mergeSplitsToPartitions(splits: Seq[HoodieMergeOnReadFileSplit]): Seq[HoodieDefaultFilePartition] = {
    optParams.getOrElse(DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_MERGE_TYPE.key(), DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_MERGE_TYPE.defaultValue()) match {
      case DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_NOT_MERGE =>
        splits.zipWithIndex.map { case (split, index) =>
          HoodieDefaultFilePartition(index, Array(split))
        }
      case DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_FILE_SIZE_BASED_MERGE =>
        mergeSplitsToPartitionsByFileSize(splits)
      case DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_BUCKET_BASED_MERGE =>
        mergeSplitsToPartitionByBucket(splits)
    }
  }

  private def mergeSplitsToPartitionsByFileSize(splits: Seq[HoodieMergeOnReadFileSplit]): Seq[HoodieDefaultFilePartition] = {
    val maxPartitionBytes = sqlContext.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sqlContext.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxPartitionBytesFraction = optParams.get(DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_MERGE_MAX_PARTITION_SIZE_FRACTION.key())
      .map(_.toDouble).getOrElse(DataSourceReadOptions.MOR_SNAPSHOT_QUERY_SPLITS_MERGE_MAX_PARTITION_SIZE_FRACTION.defaultValue())
    val maxPartitionBytesForMor = (maxPartitionBytes * maxPartitionBytesFraction).toLong
    val partitions = new ArrayBuffer[HoodieDefaultFilePartition]
    val currentFiles = new ArrayBuffer[HoodieMergeOnReadFileSplit]
    var currentSize = 0L

    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition = HoodieDefaultFilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // TODO: Implement a better heuristic for merging splits into partitions
    def splitSize(split: HoodieMergeOnReadFileSplit): Long = {
      // Calculate the size of current split
      // 1. base file exist, use the size of base file
      // 2. base file not exist, use the size of the median log file sorted by log file size
      split.dataFile.map(_.length).getOrElse(split.logFiles.sortBy(_.getFileSize).apply(split.logFiles.size / 2).getFileSize)
    }

    splits.foreach { split =>
      val currentSplitSize = splitSize(split)
      if (currentSize + currentSplitSize > maxPartitionBytesForMor) {
        closePartition()
      }
      currentSize += currentSplitSize + openCostInBytes
      currentFiles += split
    }
    closePartition()

    logInfo(s"MaxPartitionBytes: $maxPartitionBytes, MaxPartitionBytesFraction: $maxPartitionBytesFraction, MaxPartitionBytesForMor: $maxPartitionBytesForMor" +
      s"OpenCostInBytes: $openCostInBytes" +
      s"Merged ${splits.size} splits into ${partitions.size} partitions"
    )

    partitions.toSeq
  }

  private def mergeSplitsToPartitionByBucket(splits: Seq[HoodieMergeOnReadFileSplit]): Seq[HoodieDefaultFilePartition] = {
    throw new HoodieNotSupportedException("Bucket based merge is not supported yet")
  }

  protected def buildSplits(fileSlices: Seq[FileSlice]): List[HoodieMergeOnReadFileSplit] = {
    fileSlices.map { fileSlice =>
      val baseFile = toScalaOption(fileSlice.getBaseFile)
      val logFiles = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList

      val partitionedBaseFile = baseFile.map { file =>
        createPartitionedFile(
          getPartitionColumnsAsInternalRow(file.getPathInfo), file.getPathInfo.getPath, 0, file.getFileLen)
      }

      HoodieMergeOnReadFileSplit(partitionedBaseFile, logFiles)
    }.toList
  }
}

object MergeOnReadSnapshotRelation extends SparkAdapterSupport {

  /**
   * List of [[HoodieRecordPayload]] classes capable of merging projected records:
   * in some cases, when for example, user is only interested in a handful of columns rather
   * than the full row we will be able to optimize data throughput by only fetching the required
   * columns. However, to properly fulfil MOR semantic particular [[HoodieRecordPayload]] in
   * question should be able to merge records based on just such projected representation (including
   * columns required for merging, such as primary-key, pre-combine key, etc)
   */
  private val projectionCompatiblePayloadClasses: Set[String] = Seq(
    classOf[OverwriteWithLatestAvroPayload]
  ).map(_.getName).toSet

  def isProjectionCompatible(tableState: HoodieTableState): Boolean =
    projectionCompatiblePayloadClasses.contains(tableState.recordPayloadClassName)

  def createPartitionedFile(partitionValues: InternalRow,
                            filePath: StoragePath,
                            start: Long,
                            length: Long): PartitionedFile = {
    sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      partitionValues, filePath, start, length)
  }
}
