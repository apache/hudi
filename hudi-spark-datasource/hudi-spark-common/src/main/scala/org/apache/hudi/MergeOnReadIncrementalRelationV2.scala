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

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{FileSlice, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.TimelineUtils.{concatTimeline, getCommitMetadata}
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.util.{Option => HOption, StringUtils}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getWritePartitionPaths
import org.apache.hudi.storage.StoragePathInfo

import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.immutable

trait MergeOnReadIncrementalRelation {
  def listFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Map[InternalRow, Seq[FileSlice]]
  def getRequiredFilters: Seq[Filter]
  def getInstantRange: HOption[InstantRange] = HOption.empty()
  def getIncrementalFilesSize: Long
}

case class MergeOnReadIncrementalRelationV2(override val sqlContext: SQLContext,
                                            override val optParams: Map[String, String],
                                            override val metaClient: HoodieTableMetaClient,
                                            private val userSchema: Option[StructType],
                                            override val rangeType: RangeType = RangeType.OPEN_CLOSED)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, userSchema)
    with HoodieIncrementalRelationV2Trait with MergeOnReadIncrementalRelation {

  override protected def timeline: HoodieTimeline = {
    if (fullTableScan) {
      metaClient.getCommitsAndCompactionTimeline
    } else {
      queryContext.getActiveTimeline
    }
  }

  protected override def composeRDD(fileSplits: Seq[HoodieMergeOnReadFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    // The only required filters are ones that make sure we're only fetching records that
    // fall into incremental span of the timeline being queried
    val requiredFilters = incrementalSpanRecordFilters
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    new HoodieMergeOnReadRDDV2(
      sqlContext.sparkContext,
      config = jobConf,
      sqlConf = sqlContext.sparkSession.sessionState.conf,
      fileReaders = readers,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      targetInstantTime = targetInstantTime,
      mergeType = mergeType,
      fileSplits = fileSplits,
      includedInstantTimeSet = Option(includedCommits.map(_.requestedTime).toSet),
      instantRangeOpt = if (fullTableScan) getInstantRange else windowEndInstantRange,
      optionalFilters = optionalFilters,
      metaClient = metaClient,
      options = optParams)
  }

  override protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    if (includedCommits.isEmpty) {
      List()
    } else {
      val fileSlices = if (fullTableScan) {
        listLatestFileSlices(partitionFilters, dataFilters)
      } else {
        val latestCommit = includedCommits.last.requestedTime
        val modifiedPartitions = getWritePartitionPaths(commitsMetadata).asScala.toSeq
        collectIncrementalFileSlices(modifiedPartitions, latestCommit)
      }

      buildSplits(filterFileSlices(fileSlices, globPattern))
    }
  }

  override def listFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Map[InternalRow, Seq[FileSlice]] = {
    val slices = if (includedCommits.isEmpty) {
      List()
    } else {
      val fileSlices = if (fullTableScan) {
        listLatestFileSlices(partitionFilters, dataFilters)
      } else {
        val latestCommit = includedCommits.last.requestedTime
        val modifiedPartitions = getWritePartitionPaths(commitsMetadata)

        // Evaluate the partition predicates against the modified partitions only, rather than listing
        // every partition of the table.
        val partitionPaths = fileIndex.filterPartitionPaths(modifiedPartitions.asScala.toSeq,
          HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters))
        collectIncrementalFileSlices(partitionPaths, latestCommit)
      }
      filterFileSlices(fileSlices, globPattern)
    }

    slices.groupBy(fs => {
      if (fs.getBaseFile.isPresent) {
        getPartitionColumnValuesAsInternalRow(fs.getBaseFile.get.getPathInfo)
      } else {
        getPartitionColumnValuesAsInternalRow(fs.getLogFiles.findAny.get.getPathInfo)
      }
    })
  }

  override def getRequiredFilters: Seq[Filter] = {
    if (includedCommits.isEmpty) {
      Seq.empty
    } else {
      incrementalSpanRecordFilters
    }
  }

  /**
   * Returns the requested-time range selected by the completion-time query analysis when an
   * incremental query falls back to a full-table scan. The file-group reader needs this range so
   * that out-of-range log records from the latest file slices do not participate in record merging.
   */
  override def getInstantRange: HOption[InstantRange] =
    if (fullTableScan) queryContext.getInstantRange else HOption.empty()

  override def getIncrementalFilesSize: Long = {
    // A full table scan lists the whole table, so the affected files would underestimate its size.
    if (fullTableScan) Long.MaxValue else affectedFilesInCommits.asScala.map(_.getLength).sum
  }

  override def shouldIncludeLogFiles(): Boolean = fullTableScan

  /**
   * Collects the incremental file slices for the given modified partitions.
   *
   * The slices start from the files recorded in the window's commits (`affectedFilesInCommits`). On a
   * MOR table, a slice whose base file was written before the window holds only the window's log files,
   * yet resolving the current value of a record changed in the window needs the full file slice
   * (HUDI #18943):
   *  - EVENT_TIME_ORDERING: a record written in the window may carry a lower ordering value than the
   *    version already in the base/earlier-log, so the existing version wins.
   *  - Partial updates: a window log block holds only the changed columns, so the unchanged columns
   *    must be filled in from the base file.
   * Such slices are replaced by the full slice of the same file group from a listing of their
   * partitions (metadata-table-aware), so partitions whose touched file groups all have a base file in
   * the window are not listed. The listing timeline is bounded to `latestCommit` so files written by
   * later commits are not visible; the commit-time filter applied after the merge still restricts the
   * returned records to the window.
   *
   * A slice is replaced when the listing holds every log file the window recorded for it, or when
   * those log files still exist but a compaction scheduled up to `latestCommit` has already folded them
   * into a newer base file. When some were removed (e.g. by cleaning) and the full-table-scan fallback
   * is disabled, the recorded slice is kept, so the scan fails early with a file-not-found error
   * pointing the user at `hoodie.datasource.read.incr.fallback.fulltablescan.enable` instead of
   * returning a partial result.
   */
  private def collectIncrementalFileSlices(partitionPaths: Seq[String], latestCommit: String): Seq[FileSlice] = {
    val affectedSlices = latestMergedFileSlices(new HoodieTableFileSystemView(metaClient, timeline, affectedFilesInCommits),
      partitionPaths, latestCommit, _ => true)
    val slicesWithoutBaseFile = affectedSlices.filter(slice => !slice.getBaseFile.isPresent)
    if (metaClient.getTableType == HoodieTableType.COPY_ON_WRITE || slicesWithoutBaseFile.isEmpty) {
      affectedSlices
    } else {
      val fileIdsWithoutBaseFile = slicesWithoutBaseFile.map(_.getFileId).toSet
      val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(sqlContext.sparkContext))
      val fsView = FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(
        engineContext, metaClient, fileIndex.getMetadataConfig, timeline.findInstantsBeforeOrEquals(latestCommit))
      val fullSlices = latestMergedFileSlices(fsView, slicesWithoutBaseFile.map(_.getPartitionPath).distinct,
        latestCommit, slice => fileIdsWithoutBaseFile.contains(slice.getFileId))
        .map(slice => slice.getFileGroupId -> slice).toMap
      affectedSlices.map { slice =>
        fullSlices.get(slice.getFileGroupId)
          .filter(fullSlice => logFileNames(slice).subsetOf(logFileNames(fullSlice)) || logFilesExist(slice))
          .getOrElse(slice)
      }
    }
  }

  /** The latest merged file slices of `fsView` in the given partitions that satisfy `include`; closes the view. */
  private def latestMergedFileSlices(fsView: HoodieTableFileSystemView, partitionPaths: Seq[String],
                                     latestCommit: String, include: FileSlice => Boolean): Seq[FileSlice] = {
    try {
      partitionPaths.flatMap { relativePartitionPath =>
        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
          .filter(include)
      }
    } finally {
      fsView.close()
    }
  }

  private def logFileNames(slice: FileSlice): Set[String] = slice.getLogFiles.iterator().asScala.map(_.getFileName).toSet

  private def logFilesExist(slice: FileSlice): Boolean =
    slice.getLogFiles.iterator().asScala.forall(logFile => metaClient.getStorage.exists(logFile.getPath))

  /**
   * Bounds the file-group merge of the legacy RDD read to the window end, so log blocks written after
   * the window in a full file slice are not merged in.
   */
  private def windowEndInstantRange: HOption[InstantRange] =
    includedCommits.lastOption.map(last => HOption.of(InstantRange.builder()
      .rangeType(RangeType.CLOSED_CLOSED).nullableBoundary(true).endInstant(last.requestedTime).build()))
      .getOrElse(HOption.empty[InstantRange]())

  private def filterFileSlices(fileSlices: Seq[FileSlice], pathGlobPattern: String): Seq[FileSlice] = {
    val filteredFileSlices = if (!StringUtils.isNullOrEmpty(pathGlobPattern)) {
      val globMatcher = new GlobPattern("*" + pathGlobPattern)
      fileSlices.filter(fileSlice => {
        val path = toScalaOption(fileSlice.getBaseFile).map(_.getPath)
          .orElse(toScalaOption(fileSlice.getLatestLogFile).map(_.getPath.toString))
          .get
        globMatcher.matches(path)
      })
    } else {
      fileSlices
    }
    filteredFileSlices
  }
}

trait HoodieIncrementalRelationV2Trait extends HoodieBaseRelation {

  // Validate this Incremental implementation is properly configured
  validate()

  protected def startCompletionTime: String = optParams(DataSourceReadOptions.START_COMMIT.key)

  protected def endCompletionTime: String = optParams.getOrElse(
    DataSourceReadOptions.END_COMMIT.key, super.timeline.getLatestCompletionTime.get)

  protected def startInstantArchived: Boolean = !queryContext.getArchivedInstants.isEmpty

  // Some files referenced by the commits in the incremental window no longer exist on storage
  // (e.g. they were removed by cleaning). Such a window cannot be served from the incremental
  // file listing alone.
  protected lazy val hasMissingAffectedFiles: Boolean = HoodieBaseRelation.checkIfAnyFilesMissing(
    sqlContext, metaClient, affectedFilesInCommits.asScala.map(_.getPath.toString))

  // Fallback to full table scan if any of the following conditions matches:
  //   1. the start commit is archived
  //   2. the end commit is archived
  //   3. there are files in metadata be deleted
  protected lazy val fullTableScan: Boolean = {
    val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.defaultValue).toBoolean

    if (!fallbackToFullTableScan) {
      false
    } else if (startInstantArchived) {
      true
    } else {
      if (hasMissingAffectedFiles) {
        logInfo("Falling back to full table scan as some files cannot be found.")
      }
      hasMissingAffectedFiles
    }
  }

  protected val rangeType: RangeType

  protected lazy val queryContext: IncrementalQueryAnalyzer.QueryContext =
    IncrementalQueryAnalyzer.builder()
      .metaClient(metaClient)
      .startCompletionTime(optParams(DataSourceReadOptions.START_COMMIT.key))
      .endCompletionTime(optParams.getOrElse(DataSourceReadOptions.END_COMMIT.key, null))
      // do not support skip cluster for spark incremental query yet to avoid data duplication problem,
      // see details in HUDI-9672.
      // .skipClustering(optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_READ_SKIP_CLUSTER.key(),
      //  String.valueOf(DataSourceReadOptions.INCREMENTAL_READ_SKIP_CLUSTER.defaultValue)).toBoolean)
      .skipCompaction(optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.key(),
        String.valueOf(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.defaultValue)).toBoolean)
      .rangeType(rangeType)
      .build()
      .analyze()

  protected lazy val includedCommits: immutable.Seq[HoodieInstant] = queryContext.getInstants.asScala.toList

  protected lazy val commitsMetadata = includedCommits.map(
    i => {
      if (queryContext.getArchivedInstants.contains(i)) {
        getCommitMetadata(i, queryContext.getArchivedTimeline)
      } else {
        getCommitMetadata(i, queryContext.getActiveTimeline)
      }
    }).asJava

  protected lazy val affectedFilesInCommits: java.util.List[StoragePathInfo] = {
    listAffectedFilesForCommits(conf, metaClient.getBasePath, commitsMetadata)
  }

  // Record filters making sure that only records w/in the requested bounds are being fetched as part of the
  // scan collected by this relation
  protected lazy val incrementalSpanRecordFilters: Seq[Filter] = {
    val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)

    val timeStamps = includedCommits.map(_.requestedTime).toArray[Any]
    val inFilter = In(HoodieRecord.COMMIT_TIME_METADATA_FIELD, timeStamps)

    Seq(isNotNullFilter, inFilter)
  }

  override lazy val mandatoryFields: Seq[String] = {
    // NOTE: These columns are required for Incremental flow to be able to handle the rows properly, even in
    //       cases when no columns are requested to be fetched (for ex, when using {@code count()} API)
    Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++ orderingFields
  }

  protected def validate(): Unit = {
    if (super.timeline.empty()) {
      throw new HoodieException("No instants to incrementally pull")
    }

    if (!this.optParams.contains(DataSourceReadOptions.START_COMMIT.key)) {
      throw new HoodieException(s"Specify the start completion time to pull from using " +
        s"option ${DataSourceReadOptions.START_COMMIT.key}")
    }

    // MoR incremental relies on _hoodie_commit_time being present in BOTH base files AND log
    // records. The base-file writer respects hoodie.meta.fields.mode, but the log-write path
    // (HoodieAppendHandle) does not yet — until that gap is closed, MoR incremental must require
    // populate.meta.fields=true to avoid silently dropping log-file rows whose commit_time is null.
    //
    // Gate on the table type, not just the flag: HoodieCopyOnWriteIncrementalHadoopFsRelationFactory
    // builds one of these relations for CoW tables too (it backs HoodieIncrementalFileIndex), so an
    // unconditional check here rejects every selective-mode CoW table — the case this release
    // actually supports. CoW keeps its own commit-time guard in IncrementalRelationV1/V2.
    if (metaClient.getTableType == HoodieTableType.MERGE_ON_READ && !this.tableConfig.populateMetaFields()) {
      throw new HoodieException("Incremental queries on MoR tables are not supported when "
        + "hoodie.populate.meta.fields=false. Selective meta-field modes (hoodie.meta.fields.mode) "
        + "are supported for CoW only in this release; MoR support is tracked as a follow-up.")
    }
  }

  protected def globPattern: String =
    optParams.getOrElse(DataSourceReadOptions.INCR_PATH_GLOB.key, DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)

}
