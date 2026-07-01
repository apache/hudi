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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.TimelineUtils.{concatTimeline, getCommitMetadata}
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.util.StringUtils
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
}

case class MergeOnReadIncrementalRelationV2(override val sqlContext: SQLContext,
                                            override val optParams: Map[String, String],
                                            override val metaClient: HoodieTableMetaClient,
                                            private val userSchema: Option[StructType],
                                            private val prunedDataSchema: Option[StructType] = None,
                                            override val rangeType: RangeType = RangeType.OPEN_CLOSED)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, userSchema, prunedDataSchema)
    with HoodieIncrementalRelationV2Trait with MergeOnReadIncrementalRelation {

  override type Relation = MergeOnReadIncrementalRelationV2

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

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
      tableState = tableState,
      mergeType = mergeType,
      fileSplits = fileSplits,
      includedInstantTimeSet = Option(includedCommits.map(_.requestedTime).toSet),
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
        if (hasMissingAffectedFiles) {
          legacyAffectedFileSlices(modifiedPartitions, latestCommit)
        } else {
          collectIncrementalFileSlices(modifiedPartitions, latestCommit)
        }
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
        val partitionPaths = fileIndex.listMatchingPartitionPaths(HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters))
          .map(p => p.getPath).filter(p => modifiedPartitions.contains(p))
        if (hasMissingAffectedFiles) {
          legacyAffectedFileSlices(partitionPaths, latestCommit)
        } else {
          collectIncrementalFileSlices(partitionPaths, latestCommit)
        }
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

  override def shouldIncludeLogFiles(): Boolean = fullTableScan

  /**
   * Collects the incremental file slices for the given modified partitions.
   *
   * Each returned file slice carries its base file plus all of its log files so the
   * [[org.apache.hudi.common.table.read.HoodieFileGroupReader]] can perform a correct runtime merge.
   * Resolving the current value of a record changed in the incremental window requires the full file
   * slice, not just the files written within the window:
   *  - EVENT_TIME_ORDERING: a record written in the window may carry a lower ordering value than the
   *    version already in the base/earlier-log, so the existing version wins; a window-only view
   *    cannot determine the winner.
   *  - Partial updates: a window log block holds only the changed columns, so the unchanged columns
   *    must be filled in from the base file.
   * (HUDI #18943.)
   *
   * The view is built from the (modified) partition listing (which includes the latest base file of
   * each file group, honoring the metadata table when enabled) and scoped back to the file groups
   * actually touched in the window (see [[affectedFileGroupIds]]) so untouched file groups are not
   * read. The commit-time record filter applied during the scan still restricts the returned records
   * to the incremental window.
   *
   * The view timeline is bounded to instants at or before `latestCommit` (the window's last commit)
   * so that base/log files written by later commits are not visible. Without this bound a record
   * updated again after the window would be merged with those later log files and its merged commit
   * time would fall outside the window, dropping the in-window change from the result.
   */
  private def collectIncrementalFileSlices(partitionPaths: Seq[String], latestCommit: String): Seq[FileSlice] = {
    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(sqlContext.sparkContext))
    val fsView = FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(
      engineContext, metaClient, fileIndex.metadataConfig, timeline.findInstantsBeforeOrEquals(latestCommit))
    try {
      partitionPaths.flatMap { relativePartitionPath =>
        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
          .filter(fs => affectedFileGroupIds.contains(fs.getFileId))
      }
    } finally {
      fsView.close()
    }
  }

  /**
   * Builds the incremental file slices directly from the files recorded in the window's commits
   * (`affectedFilesInCommits`), as the read path did before HUDI #18943.
   *
   * This is used only when [[hasMissingAffectedFiles]] is true and the full-table-scan fallback is
   * disabled: some files referenced by the window have been removed (e.g. by cleaning), so there is
   * no correct incremental result to produce. Listing from the recorded files (which include the
   * missing paths) preserves the prior fail-early contract - the scan surfaces a file-not-found
   * error pointing the user at `hoodie.datasource.read.incr.fallback.fulltablescan.enable` - instead
   * of silently returning an empty/partial result from a fresh listing that no longer sees those
   * files.
   */
  private def legacyAffectedFileSlices(partitionPaths: Seq[String], latestCommit: String): Seq[FileSlice] = {
    val fsView = new HoodieTableFileSystemView(metaClient, timeline, affectedFilesInCommits)
    try {
      partitionPaths.flatMap { relativePartitionPath =>
        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
      }
    } finally {
      fsView.close()
    }
  }

  /**
   * File group ids touched within the incremental window. Used to scope the partition-listing based
   * view (which surfaces every file group in a modified partition) back to only the changed file
   * groups, so we do not read and discard untouched file groups.
   */
  private lazy val affectedFileGroupIds: Set[String] =
    affectedFilesInCommits.asScala.map(f => FSUtils.getFileIdFromFilePath(f.getPath)).toSet

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
  protected lazy val hasMissingAffectedFiles: Boolean =
    affectedFilesInCommits.asScala.exists(fileStatus => !metaClient.getStorage.exists(fileStatus.getPath))

  // Fallback to full table scan if any of the following conditions matches:
  //   1. the start commit is archived
  //   2. the end commit is archived
  //   3. there are files in metadata be deleted
  protected lazy val fullTableScan: Boolean = {
    val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.defaultValue).toBoolean

    fallbackToFullTableScan && (startInstantArchived || hasMissingAffectedFiles)
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

    if (!this.tableConfig.populateMetaFields()) {
      throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
    }
  }

  protected def globPattern: String =
    optParams.getOrElse(DataSourceReadOptions.INCR_PATH_GLOB.key, DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)

}

