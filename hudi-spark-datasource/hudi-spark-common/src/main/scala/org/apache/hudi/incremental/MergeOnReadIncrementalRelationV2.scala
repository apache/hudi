/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.incremental

import org.apache.hudi.{DataSourceReadOptions, HoodieFileIndex}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getWritePartitionPaths
import org.apache.hudi.storage.StoragePathInfo

import org.apache.hadoop.fs.GlobPattern
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
                                            override val rangeType: RangeType = RangeType.OPEN_CLOSED)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema)
    with HoodieIncrementalRelationV2Trait with MergeOnReadIncrementalRelation {

  override protected def timeline: HoodieTimeline = {
    if (fullTableScan) {
      metaClient.getCommitsAndCompactionTimeline
    } else {
      queryContext.getActiveTimeline
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
        val fsView = new HoodieTableFileSystemView(metaClient, timeline, affectedFilesInCommits)
        val modifiedPartitions = getWritePartitionPaths(commitsMetadata)

        fileIndex.listMatchingPartitionPaths(HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters))
          .map(p => p.getPath).filter(p => modifiedPartitions.contains(p))
          .flatMap { relativePartitionPath =>
            fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
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

  // Fallback to full table scan if any of the following conditions matches:
  //   1. the start commit is archived
  //   2. the end commit is archived
  //   3. there are files in metadata be deleted
  protected lazy val fullTableScan: Boolean = {
    val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.defaultValue).toBoolean

    fallbackToFullTableScan && (startInstantArchived
      || affectedFilesInCommits.asScala.exists(fileStatus => !metaClient.getStorage.exists(fileStatus.getPath)))
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

