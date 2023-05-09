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

import org.apache.hadoop.fs.{FileStatus, GlobPattern, Path}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters._
import scala.collection.immutable


trait HoodieIncrementalRelationTrait extends HoodieBaseRelation {

  // Validate this Incremental implementation is properly configured
  validate()

  protected val useStateTransitionTime: Boolean =
    optParams.get(DataSourceReadOptions.READ_BY_STATE_TRANSITION_TIME.key)
      .map(_.toBoolean)
      .getOrElse(DataSourceReadOptions.READ_BY_STATE_TRANSITION_TIME.defaultValue)

  protected def startTimestamp: String = optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)
  protected def endTimestamp: String = optParams.getOrElse(
    DataSourceReadOptions.END_INSTANTTIME.key,
    if (useStateTransitionTime) super.timeline.lastInstant().get.getStateTransitionTime
    else super.timeline.lastInstant().get.getTimestamp)

  protected def startInstantArchived: Boolean = super.timeline.isBeforeTimelineStarts(startTimestamp)
  protected def endInstantArchived: Boolean = super.timeline.isBeforeTimelineStarts(endTimestamp)

  // Fallback to full table scan if any of the following conditions matches:
  //   1. the start commit is archived
  //   2. the end commit is archived
  //   3. there are files in metadata be deleted
  protected lazy val fullTableScan: Boolean = {
    val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.defaultValue).toBoolean

    fallbackToFullTableScan && (startInstantArchived || endInstantArchived || affectedFilesInCommits.exists(fileStatus => !metaClient.getFs.exists(fileStatus.getPath)))
  }

  protected lazy val includedCommits: immutable.Seq[HoodieInstant] = {
    if (!startInstantArchived || !endInstantArchived) {
      // If endTimestamp commit is not archived, will filter instants
      // before endTimestamp.
      if (useStateTransitionTime) {
        super.timeline.findInstantsInRangeByStateTransitionTs(startTimestamp, endTimestamp).getInstants.asScala.toList
      } else {
        super.timeline.findInstantsInRange(startTimestamp, endTimestamp).getInstants.asScala.toList
      }
    } else {
      super.timeline.getInstants.asScala.toList
    }
  }

  protected lazy val commitsMetadata = includedCommits.map(getCommitMetadata(_, super.timeline)).asJava

  protected lazy val affectedFilesInCommits: Array[FileStatus] = {
    listAffectedFilesForCommits(conf, new Path(metaClient.getBasePath), commitsMetadata)
  }

  // Record filters making sure that only records w/in the requested bounds are being fetched as part of the
  // scan collected by this relation
  protected lazy val incrementalSpanRecordFilters: Seq[Filter] = {
    val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)

    val largerThanFilter = if (startInstantArchived) {
      GreaterThan(HoodieRecord.COMMIT_TIME_METADATA_FIELD, startTimestamp)
    } else {
      GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, includedCommits.head.getTimestamp)
    }

    val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      if (endInstantArchived) endTimestamp else includedCommits.last.getTimestamp)

    Seq(isNotNullFilter, largerThanFilter, lessThanFilter)
  }

  override lazy val mandatoryFields: Seq[String] = {
    // NOTE: This columns are required for Incremental flow to be able to handle the rows properly, even in
    //       cases when no columns are requested to be fetched (for ex, when using {@code count()} API)
    Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++
      preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())
  }

  protected def validate(): Unit = {
    if (super.timeline.empty()) {
      throw new HoodieException("No instants to incrementally pull")
    }

    if (!this.optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME.key)) {
      throw new HoodieException(s"Specify the begin instant time to pull from using " +
        s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME.key}")
    }

    if (!this.tableConfig.populateMetaFields()) {
      throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
    }

    if (useStateTransitionTime && fullTableScan) {
      throw new HoodieException("Cannot use stateTransitionTime while enables full table scan")
    }
  }

  protected def globPattern: String =
    optParams.getOrElse(DataSourceReadOptions.INCR_PATH_GLOB.key, DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)

  protected def filterFileSlices(fileSlices: Seq[FileSlice], pathGlobPattern: String): Seq[FileSlice] = {
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
