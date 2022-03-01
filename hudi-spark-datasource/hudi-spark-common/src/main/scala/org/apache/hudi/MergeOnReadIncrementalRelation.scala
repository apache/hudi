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

import org.apache.hadoop.fs.{GlobPattern, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.{getCommitMetadata, getWritePartitionPaths, listAffectedFilesForCommits}
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Experimental.
  * Relation, that implements the Hoodie incremental view for Merge On Read table.
  *
  */
class MergeOnReadIncrementalRelation(sqlContext: SQLContext,
                                     val optParams: Map[String, String],
                                     val userSchema: Option[StructType],
                                     val metaClient: HoodieTableMetaClient)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)

  private val commitTimeline = metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants()
  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME.key)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME.key}")
  }
  if (!metaClient.getTableConfig.populateMetaFields()) {
    throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
  }

  private val lastInstant = commitTimeline.lastInstant().get()
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  private val commitsTimelineToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key, lastInstant.getTimestamp))
  logDebug(s"${commitsTimelineToReturn.getInstants.iterator().toList.map(f => f.toString).mkString(",")}")
  private val commitsToReturn = commitsTimelineToReturn.getInstants.iterator().toList

  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)

  private val fileIndex = if (commitsToReturn.isEmpty) List() else buildFileIndex()

  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD.key)
    }
  }

  override def needConversion: Boolean = false

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (fileIndex.isEmpty) {
      filters
    } else {
      val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
      val largerThanFilter = GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.head.getTimestamp)
      val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.last.getTimestamp)
      filters :+ isNotNullFilter :+ largerThanFilter :+ lessThanFilter
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (fileIndex.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      logDebug(s"buildScan requiredColumns = ${requiredColumns.mkString(",")}")
      logDebug(s"buildScan filters = ${filters.mkString(",")}")
      // config to ensure the push down filter for parquet will be applied.
      sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
      sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.recordLevelFilter.enabled", "true")
      sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
      val pushDownFilter = {
        val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
        val largerThanFilter = GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.head.getTimestamp)
        val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.last.getTimestamp)
        filters :+ isNotNullFilter :+ largerThanFilter :+ lessThanFilter
      }
      val (requiredAvroSchema, requiredStructSchema) =
        HoodieSparkUtils.getRequiredSchema(tableAvroSchema, requiredColumns)

      val hoodieTableState = HoodieMergeOnReadTableState(
        tableStructSchema,
        requiredStructSchema,
        tableAvroSchema.toString,
        requiredAvroSchema.toString,
        fileIndex,
        preCombineField,
        Option.empty
      )
      val fullSchemaParquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
        sparkSession = sqlContext.sparkSession,
        dataSchema = tableStructSchema,
        partitionSchema = StructType(Nil),
        requiredSchema = tableStructSchema,
        filters = pushDownFilter,
        options = optParams,
        hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
      )

      val requiredSchemaParquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
        sparkSession = sqlContext.sparkSession,
        dataSchema = tableStructSchema,
        partitionSchema = StructType(Nil),
        requiredSchema = tableStructSchema,
        filters = pushDownFilter,
        options = optParams,
        hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
      )

      val rdd = new HoodieMergeOnReadRDD(
        sqlContext.sparkContext,
        jobConf,
        fullSchemaParquetReader,
        requiredSchemaParquetReader,
        hoodieTableState
      )
      rdd.asInstanceOf[RDD[Row]]
    }
  }

  def buildFileIndex(): List[HoodieMergeOnReadFileSplit] = {
    val metadataList = commitsToReturn.map(instant => getCommitMetadata(instant, commitsTimelineToReturn))
    val affectedFileStatus = listAffectedFilesForCommits(conf, new Path(metaClient.getBasePath), metadataList)
    val fsView = new HoodieTableFileSystemView(metaClient, commitsTimelineToReturn, affectedFileStatus)

    // Iterate partitions to create splits
    val fileGroups = getWritePartitionPaths(metadataList).flatMap(partitionPath =>
      fsView.getAllFileGroups(partitionPath).iterator()
    ).toList
    val latestCommit = fsView.getLastInstant.get.getTimestamp
    if (log.isDebugEnabled) {
      fileGroups.foreach(f => logDebug(s"current file group id: " +
        s"${f.getFileGroupId} and file slices ${f.getLatestFileSlice.get.toString}"))
    }

    // Filter files based on user defined glob pattern
    val pathGlobPattern = optParams.getOrElse(
      DataSourceReadOptions.INCR_PATH_GLOB.key,
      DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)
    val filteredFileGroup = if (!pathGlobPattern.equals(DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)) {
      val globMatcher = new GlobPattern("*" + pathGlobPattern)
      fileGroups.filter(fg => {
        val latestFileSlice = fg.getLatestFileSlice.get
        if (latestFileSlice.getBaseFile.isPresent) {
          globMatcher.matches(latestFileSlice.getBaseFile.get.getPath)
        } else {
          globMatcher.matches(latestFileSlice.getLatestLogFile.get.getPath.toString)
        }
      })
    } else {
      fileGroups
    }

    // Build HoodieMergeOnReadFileSplit.
    filteredFileGroup.map(f => {
      // Ensure get the base file when there is a pending compaction, which means the base file
      // won't be in the latest file slice.
      val baseFiles = f.getAllFileSlices.iterator().filter(slice => slice.getBaseFile.isPresent).toList
      val partitionedFile = if (baseFiles.nonEmpty) {
        val baseFile = baseFiles.head.getBaseFile
        val filePath = MergeOnReadSnapshotRelation.getFilePath(baseFile.get.getFileStatus.getPath)
        Option(PartitionedFile(InternalRow.empty, filePath, 0, baseFile.get.getFileLen))
      }
      else {
        Option.empty
      }

      val logPath = if (f.getLatestFileSlice.isPresent) {
        //If log path doesn't exist, we still include an empty path to avoid using
        // the default parquet reader to ensure the push down filter will be applied.
        Option(f.getLatestFileSlice.get().getLogFiles.iterator().toList
          .map(logfile => logfile.getPath.toString))
      }
      else {
        Option.empty
      }

      HoodieMergeOnReadFileSplit(partitionedFile, logPath,
        latestCommit, metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
    })
  }
}
