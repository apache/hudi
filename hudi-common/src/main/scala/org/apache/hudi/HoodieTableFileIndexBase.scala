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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
import org.apache.hudi.common.model.{FileSlice, HoodieTableQueryType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.view.{FileSystemViewStorageConfig, HoodieTableFileSystemView}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Common (engine-agnostic) File Index implementation enabling individual query engines to
 * list Hudi Table contents based on the
 *
 * <ul>
 *   <li>Table type (MOR, COW)</li>
 *   <li>Query type (snapshot, read_optimized, incremental)</li>
 *   <li>Query instant/range</li>
 * </ul>
 *
 * @param engineContext Hudi engine-specific context
 * @param metaClient Hudi table's meta-client
 * @param configProperties unifying configuration (in the form of generic properties)
 * @param queryType target query type
 * @param queryPaths target DFS paths being queried
 * @param specifiedQueryInstant instant as of which table is being queried
 * @param shouldIncludePendingCommits flags whether file-index should exclude any pending operations
 * @param fileStatusCache transient cache of fetched [[FileStatus]]es
 */
abstract class HoodieTableFileIndexBase(engineContext: HoodieEngineContext,
                                        metaClient: HoodieTableMetaClient,
                                        configProperties: TypedProperties,
                                        queryType: HoodieTableQueryType,
                                        protected val queryPaths: Seq[Path],
                                        specifiedQueryInstant: Option[String] = None,
                                        shouldIncludePendingCommits: Boolean = false,
                                        @transient fileStatusCache: FileStatusCacheTrait) {
  /**
   * Get all completeCommits.
   */
  lazy val completedCommits = metaClient.getCommitsTimeline
    .filterCompletedInstants().getInstants.iterator().toList.map(_.getTimestamp)

  private lazy val _partitionColumns: Array[String] =
    metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())

  private lazy val fileSystemStorageConfig = FileSystemViewStorageConfig.newBuilder()
    .fromProperties(configProperties)
    .build()
  private lazy val metadataConfig = HoodieMetadataConfig.newBuilder
    .fromProperties(configProperties)
    .build()

  private val tableType = metaClient.getTableType

  protected val basePath: String = metaClient.getBasePath

  @transient
  @volatile protected var cachedFileSize: Long = 0L
  @transient
  @volatile protected var cachedAllInputFileSlices: Map[PartitionPath, Seq[FileSlice]] = _
  @volatile protected var queryAsNonePartitionedTable: Boolean = _
  @transient
  @volatile private var fileSystemView: HoodieTableFileSystemView = _

  refresh0()

  /**
   * Returns latest completed instant as seen by this instance of the file-index
   */
  def latestCompletedInstant(): Option[HoodieInstant] =
    getActiveTimeline.filterCompletedInstants().lastInstant()

  /**
   * Fetch list of latest base files and log files per partition.
   *
   * @return mapping from string partition paths to its base/log files
   */
  def listFileSlices(): Map[String, Seq[FileSlice]] = {
    if (queryAsNonePartitionedTable) {
      // Read as Non-Partitioned table.
      cachedAllInputFileSlices.map(entry => (entry._1.path, entry._2))
    } else {
      cachedAllInputFileSlices.keys.toSeq.map(partition => {
        (partition.path, cachedAllInputFileSlices(partition))
      }).toMap
    }
  }

  private def refresh0(): Unit = {
    val startTime = System.currentTimeMillis()
    val partitionFiles = loadPartitionPathFiles()
    val allFiles = partitionFiles.values.reduceOption(_ ++ _)
      .getOrElse(Array.empty[FileStatus])

    metaClient.reloadActiveTimeline()

    val activeTimeline = getActiveTimeline
    val latestInstant = activeTimeline.lastInstant()
    // TODO we can optimize the flow by:
    //  - First fetch list of files from instants of interest
    //  - Load FileStatus's
    fileSystemView = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles)
    val queryInstant = if (specifiedQueryInstant.isDefined) {
      specifiedQueryInstant
    } else if (latestInstant.isPresent) {
      Some(latestInstant.get.getTimestamp)
    } else {
      None
    }

    (tableType, queryType) match {
      case (MERGE_ON_READ, HoodieTableQueryType.QUERY_TYPE_SNAPSHOT) =>
        // Fetch and store latest base and log files, and their sizes
        cachedAllInputFileSlices = partitionFiles.map(p => {
          val latestSlices = if (queryInstant.isDefined) {
            fileSystemView.getLatestMergedFileSlicesBeforeOrOn(p._1.path, queryInstant.get)
              .iterator().asScala.toSeq
          } else {
            Seq()
          }
          (p._1, latestSlices)
        })
        cachedFileSize = cachedAllInputFileSlices.values.flatten.map(fileSlice => {
          if (fileSlice.getBaseFile.isPresent) {
            fileSlice.getBaseFile.get().getFileLen + fileSlice.getLogFiles.iterator().asScala.map(_.getFileSize).sum
          } else {
            fileSlice.getLogFiles.iterator().asScala.map(_.getFileSize).sum
          }
        }).sum
      case (_, _) =>
        // Fetch and store latest base files and its sizes
        cachedAllInputFileSlices = partitionFiles.map(p => {
          val fileSlices = specifiedQueryInstant
            .map(instant =>
              fileSystemView.getLatestFileSlicesBeforeOrOn(p._1.path, instant, true))
            .getOrElse(fileSystemView.getLatestFileSlices(p._1.path))
            .iterator().asScala.toSeq
          (p._1, fileSlices)
        })
        cachedFileSize = cachedAllInputFileSlices.values.flatten.map(fileSliceSize).sum
    }

    // If the partition value contains InternalRow.empty, we query it as a non-partitioned table.
    queryAsNonePartitionedTable = partitionFiles.keys.exists(p => p.values.isEmpty)
    val flushSpend = System.currentTimeMillis() - startTime

    logInfo(s"Refresh table ${metaClient.getTableConfig.getTableName}," +
      s" spend: $flushSpend ms")
  }

  protected def refresh(): Unit = {
    fileStatusCache.invalidate()
    refresh0()
  }

  private def getActiveTimeline = {
    // NOTE: We have to use commits and compactions timeline, to make sure that we're properly
    //       handling the following case: when records are inserted into the new log-file w/in the file-group
    //       that is under the pending compaction process, new log-file will bear the compaction's instant (on the
    //       timeline) in its name, as opposed to the base-file's commit instant. To make sure we're not filtering
    //       such log-file we have to _always_ include pending compaction instants into consideration
    // TODO(HUDI-3302) re-evaluate whether we should not filter any commits in here
    val timeline = metaClient.getCommitsAndCompactionTimeline
    if (shouldIncludePendingCommits) {
      timeline
    } else {
      timeline.filterCompletedAndCompactionInstants()
    }
  }

  private def fileSliceSize(fileSlice: FileSlice): Long = {
    val logFileSize = fileSlice.getLogFiles.iterator().asScala.map(_.getFileSize).filter(_ > 0).sum
    if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get().getFileLen + logFileSize
    } else {
      logFileSize
    }
  }

  /**
   * Load all partition paths and it's files under the query table path.
   */
  private def loadPartitionPathFiles(): Map[PartitionPath, Array[FileStatus]] = {
    val partitionPaths = getAllQueryPartitionPaths
    // List files in all of the partition path.
    val pathToFetch = mutable.ArrayBuffer[PartitionPath]()
    val cachePartitionToFiles = mutable.Map[PartitionPath, Array[FileStatus]]()
    // Fetch from the FileStatusCache
    partitionPaths.foreach { partitionPath =>
      fileStatusCache.get(partitionPath.fullPartitionPath(basePath)) match {
        case Some(filesInPartition) =>
          cachePartitionToFiles.put(partitionPath, filesInPartition)

        case None => pathToFetch.append(partitionPath)
      }
    }

    val fetchedPartitionToFiles =
      if (pathToFetch.nonEmpty) {
        val fullPartitionPathsToFetch = pathToFetch.map(p => (p, p.fullPartitionPath(basePath).toString)).toMap
        val partitionToFilesMap = FSUtils.getFilesInPartitions(engineContext, metadataConfig, basePath,
          fullPartitionPathsToFetch.values.toArray, fileSystemStorageConfig.getSpillableDir)
        fullPartitionPathsToFetch.map(p => {
          (p._1, partitionToFilesMap.get(p._2))
        })
      } else {
        Map.empty[PartitionPath, Array[FileStatus]]
      }

    // Update the fileStatusCache
    fetchedPartitionToFiles.foreach {
      case (partitionRowPath, filesInPartition) =>
        fileStatusCache.put(partitionRowPath.fullPartitionPath(basePath), filesInPartition)
    }
    cachePartitionToFiles.toMap ++ fetchedPartitionToFiles
  }

  def getAllQueryPartitionPaths: Seq[PartitionPath] = {
    val queryRelativePartitionPaths = queryPaths.map(FSUtils.getRelativePartitionPath(new Path(basePath), _))
    // Load all the partition path from the basePath, and filter by the query partition path.
    // TODO load files from the queryRelativePartitionPaths directly.
    val partitionPaths = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath).asScala
      .filter(path => queryRelativePartitionPaths.exists(path.startsWith))

    val partitionSchema = _partitionColumns

    // Convert partition's path into partition descriptor
    partitionPaths.map { partitionPath =>
      val partitionColumnValues = parsePartitionColumnValues(partitionSchema, partitionPath)
      PartitionPath(partitionPath, partitionColumnValues)
    }
  }

  /**
   * Parses partition columns' values from the provided partition's path, returning list of
   * values (that might have engine-specific representation)
   *
   * @param partitionColumns partitioning columns identifying the partition
   * @param partitionPath partition's path to parse partitioning columns' values from
   */
  protected def parsePartitionColumnValues(partitionColumns: Array[String], partitionPath: String): Array[Any]

  // TODO eval whether we should just use logger directly
  protected def logWarning(str: => String): Unit
  protected def logInfo(str: => String): Unit

  /**
   * Represents a partition as a tuple of
   * <ul>
   *   <li>Actual partition path (relative to the table's base path)</li>
   *   <li>Values of the corresponding columns table is being partitioned by (partitioning columns)</li>
   * </ul>
   *
   * E.g. PartitionPath("2021/02/01", Array("2021","02","01"))
   *
   * NOTE: Partitioning column values might have engine specific representation (for ex,
   * {@code UTF8String} for Spark, etc) and are solely used in partition pruning in an very
   * engine-specific ways
   *
   * @param values values of the corresponding partitioning columns
   * @param path partition's path
   *
   * TODO expose as a trait and make impls engine-specific (current impl is tailored for Spark)
   */
  case class PartitionPath(path: String, values: Array[Any]) {
    override def equals(other: Any): Boolean = other match {
      case PartitionPath(otherPath, _) => path == otherPath
      case _ => false
    }

    override def hashCode(): Int = {
      path.hashCode
    }

    def fullPartitionPath(basePath: String): Path = {
      if (path.isEmpty) {
        new Path(basePath) // This is a non-partition path
      } else {
        new Path(basePath, path)
      }
    }
  }

  /**
   * Converts Hudi's internal representation of the {@code Option} into Scala's default one
   */
  implicit def asScalaOption[T](opt: org.apache.hudi.common.util.Option[T]): Option[T] =
    if (opt.isPresent) {
      Some(opt.get)
    } else {
      None
    }
}

trait FileStatusCacheTrait {
  def get(path: Path): Option[Array[FileStatus]]
  def put(path: Path, leafFiles: Array[FileStatus]): Unit
  def invalidate(): Unit
}
