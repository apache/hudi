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
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.{FileSystemViewStorageConfig, HoodieTableFileSystemView}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache}
import org.apache.spark.sql.hudi.HoodieSqlUtils
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A file index which support partition prune for hoodie snapshot and read-optimized query.
 *
 * Main steps to get the file list for query:
 * 1、Load all files and partition values from the table path.
 * 2、Do the partition prune by the partition filter condition.
 *
 * There are 3 cases for this:
 * 1、If the partition columns size is equal to the actually partition path level, we
 * read it as partitioned table.(e.g partition column is "dt", the partition path is "2021-03-10")
 *
 * 2、If the partition columns size is not equal to the partition path level, but the partition
 * column size is "1" (e.g. partition column is "dt", but the partition path is "2021/03/10"
 * who's directory level is 3).We can still read it as a partitioned table. We will mapping the
 * partition path (e.g. 2021/03/10) to the only partition column (e.g. "dt").
 *
 * 3、Else the the partition columns size is not equal to the partition directory level and the
 * size is great than "1" (e.g. partition column is "dt,hh", the partition path is "2021/03/10/12")
 * , we read it as a Non-Partitioned table because we cannot know how to mapping the partition
 * path with the partition columns in this case.
 *
 */
abstract class HoodieTableFileIndex(engineContext: HoodieEngineContext,
                                    metaClient: HoodieTableMetaClient,
                                    configProperties: TypedProperties,
                                    @transient fileStatusCache: FileStatusCache = NoopCache) {
  /**
   * Get all completeCommits.
   */
  lazy val completedCommits = metaClient.getCommitsTimeline
    .filterCompletedInstants().getInstants.iterator().toList.map(_.getTimestamp)
  /**
   * Get the partition schema from the hoodie.properties.
   */
  private lazy val _partitionColumns: Array[String] =
    metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())

  private lazy val fileSystemStorageConfig = FileSystemViewStorageConfig.newBuilder()
    .fromProperties(configProperties)
    .build()
  private lazy val metadataConfig = HoodieMetadataConfig.newBuilder
    .fromProperties(configProperties)
    .build()
  protected val basePath: String = metaClient.getBasePath

  private val queryType = configProperties(QUERY_TYPE.key())
  private val tableType = metaClient.getTableType

  private val specifiedQueryInstant =
    Option.apply(configProperties.getString(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key))
      .map(HoodieSqlUtils.formatQueryInstant)

  @transient private val queryPath = new Path(configProperties.getOrElse("path", "'path' option required"))
  @transient
  @volatile protected var cachedFileSize: Long = 0L
  @transient
  @volatile protected var cachedAllInputFileSlices: Map[PartitionRowPath, Seq[FileSlice]] = _
  @volatile protected var queryAsNonePartitionedTable: Boolean = _
  @transient
  @volatile private var fileSystemView: HoodieTableFileSystemView = _

  refresh0()

  /**
   * Fetch list of latest base files and log files per partition.
   *
   * @return mapping from string partition paths to its base/log files
   */
  def listFileSlices(): Map[String, Seq[FileSlice]] = {
    if (queryAsNonePartitionedTable) {
      // Read as Non-Partitioned table.
      cachedAllInputFileSlices.map(entry => (entry._1.partitionPath, entry._2))
    } else {
      cachedAllInputFileSlices.keys.toSeq.map(partition => {
        (partition.partitionPath, cachedAllInputFileSlices(partition))
      }).toMap
    }
  }

  /**
   * Returns the FileStatus for all the base files (excluding log files). This should be used only for
   * cases where Spark directly fetches the list of files via HoodieFileIndex or for read optimized query logic
   * implemented internally within Hudi like HoodieBootstrapRelation. This helps avoid the use of path filter
   * to filter out log files within Spark.
   *
   * @return List of FileStatus for base files
   */
  def allFiles: Seq[FileStatus] = {
    cachedAllInputFileSlices.values.flatten
      .filter(_.getBaseFile.isPresent)
      .map(_.getBaseFile.get().getFileStatus)
      .toSeq
  }

  def getAllQueryPartitionPaths: Seq[PartitionRowPath] = {
    val queryPartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), queryPath)
    // Load all the partition path from the basePath, and filter by the query partition path.
    // TODO load files from the queryPartitionPath directly.
    val partitionPaths = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath).asScala
      .filter(_.startsWith(queryPartitionPath))

    val partitionSchema = _partitionColumns

    // Convert partition path to PartitionRowPath
    partitionPaths.map { partitionPath =>
      val partitionRow: Array[Any] = if (partitionSchema.length == 0) {
        // This is a non-partitioned table
        Array.empty
      } else {
        val partitionFragments = partitionPath.split("/")

        if (partitionFragments.length != partitionSchema.length &&
          partitionSchema.length == 1) {
          // If the partition column size is not equal to the partition fragment size
          // and the partition column size is 1, we map the whole partition path
          // to the partition column which can benefit from the partition prune.
          val prefix = s"${partitionSchema.head}="
          val partitionValue = if (partitionPath.startsWith(prefix)) {
            // support hive style partition path
            partitionPath.substring(prefix.length)
          } else {
            partitionPath
          }
          // TODO replace w/ byte array
          Array(UTF8String.fromString(partitionValue))
        } else if (partitionFragments.length != partitionSchema.length &&
          partitionSchema.length > 1) {
          // If the partition column size is not equal to the partition fragments size
          // and the partition column size > 1, we do not know how to map the partition
          // fragments to the partition columns. So we trait it as a Non-Partitioned Table
          // for the query which do not benefit from the partition prune.
          logWarning(s"Cannot do the partition prune for table $basePath." +
            s"The partitionFragments size (${partitionFragments.mkString(",")})" +
            s" is not equal to the partition columns size(${partitionSchema.mkString(",")})")
          Array.empty
        } else { // If partitionSeqs.length == partitionSchema.fields.length

          // Append partition name to the partition value if the
          // HIVE_STYLE_PARTITIONING is disable.
          // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
          val partitionWithName =
          partitionFragments.zip(partitionSchema).map {
            case (partition, columnName) =>
              if (partition.indexOf("=") == -1) {
                s"${columnName}=$partition"
              } else {
                partition
              }
          }.mkString("/")
          val pathWithPartitionName = new Path(basePath, partitionWithName)
          val partitionValues = parsePartitionValuesFromPath(pathWithPartitionName)
            .getOrElse(Seq())
            .map(_._2)
            .toArray

          partitionValues
        }
      }
      PartitionRowPath(partitionRow, partitionPath)
    }
  }

  protected def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh0()
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
  private def loadPartitionPathFiles(): Map[PartitionRowPath, Array[FileStatus]] = {
    val partitionRowPaths = getAllQueryPartitionPaths
    // List files in all of the partition path.
    val pathToFetch = mutable.ArrayBuffer[PartitionRowPath]()
    val cachePartitionToFiles = mutable.Map[PartitionRowPath, Array[FileStatus]]()
    // Fetch from the FileStatusCache
    partitionRowPaths.foreach { partitionRowPath =>
      fileStatusCache.getLeafFiles(partitionRowPath.fullPartitionPath(basePath)) match {
        case Some(filesInPartition) =>
          cachePartitionToFiles.put(partitionRowPath, filesInPartition)

        case None => pathToFetch.append(partitionRowPath)
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
        Map.empty[PartitionRowPath, Array[FileStatus]]
      }

    // Update the fileStatusCache
    fetchedPartitionToFiles.foreach {
      case (partitionRowPath, filesInPartition) =>
        fileStatusCache.putLeafFiles(partitionRowPath.fullPartitionPath(basePath), filesInPartition)
    }
    cachePartitionToFiles.toMap ++ fetchedPartitionToFiles
  }

  private def refresh0(): Unit = {
    val startTime = System.currentTimeMillis()
    val partitionFiles = loadPartitionPathFiles()
    val allFiles = partitionFiles.values.reduceOption(_ ++ _)
      .getOrElse(Array.empty[FileStatus])

    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val latestInstant = activeInstants.lastInstant()
    // TODO we can optimize the flow by:
    //  - First fetch list of files from instants of interest
    //  - Load FileStatus's
    fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants, allFiles)
    val queryInstant = if (specifiedQueryInstant.isDefined) {
      specifiedQueryInstant
    } else if (latestInstant.isPresent) {
      Some(latestInstant.get.getTimestamp)
    } else {
      None
    }

    (tableType, queryType) match {
      case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL) =>
        // Fetch and store latest base and log files, and their sizes
        cachedAllInputFileSlices = partitionFiles.map(p => {
          // TODO conditional is incorrect -- this should be queryInstant
          val latestSlices = if (latestInstant.isPresent) {
            fileSystemView.getLatestMergedFileSlicesBeforeOrOn(p._1.partitionPath, queryInstant.get)
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
              fileSystemView.getLatestFileSlicesBeforeOrOn(p._1.partitionPath, instant, true))
            .getOrElse(fileSystemView.getLatestFileSlices(p._1.partitionPath))
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

  // TODO java-doc
  protected def parsePartitionValuesFromPath(pathWithPartitionName: Path): Option[Seq[(String, Any)]]

  // TODO eval whether we should just use logger directly
  protected def logWarning(str: => String): Unit
  protected def logInfo(str: => String): Unit

  /**
   * Represent a partition path.
   * e.g. PartitionPath(Array("2021","02","01"), "2021/02/01"))
   *
   * @param values        The partition values of this partition path.
   * @param partitionPath The partition path string.
   */
  case class PartitionRowPath(values: Array[Any], partitionPath: String) {
    override def equals(other: Any): Boolean = other match {
      case PartitionRowPath(_, otherPath) => partitionPath == otherPath
      case _ => false
    }

    override def hashCode(): Int = {
      partitionPath.hashCode
    }

    def fullPartitionPath(basePath: String): Path = {
      if (partitionPath.isEmpty) {
        new Path(basePath) // This is a non-partition path
      } else {
        new Path(basePath, partitionPath)
      }
    }
  }
}
