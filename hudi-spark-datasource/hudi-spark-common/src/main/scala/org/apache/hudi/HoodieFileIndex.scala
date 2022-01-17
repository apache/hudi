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
import org.apache.hudi.HoodieFileIndex.getConfigProperties
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.hudi.DataSkippingUtils.createColumnStatsIndexFilterExpr
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Column, SparkSession}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

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
 * TODO rename to HoodieSparkSqlFileIndex
 */
case class HoodieFileIndex(spark: SparkSession,
                           metaClient: HoodieTableMetaClient,
                           schemaSpec: Option[StructType],
                           options: Map[String, String],
                           @transient fileStatusCache: FileStatusCache = NoopCache)
  extends SparkHoodieTableFileIndex(
    spark = spark,
    metaClient = metaClient,
    schemaSpec = schemaSpec,
    configProperties = getConfigProperties(spark, options),
    specifiedQueryInstant = options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).map(HoodieSqlCommonUtils.formatQueryInstant),
    fileStatusCache = fileStatusCache
  )
    with FileIndex {

  @transient private val queryPath = new Path(options.getOrElse("path", "'path' option required"))

  override def rootPaths: Seq[Path] = queryPath :: Nil

  def enableDataSkipping(): Boolean = {
    options.getOrElse(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(),
      spark.sessionState.conf.getConfString(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(), "false")).toBoolean
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

  /**
   * Invoked by Spark to fetch list of latest base files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters      data columns filters
   * @return list of PartitionDirectory containing partition to base files mapping
   */
  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // Look up candidate files names in the col-stats index, if all of the following conditions are true
    //    - Data-skipping is enabled
    //    - Col-Stats Index is present
    //    - List of predicates (filters) is present
    val candidateFilesNamesOpt: Option[Set[String]] =
    lookupCandidateFilesInColStatsIndex(dataFilters) match {
      case Success(opt) => opt
      case Failure(e) =>
        if (e.isInstanceOf[AnalysisException]) {
          logDebug("Failed to relay provided data filters to Z-index lookup", e)
        } else {
          logError("Failed to lookup candidate files in Z-index", e)
        }
        Option.empty
    }

    logDebug(s"Overlapping candidate files (from Z-index): ${candidateFilesNamesOpt.getOrElse(Set.empty)}")

    if (queryAsNonePartitionedTable) {
      // Read as Non-Partitioned table
      // Filter in candidate files based on the col-stats index lookup
      val candidateFiles =
      allFiles.filter(fileStatus =>
        // NOTE: This predicate is true when {@code Option} is empty
        candidateFilesNamesOpt.forall(_.contains(fileStatus.getPath.getName))
      )

      logInfo(s"Total files : ${allFiles.size}; " +
        s"candidate files after data skipping: ${candidateFiles.size}; " +
        s"skipping percent ${if (allFiles.nonEmpty) (allFiles.size - candidateFiles.size) / allFiles.size.toDouble else 0}")

      Seq(PartitionDirectory(InternalRow.empty, candidateFiles))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllInputFileSlices.keys.toSeq, partitionFilters)
      var totalFileSize = 0
      var candidateFileSize = 0

      val result = prunedPartitions.map { partition =>
        val baseFileStatuses: Seq[FileStatus] =
          cachedAllInputFileSlices(partition)
            .map(fs => fs.getBaseFile.orElse(null))
            .filter(_ != null)
            .map(_.getFileStatus)

        // Filter in candidate files based on the col-stats index lookup
        val candidateFiles =
          baseFileStatuses.filter(fs =>
            // NOTE: This predicate is true when {@code Option} is empty
            candidateFilesNamesOpt.forall(_.contains(fs.getPath.getName)))

        totalFileSize += baseFileStatuses.size
        candidateFileSize += candidateFiles.size
        PartitionDirectory(InternalRow.fromSeq(partition.values), candidateFiles)
      }

      logInfo(s"Total base files: ${totalFileSize}; " +
        s"candidate files after data skipping : ${candidateFileSize}; " +
        s"skipping percent ${if (allFiles.nonEmpty) (totalFileSize - candidateFileSize) / totalFileSize.toDouble else 0}")

      result
    }
  }

  private def lookupFileNamesMissingFromIndex(allIndexedFileNames: Set[String]) = {
    val allBaseFileNames = allFiles.map(f => f.getPath.getName).toSet
    allBaseFileNames -- allIndexedFileNames
  }

  /**
   * Computes pruned list of candidate base-files' names based on provided list of {@link dataFilters}
   * conditions, by leveraging custom Column Statistics index (col-stats-index) bearing "min", "max",
   * "num_nulls" statistics for all clustered columns.
   *
   * NOTE: This method has to return complete set of candidate files, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every base-file's name, that is NOT referenced in its index.
   *
   * @param queryFilters list of original data filters passed down from querying engine
   * @return list of pruned (data-skipped) candidate base-files' names
   */
  private def lookupCandidateFilesInColStatsIndex(queryFilters: Seq[Expression]): Try[Option[Set[String]]] = Try {
    val indexPath = metaClient.getColumnStatsIndexPath
    val fs = metaClient.getFs

    if (!enableDataSkipping() || !fs.exists(new Path(indexPath)) || queryFilters.isEmpty) {
      // scalastyle:off return
      return Success(Option.empty)
      // scalastyle:on return
    }

    // Collect all index tables present in `.zindex` folder
    val candidateIndexTables =
      fs.listStatus(new Path(indexPath))
        .filter(_.isDirectory)
        .map(_.getPath.getName)
        .filter(f => completedCommits.contains(f))
        .sortBy(x => x)

    if (candidateIndexTables.isEmpty) {
      // scalastyle:off return
      return Success(Option.empty)
      // scalastyle:on return
    }

    val dataFrameOpt = try {
      Some(spark.read.load(new Path(indexPath, candidateIndexTables.last).toString))
    } catch {
      case t: Throwable =>
        logError("Failed to read col-stats index; skipping", t)
        None
    }

    dataFrameOpt.map(df => {
      val indexSchema = df.schema
      val indexFilter =
        queryFilters.map(createColumnStatsIndexFilterExpr(_, indexSchema))
          .reduce(And)

      logInfo(s"Index filter condition: $indexFilter")

      df.persist()

      val allIndexedFileNames =
        df.select("file")
          .collect()
          .map(_.getString(0))
          .toSet

      val prunedCandidateFileNames =
        df.where(new Column(indexFilter))
          .select("file")
          .collect()
          .map(_.getString(0))
          .toSet

      df.unpersist()

      // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
      //       base-file: since it's bound to clustering, which could occur asynchronously
      //       at arbitrary point in time, and is not likely to be touching all of the base files.
      //
      //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
      //       files and all outstanding base-files, and make sure that all base files not
      //       represented w/in the index are included in the output of this method
      val notIndexedFileNames =
      lookupFileNamesMissingFromIndex(allIndexedFileNames)

      prunedCandidateFileNames ++ notIndexedFileNames
    })
  }

  override def refresh(): Unit = super.refresh()

  override def inputFiles: Array[String] = {
    val fileStatusList = allFiles
    fileStatusList.map(_.getPath.toString).toArray
  }

  override def sizeInBytes: Long = {
    cachedFileSize
  }
}

object HoodieFileIndex {

  def getConfigProperties(spark: SparkSession, options: Map[String, String]) = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = new TypedProperties()

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    properties.setProperty(HoodieMetadataConfig.ENABLE.key(),
      sqlConf.getConfString(HoodieMetadataConfig.ENABLE.key(),
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS.toString))
    properties.putAll(options.asJava)
    properties
  }
}
