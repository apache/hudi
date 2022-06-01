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

package org.apache.spark.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.HoodieHadoopFSUtils
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HoodieInMemoryFileIndex(sparkSession: SparkSession,
                              rootPathsSpecified: Seq[Path],
                              parameters: Map[String, String],
                              userSpecifiedSchema: Option[StructType],
                              fileStatusCache: FileStatusCache = NoopCache)
  extends InMemoryFileIndex(sparkSession, rootPathsSpecified, parameters, userSpecifiedSchema, fileStatusCache) {

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is unpartitioned,
   * this will return a single partition with no partition values
   *
   * NOTE: This method replicates the one it overrides, however it uses custom method
   * that accepts files starting with "."
   */
  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      PartitionDirectory(InternalRow.empty, allFiles().filter(f => isDataPath(f.getPath))) :: Nil
    } else {
      prunePartitions(partitionFilters, partitionSpec()).map {
        case PartitionPath(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => isDataPath(f.getPath))

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          PartitionDirectory(values, files)
      }
    }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !(name.startsWith("_") && !name.contains("="))
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery threshold.
   *
   * This is publicly visible for testing.
   *
   * NOTE: This method replicates the one it overrides, however it uses custom method to run parallel
   *       listing that accepts files starting with "."
   */
  override def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    val startTime = System.nanoTime()
    val output = mutable.LinkedHashSet[FileStatus]()
    val pathsToFetch = mutable.ArrayBuffer[Path]()
    for (path <- paths) {
      fileStatusCache.getLeafFiles(path) match {
        case Some(files) =>
          HiveCatalogMetrics.incrementFileCacheHits(files.length)
          output ++= files
        case None =>
          pathsToFetch += path
      }
      () // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))
    val discovered = bulkListLeafFiles(sparkSession, pathsToFetch, filter, hadoopConf)

    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)
      output ++= leafFiles
    }

    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf files" +
      s" for ${paths.length} paths.")

    output
  }

  protected def bulkListLeafFiles(sparkSession: SparkSession, paths: ArrayBuffer[Path], filter: PathFilter, hadoopConf: Configuration): Seq[(Path, Seq[FileStatus])] = {
    HoodieHadoopFSUtils.parallelListLeafFiles(
      sc = sparkSession.sparkContext,
      paths = paths,
      hadoopConf = hadoopConf,
      filter = new PathFilterWrapper(filter),
      ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles,
      // NOTE: We're disabling fetching Block Info to speed up file listing
      ignoreLocality = true,
      parallelismThreshold = sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold,
      parallelismMax = sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism)
  }
}

object HoodieInMemoryFileIndex {
  def create(sparkSession: SparkSession, globbedPaths: Seq[Path]): HoodieInMemoryFileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new HoodieInMemoryFileIndex(sparkSession, globbedPaths, Map(), Option.empty, fileStatusCache)
  }
}

private class PathFilterWrapper(val filter: PathFilter) extends PathFilter with Serializable {
  override def accept(path: Path): Boolean = {
    (filter == null || filter.accept(path)) && !HoodieHadoopFSUtils.shouldFilterOutPathName(path.getName)
  }
}
