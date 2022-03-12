package org.apache.spark.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.HoodieHadoopFSUtils
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.HadoopFSUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HoodieInMemoryFileIndex(sparkSession: SparkSession,
                              rootPathsSpecified: Seq[Path],
                              parameters: Map[String, String],
                              userSpecifiedSchema: Option[StructType],
                              fileStatusCache: FileStatusCache = NoopCache,
                              userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
                              override val metadataOpsTimeNs: Option[Long] = None)
  extends InMemoryFileIndex(sparkSession, rootPathsSpecified, parameters, userSpecifiedSchema, fileStatusCache,
    userSpecifiedPartitionSpec, metadataOpsTimeNs) {

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
      ignoreLocality = sparkSession.sessionState.conf.ignoreDataLocality,
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
    (filter == null || filter.accept(path)) && !HadoopFSUtils.shouldFilterOutPathName(path.getName)
  }
}
