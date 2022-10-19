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

package org.apache.spark.sql.hudi.command

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.PartitionStatistics
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.util.control.NonFatal

/**
 * Command for repair hudi table's partitions.
 * Most of the code is copied from spark3.3, the main change is scanPartitions() when disable isHiveStyledPartitioning
 */
case class RepairHoodieTableCommand(tableName: TableIdentifier,
                                    enableAddPartitions: Boolean,
                                    enableDropPartitions: Boolean,
                                    cmd: String = "MSCK REPAIR TABLE") extends HoodieLeafRunnableCommand {

  // These are list of statistics that can be collected quickly without requiring a scan of the data
  // see https://github.com/apache/hive/blob/master/
  //   common/src/java/org/apache/hadoop/hive/common/StatsSetupConst.java
  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  private def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    new PathFilter {
      override def accept(path: Path): Boolean = {
        val name = path.getName
        if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
          pathFilter == null || pathFilter.accept(path)
        } else {
          false
        }
      }
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString
    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
    }

    if (table.storage.locationUri.isEmpty) {
      throw new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
        s"location provided: $tableIdentWithDB")
    }

    val root = new Path(table.location)
    logInfo(s"Recover all the partitions in $root")
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = root.getFileSystem(hadoopConf)

    val droppedAmount = if (enableDropPartitions) {
      dropPartitions(catalog, fs)
    } else 0
    val addedAmount = if (enableAddPartitions) {
      val threshold = spark.sparkContext.conf.getInt("spark.rdd.parallelListingThreshold", 10)
      val pathFilter = getPathFilter(hadoopConf)

      val evalPool = ThreadUtils.newForkJoinPool("RepairTableCommand", 8)
      val hoodieCatalogTable = HoodieCatalogTable(spark, table)
      val isHiveStyledPartitioning = hoodieCatalogTable.catalogProperties.getOrElse(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key, "true")
      val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] =
        try {
          scanPartitions(spark, fs, pathFilter, root, Map(), table.partitionColumnNames, threshold,
            spark.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool), isHiveStyledPartitioning.equals("true")).seq
        } finally {
          evalPool.shutdown()
        }
      val total = partitionSpecsAndLocs.length
      logInfo(s"Found $total partitions in $root")

      val partitionStats = if (spark.sqlContext.conf.gatherFastStats) {
        gatherPartitionStats(spark, partitionSpecsAndLocs, fs, pathFilter, threshold)
      } else {
        Map.empty[String, PartitionStatistics]
      }
      logInfo(s"Finished to gather the fast stats for all $total partitions.")

      addPartitions(spark, table, partitionSpecsAndLocs, partitionStats)
      total
    } else 0
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    spark.sessionState.catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    try {
      spark.catalog.refreshTable(tableIdentWithDB)
    } catch {
      case NonFatal(e) =>
        logError(s"Cannot refresh the table '$tableIdentWithDB'. A query of the table " +
          "might return wrong result if the table was cached. To avoid such issue, you should " +
          "uncache the table manually via the UNCACHE TABLE command after table recovering will " +
          "complete fully.", e)
    }
    logInfo(s"Recovered all partitions: added ($addedAmount), dropped ($droppedAmount).")
    Seq.empty[Row]
  }

  private def scanPartitions(spark: SparkSession,
                             fs: FileSystem,
                             filter: PathFilter,
                             path: Path,
                             spec: TablePartitionSpec,
                             partitionNames: Seq[String],
                             threshold: Int,
                             resolver: Resolver,
                             evalTaskSupport: ForkJoinTaskSupport,
                             isHiveStyledPartitioning: Boolean): Seq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: Seq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = new ParVector(statuses.toVector)
        parArray.tasksupport = evalTaskSupport
        parArray.seq
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      (st.isDirectory, isHiveStyledPartitioning, name.contains("=")) match {
        case (true, true, true) =>
          val ps = name.split("=", 2)
          val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
          // TODO: Validate the value
          val value = ExternalCatalogUtils.unescapePathName(ps(1))
          if (resolver(columnName, partitionNames.head)) {
            scanPartitions(spark, fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
              partitionNames.drop(1), threshold, resolver, evalTaskSupport, isHiveStyledPartitioning)
          } else {
            logWarning(
              s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
            Seq.empty
          }
        case (true, false, _) =>
          // TODO: Validate the value
          val value = ExternalCatalogUtils.unescapePathName(name)
          scanPartitions(spark, fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver, evalTaskSupport, isHiveStyledPartitioning)
        case _ =>
          logWarning(s"ignore ${new Path(path, name)}")
          Seq.empty
      }
    }
  }

  private def gatherPartitionStats(spark: SparkSession,
                                   partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
                                   fs: FileSystem,
                                   pathFilter: PathFilter,
                                   threshold: Int): Map[String, PartitionStatistics] = {
    if (partitionSpecsAndLocs.length > threshold) {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val serializedPaths = partitionSpecsAndLocs.map(_._2.toString).toArray

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(serializedPaths.length,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      spark.sparkContext.parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          paths.map(new Path(_)).map { path =>
            val fs = path.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(path, pathFilter)
            (path.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap().toMap
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def addPartitions(spark: SparkSession,
                            table: CatalogTable,
                            partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
                            partitionStats: Map[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = spark.sparkContext.conf.getInt("spark.sql.addPartitionInBatch.size", 100)
    partitionSpecsAndLocs.iterator.grouped(batchSize).foreach { batch =>
      val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri)),
          params)
      }
      spark.sessionState.catalog.createPartitions(tableName, parts, ignoreIfExists = true)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }

  // Drops the partitions that do not exist in the file system
  private def dropPartitions(catalog: SessionCatalog, fs: FileSystem): Int = {
    val dropPartSpecs = ThreadUtils.parmap(
      catalog.listPartitions(tableName),
      "RepairTableCommand: non-existing partitions",
      maxThreads = 8) { partition =>
      partition.storage.locationUri.flatMap { uri =>
        if (fs.exists(new Path(uri))) None else Some(partition.spec)
      }
    }.flatten
    catalog.dropPartitions(
      tableName,
      dropPartSpecs,
      ignoreIfNotExists = true,
      purge = false,
      // Since we have already checked that partition directories do not exist, we can avoid
      // additional calls to the file system at the catalog side by setting this flag.
      retainData = true)
    dropPartSpecs.length
  }
}
