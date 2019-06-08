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

package com.uber.hoodie.table;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.SyncableFileSystemView;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.ConsistencyGuard;
import com.uber.hoodie.common.util.ConsistencyGuard.FileVisibility;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.FailSafeConsistencyGuard;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieSavepointException;
import com.uber.hoodie.index.HoodieIndex;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Abstract implementation of a HoodieTable
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {

  private static Logger logger = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndex<T> index;

  private SerializableConfiguration hadoopConfiguration;
  private transient FileSystemViewManager viewManager;

  protected HoodieTable(HoodieWriteConfig config, JavaSparkContext jsc) {
    this.config = config;
    this.hadoopConfiguration = new SerializableConfiguration(jsc.hadoopConfiguration());
    this.viewManager = FileSystemViewManager.createViewManager(
        new SerializableConfiguration(jsc.hadoopConfiguration()), config.getViewStorageConfig());
    this.metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true);
    this.index = HoodieIndex.createIndex(config, jsc);
  }

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(hadoopConfiguration,
          config.getViewStorageConfig());
    }
    return viewManager;
  }

  public static <T extends HoodieRecordPayload> HoodieTable<T> getHoodieTable(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, JavaSparkContext jsc) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieCopyOnWriteTable<>(config, jsc);
      case MERGE_ON_READ:
        return new HoodieMergeOnReadTable<>(config, jsc);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Provides a partitioner to perform the upsert operation, based on the workload profile
   */
  public abstract Partitioner getUpsertPartitioner(WorkloadProfile profile);

  /**
   * Provides a partitioner to perform the insert operation, based on the workload profile
   */
  public abstract Partitioner getInsertPartitioner(WorkloadProfile profile);

  /**
   * Return whether this HoodieTable implementation can benefit from workload profiling
   */
  public abstract boolean isWorkloadProfileNeeded();

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Configuration getHadoopConf() {
    return metaClient.getHadoopConf();
  }

  /**
   * Get the view of the file system for this table
   */
  public TableFileSystemView getFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitsTimeline());
  }

  /**
   * Get the read optimized view of the file system for this table
   */
  public TableFileSystemView.ReadOptimizedView getROFileSystemView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get the real time view of the file system for this table
   */
  public TableFileSystemView.RealtimeView getRTFileSystemView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get complete view of the file system for this table with ability to force sync
   */
  public SyncableFileSystemView getHoodieView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return metaClient.getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) commit timeline
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return metaClient.getCommitTimeline().filterCompletedInstants();
  }

  /**
   * Get only the inflights (no-completed) commit timeline
   */
  public HoodieTimeline getInflightCommitTimeline() {
    return metaClient.getCommitsTimeline().filterInflightsExcludingCompaction();
  }

  /**
   * Get only the completed (no-inflights) clean timeline
   */
  public HoodieTimeline getCompletedCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) savepoint timeline
   */
  public HoodieTimeline getCompletedSavepointTimeline() {
    return getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
  }

  /**
   * Get the list of savepoints in this table
   */
  public List<String> getSavepoints() {
    return getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }

  /**
   * Get the list of data file names savepointed
   */
  public Stream<String> getSavepointedDataFiles(String savepointTime) {
    if (!getSavepoints().contains(savepointTime)) {
      throw new HoodieSavepointException(
          "Could not get data files for savepoint " + savepointTime + ". No such savepoint.");
    }
    HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION,
        savepointTime);
    HoodieSavepointMetadata metadata = null;
    try {
      metadata = AvroUtils
          .deserializeHoodieSavepointMetadata(getActiveTimeline().getInstantDetails(instant).get());
    } catch (IOException e) {
      throw new HoodieSavepointException(
          "Could not get savepointed data files for savepoint " + savepointTime, e);
    }
    return metadata.getPartitionMetadata().values().stream()
        .flatMap(s -> s.getSavepointDataFile().stream());
  }

  public HoodieActiveTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline();
  }

  /**
   * Return the index
   */
  public HoodieIndex<T> getIndex() {
    return index;
  }

  /**
   * Perform the ultimate IO for a given upserted (RDD) partition
   */
  public abstract Iterator<List<WriteStatus>> handleUpsertPartition(String commitTime,
      Integer partition, Iterator<HoodieRecord<T>> recordIterator, Partitioner partitioner);

  /**
   * Perform the ultimate IO for a given inserted (RDD) partition
   */
  public abstract Iterator<List<WriteStatus>> handleInsertPartition(String commitTime,
      Integer partition, Iterator<HoodieRecord<T>> recordIterator, Partitioner partitioner);

  /**
   * Schedule compaction for the instant time
   * @param jsc         Spark Context
   * @param instantTime Instant Time for scheduling compaction
   * @return
   */
  public abstract HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String instantTime);

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data
   * access
   *
   * @param jsc                   Spark Context
   * @param compactionInstantTime Instant Time
   * @param compactionPlan        Compaction Plan
   */
  public abstract JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionInstantTime,
      HoodieCompactionPlan compactionPlan);

  /**
   * Clean partition paths according to cleaning policy and returns the number of files cleaned.
   */
  public abstract List<HoodieCleanStat> clean(JavaSparkContext jsc);

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Four steps: (1)
   * Atomically unpublish this commit (2) clean indexing data (3) clean new generated parquet files
   * / log blocks (4) Finally, delete .<action>.commit or .<action>.inflight file if deleteInstants = true
   */
  public abstract List<HoodieRollbackStat> rollback(JavaSparkContext jsc, String commit, boolean deleteInstants)
      throws IOException;

  /**
   * Finalize the written data onto storage. Perform any final cleanups
   *
   * @param jsc Spark Context
   * @param stats List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    cleanFailedWrites(jsc, instantTs, stats, config.isConsistencyCheckEnabled());
  }

  /**
   * Delete Marker directory corresponding to an instant
   * @param instantTs Instant Time
   */
  protected void deleteMarkerDir(String instantTs) {
    try {
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));
      if (fs.exists(markerDir)) {
        // For append only case, we do not write to marker dir. Hence, the above check
        logger.info("Removing marker directory=" + markerDir);
        fs.delete(markerDir, true);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Reconciles WriteStats and marker files to detect and safely delete duplicate data files created because of Spark
   * retries.
   *
   * @param jsc       Spark Context
   * @param instantTs Instant Timestamp
   * @param stats   Hoodie Write Stat
   * @param consistencyCheckEnabled  Consistency Check Enabled
   * @throws HoodieIOException
   */
  protected void cleanFailedWrites(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats,
      boolean consistencyCheckEnabled) throws HoodieIOException {
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));

      if (!fs.exists(markerDir)) {
        // Happens when all writes are appends
        return;
      }

      List<String> invalidDataPaths = FSUtils.getAllDataFilesForMarkers(fs, basePath, instantTs, markerDir.toString());
      List<String> validDataPaths = stats.stream().map(w -> String.format("%s/%s", basePath, w.getPath()))
          .filter(p -> p.endsWith(".parquet")).collect(Collectors.toList());
      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);
      logger.warn("InValid data paths=" + invalidDataPaths);

      Map<String, List<Pair<String, String>>> groupByPartition = invalidDataPaths.stream()
          .map(dp -> Pair.of(new Path(dp).getParent().toString(), dp))
          .collect(Collectors.groupingBy(Pair::getKey));

      if (!groupByPartition.isEmpty()) {
        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(jsc, groupByPartition, FileVisibility.APPEAR);
        }

        // Now delete partially written files
        jsc.parallelize(new ArrayList<>(groupByPartition.values()), config.getFinalizeWriteParallelism())
            .map(partitionWithFileList -> {
              final FileSystem fileSystem = metaClient.getFs();
              logger.info("Deleting invalid data files=" + partitionWithFileList);
              if (partitionWithFileList.isEmpty()) {
                return true;
              }
              // Delete
              partitionWithFileList.stream().map(Pair::getValue).forEach(file -> {
                try {
                  fileSystem.delete(new Path(file), false);
                } catch (IOException e) {
                  throw new HoodieIOException(e.getMessage(), e);
                }
              });

              return true;
            }).collect();

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(jsc, groupByPartition, FileVisibility.DISAPPEAR);
        }
      }
      // Now delete the marker directory
      deleteMarkerDir(instantTs);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Ensures all files passed either appear or disappear
   * @param jsc   JavaSparkContext
   * @param groupByPartition Files grouped by partition
   * @param visibility Appear/Disappear
   */
  private void waitForAllFiles(JavaSparkContext jsc, Map<String, List<Pair<String, String>>> groupByPartition,
      FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    boolean checkPassed =
        jsc.parallelize(new ArrayList<>(groupByPartition.entrySet()), config.getFinalizeWriteParallelism())
            .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
                partitionWithFileList.getValue().stream(), visibility))
            .collect().stream().allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

  private boolean waitForCondition(String partitionPath, Stream<Pair<String, String>> partitionFilePaths,
      FileVisibility visibility) {
    final FileSystem fileSystem = metaClient.getFs();
    List<String> fileList = partitionFilePaths.map(Pair::getValue).collect(Collectors.toList());
    try {
      getFailSafeConsistencyGuard(fileSystem).waitTill(partitionPath, fileList, visibility);
    } catch (IOException | TimeoutException ioe) {
      logger.error("Got exception while waiting for files to show up", ioe);
      return false;
    }
    return true;
  }

  private ConsistencyGuard getFailSafeConsistencyGuard(FileSystem fileSystem) {
    return new FailSafeConsistencyGuard(fileSystem, config.getMaxConsistencyChecks(),
        config.getInitialConsistencyCheckIntervalMs(),
        config.getMaxConsistencyCheckIntervalMs());
  }
}
