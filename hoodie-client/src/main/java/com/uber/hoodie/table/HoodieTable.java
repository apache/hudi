/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.table;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieSavepointException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Abstract implementation of a HoodieTable
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;

  private static Logger logger = LogManager.getLogger(HoodieTable.class);

  protected HoodieTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.metaClient = metaClient;
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
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitTimeline());
  }

  /**
   * Get the read optimized view of the file system for this table
   */
  public TableFileSystemView.ReadOptimizedView getROFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitTimeline());
  }

  /**
   * Get the real time view of the file system for this table
   */
  public TableFileSystemView.RealtimeView getRTFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitTimeline());
  }

  /**
   * Get the completed (commit + compaction) view of the file system for this table
   */
  public TableFileSystemView getCompletedFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCommitsTimeline());
  }

  /**
   * Get only the completed (no-inflights) commit timeline
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get only the inflights (no-completed) commit timeline
   */
  public HoodieTimeline getInflightCommitTimeline() {
    return getCommitsTimeline().filterInflights();
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
    HoodieInstant instant =
        new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    HoodieSavepointMetadata metadata = null;
    try {
      metadata = AvroUtils.deserializeHoodieSavepointMetadata(
          getActiveTimeline().getInstantDetails(instant).get());
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
   * Get the commit timeline visible for this table
   */
  public HoodieTimeline getCommitsTimeline() {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return getActiveTimeline().getCommitTimeline();
      case MERGE_ON_READ:
        // We need to include the parquet files written out in delta commits
        // Include commit action to be able to start doing a MOR over a COW dataset - no migration required
        return getActiveTimeline().getCommitsTimeline();
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Get the compacted commit timeline visible for this table
   */
  public HoodieTimeline getCommitTimeline() {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
      case MERGE_ON_READ:
        // We need to include the parquet files written out in delta commits in tagging
        return getActiveTimeline().getCommitTimeline();
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Gets the commit action type
   */
  public String getCommitActionType() {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return HoodieActiveTimeline.COMMIT_ACTION;
      case MERGE_ON_READ:
        return HoodieActiveTimeline.DELTA_COMMIT_ACTION;
    }
    throw new HoodieCommitException(
        "Could not commit on unknown storage type " + metaClient.getTableType());
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


  public static <T extends HoodieRecordPayload> HoodieTable<T> getHoodieTable(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieCopyOnWriteTable<>(config, metaClient);
      case MERGE_ON_READ:
        return new HoodieMergeOnReadTable<>(config, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data
   * access
   */
  public abstract Optional<HoodieCommitMetadata> compact(JavaSparkContext jsc,
                                                         String commitCompactionTime);

  /**
   * Clean partition paths according to cleaning policy and returns the number of files cleaned.
   */
  public abstract List<HoodieCleanStat> clean(JavaSparkContext jsc);

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Four steps: (1)
   * Atomically unpublish this commit (2) clean indexing data (3) clean new generated parquet files
   * / log blocks (4) Finally, delete .<action>.commit or .<action>.inflight file
   */
  public abstract List<HoodieRollbackStat> rollback(JavaSparkContext jsc, List<String> commits)
      throws IOException;

  /**
   * Finalize the written data files
   *
   * @param writeStatuses List of WriteStatus
   * @return number of files finalized
   */
  public abstract Optional<Integer> finalizeWrite(JavaSparkContext jsc, List<Tuple2<String, HoodieWriteStat>> writeStatuses);
}
