/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.profile;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sink.partitioner.BucketAssigner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Profiling of write statistics for {@link BucketAssigner},
 * such as the average record size and small files.
 *
 * <p>The profile is re-constructed when there are new commits on the timeline.
 */
public class WriteProfile {
  private static final Logger LOG = LoggerFactory.getLogger(WriteProfile.class);

  /**
   * The write config.
   */
  protected final HoodieWriteConfig config;

  /**
   * Table base path.
   */
  private final Path basePath;

  /**
   * The meta client.
   */
  protected final HoodieTableMetaClient metaClient;

  /**
   * The average record size.
   */
  private long avgSize = -1L;

  /**
   * Total records to write for each bucket based on
   * the config option {@link org.apache.hudi.config.HoodieStorageConfig#PARQUET_MAX_FILE_SIZE}.
   */
  private long recordsPerBucket;

  /**
   * Partition path to small files mapping.
   */
  private final Map<String, List<SmallFile>> smallFilesMap;

  /**
   * Checkpoint id to avoid redundant reload.
   */
  private long reloadedCheckpointId;

  /**
   * The file system view cache for one checkpoint interval.
   */
  protected SyncableFileSystemView fsView;

  /**
   * Metadata cache to reduce IO of metadata files.
   */
  private final Map<String, HoodieCommitMetadata> metadataCache;

  /**
   * The engine context.
   */
  private final HoodieFlinkEngineContext context;

  public WriteProfile(HoodieWriteConfig config, HoodieFlinkEngineContext context) {
    this.config = config;
    this.context = context;
    this.basePath = new Path(config.getBasePath());
    this.smallFilesMap = new HashMap<>();
    this.recordsPerBucket = config.getCopyOnWriteInsertSplitSize();
    this.metaClient = StreamerUtil.createMetaClient(config.getBasePath(), context.getHadoopConf().get());
    this.metadataCache = new HashMap<>();
    this.fsView = getFileSystemView();
    // profile the record statistics on construction
    recordProfile();
  }

  public long getAvgSize() {
    return avgSize;
  }

  public long getRecordsPerBucket() {
    return recordsPerBucket;
  }

  public HoodieTableMetaClient getMetaClient() {
    return this.metaClient;
  }

  protected HoodieTable<?, ?, ?, ?> getTable() {
    return HoodieFlinkTable.create(config, context);
  }

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  private long averageBytesPerRecord() {
    long avgSize = config.getCopyOnWriteRecordSizeEstimate();
    long fileSizeThreshold = (long) (config.getRecordSizeEstimationThreshold() * config.getParquetSmallFileLimit());
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    if (!commitTimeline.empty()) {
      // Go over the reverse ordered commits to get a more recent estimate of average record size.
      Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
      while (instants.hasNext()) {
        HoodieInstant instant = instants.next();
        final HoodieCommitMetadata commitMetadata =
            this.metadataCache.computeIfAbsent(
                instant.getTimestamp(),
                k -> WriteProfiles.getCommitMetadataSafely(config.getTableName(), basePath, instant, commitTimeline)
                    .orElse(null));
        if (commitMetadata == null) {
          continue;
        }
        long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
        long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
        if (totalBytesWritten > fileSizeThreshold && totalRecordsWritten > 0) {
          avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
          break;
        }
      }
    }
    LOG.info("Refresh average bytes per record => " + avgSize);
    return avgSize;
  }

  /**
   * Returns a list of small files in the given partition path.
   *
   * <p>Note: This method should be thread safe.
   */
  public synchronized List<SmallFile> getSmallFiles(String partitionPath) {
    // lookup the cache first
    if (smallFilesMap.containsKey(partitionPath)) {
      return smallFilesMap.get(partitionPath);
    }

    List<SmallFile> smallFiles = new ArrayList<>();
    if (config.getParquetSmallFileLimit() <= 0) {
      this.smallFilesMap.put(partitionPath, smallFiles);
      return smallFiles;
    }

    smallFiles = smallFilesProfile(partitionPath);
    this.smallFilesMap.put(partitionPath, smallFiles);
    return smallFiles;
  }

  /**
   * Returns a list of small files in the given partition path from the latest filesystem view.
   */
  protected List<SmallFile> smallFilesProfile(String partitionPath) {
    // smallFiles only for partitionPath
    List<SmallFile> smallFileLocations = new ArrayList<>();

    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();

    if (!commitTimeline.empty()) { // if we have some commits
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      List<HoodieBaseFile> allFiles = fsView
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        // filter out the corrupted files.
        if (file.getFileSize() < config.getParquetSmallFileLimit() && file.getFileSize() > 0) {
          String filename = file.getFileName();
          SmallFile sf = new SmallFile();
          sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
          sf.sizeBytes = file.getFileSize();
          smallFileLocations.add(sf);
        }
      }
    }

    return smallFileLocations;
  }

  protected SyncableFileSystemView getFileSystemView() {
    return (SyncableFileSystemView) getTable().getBaseFileOnlyView();
  }

  /**
   * Remove the overdue metadata from the cache
   * whose instant does not belong to the given instants {@code instants}.
   */
  private void cleanMetadataCache(Stream<HoodieInstant> instants) {
    Set<String> timestampSet = instants.map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    this.metadataCache.keySet().retainAll(timestampSet);
  }

  private void recordProfile() {
    this.avgSize = averageBytesPerRecord();
    if (config.shouldAllowMultiWriteOnSameInstant()) {
      this.recordsPerBucket = config.getParquetMaxFileSize() / avgSize;
      LOG.info("Refresh insert records per bucket => " + recordsPerBucket);
    }
  }

  /**
   * Reload the write profile, should do once for each checkpoint.
   *
   * <p>We do these things: i). reload the timeline; ii). re-construct the record profile;
   * iii) clean the small files cache.
   *
   * <p>Note: This method should be thread safe.
   */
  public synchronized void reload(long checkpointId) {
    if (this.reloadedCheckpointId >= checkpointId) {
      // already reloaded
      return;
    }
    this.metaClient.reloadActiveTimeline();
    // release the old fs view and create a new one
    SyncableFileSystemView oldFsView = this.fsView;
    this.fsView = getFileSystemView();
    oldFsView.close();

    recordProfile();
    cleanMetadataCache(this.metaClient.getCommitsTimeline().filterCompletedInstants().getInstants());
    this.smallFilesMap.clear();
    this.reloadedCheckpointId = checkpointId;
  }

  @VisibleForTesting
  public Map<String, HoodieCommitMetadata> getMetadataCache() {
    return this.metadataCache;
  }
}
