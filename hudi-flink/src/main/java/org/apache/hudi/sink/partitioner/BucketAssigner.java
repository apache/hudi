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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.util.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Bucket assigner that assigns the data buffer of one checkpoint into buckets.
 *
 * <p>This assigner assigns the record one by one.
 * If the record is an update, checks and reuse existing UPDATE bucket or generates a new one;
 * If the record is an insert, checks the record partition for small files first, try to find a small file
 * that has space to append new records and reuse the small file's data bucket, if
 * there is no small file(or no left space for new records), generates an INSERT bucket.
 *
 * <p>Use {partition}_{fileId} as the bucket identifier, so that the bucket is unique
 * within and among partitions.
 */
public class BucketAssigner {
  private static final Logger LOG = LogManager.getLogger(BucketAssigner.class);

  /**
   * Remembers what type each bucket is for later.
   */
  private final HashMap<String, BucketInfo> bucketInfoMap;

  protected HoodieTable<?, ?, ?, ?> table;

  /**
   * Fink engine context.
   */
  private final HoodieFlinkEngineContext context;

  /**
   * The write config.
   */
  protected final HoodieWriteConfig config;

  /**
   * The average record size.
   */
  private final long averageRecordSize;

  /**
   * Total records to write for each bucket based on
   * the config option {@link org.apache.hudi.config.HoodieStorageConfig#PARQUET_FILE_MAX_BYTES}.
   */
  private final long insertRecordsPerBucket;

  /**
   * Partition path to small files mapping.
   */
  private final Map<String, List<SmallFile>> partitionSmallFilesMap;

  /**
   * Bucket ID(partition + fileId) -> small file assign state.
   */
  private final Map<String, SmallFileAssignState> smallFileAssignStates;

  /**
   * Bucket ID(partition + fileId) -> new file assign state.
   */
  private final Map<String, NewFileAssignState> newFileAssignStates;

  public BucketAssigner(
      HoodieFlinkEngineContext context,
      HoodieWriteConfig config) {
    bucketInfoMap = new HashMap<>();
    partitionSmallFilesMap = new HashMap<>();
    smallFileAssignStates = new HashMap<>();
    newFileAssignStates = new HashMap<>();
    this.context = context;
    this.config = config;
    this.table = HoodieFlinkTable.create(this.config, this.context);
    averageRecordSize = averageBytesPerRecord(
        table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        config);
    LOG.info("AvgRecordSize => " + averageRecordSize);
    insertRecordsPerBucket = config.shouldAutoTuneInsertSplits()
        ? config.getParquetMaxFileSize() / averageRecordSize
        : config.getCopyOnWriteInsertSplitSize();
    LOG.info("InsertRecordsPerBucket => " + insertRecordsPerBucket);
  }

  /**
   * Reset the states of this assigner, should do once for each checkpoint,
   * all the states are accumulated within one checkpoint interval.
   */
  public void reset() {
    bucketInfoMap.clear();
    partitionSmallFilesMap.clear();
    smallFileAssignStates.clear();
    newFileAssignStates.clear();
  }

  public BucketInfo addUpdate(String partitionPath, String fileIdHint) {
    final String key = StreamerUtil.generateBucketKey(partitionPath, fileIdHint);
    if (!bucketInfoMap.containsKey(key)) {
      BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdHint, partitionPath);
      bucketInfoMap.put(key, bucketInfo);
    }
    // else do nothing because the bucket already exists.
    return bucketInfoMap.get(key);
  }

  public BucketInfo addInsert(String partitionPath) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    List<SmallFile> smallFiles = getSmallFilesForPartition(partitionPath);

    // first try packing this into one of the smallFiles
    for (SmallFile smallFile : smallFiles) {
      final String key = StreamerUtil.generateBucketKey(partitionPath, smallFile.location.getFileId());
      SmallFileAssignState assignState = smallFileAssignStates.get(key);
      assert assignState != null;
      if (assignState.canAssign()) {
        assignState.assign();
        // create a new bucket or re-use an existing bucket
        BucketInfo bucketInfo;
        if (bucketInfoMap.containsKey(key)) {
          // Assigns an inserts to existing update bucket
          bucketInfo = bucketInfoMap.get(key);
        } else {
          bucketInfo = addUpdate(partitionPath, smallFile.location.getFileId());
        }
        return bucketInfo;
      }
    }

    // if we have anything more, create new insert buckets, like normal
    if (newFileAssignStates.containsKey(partitionPath)) {
      NewFileAssignState newFileAssignState = newFileAssignStates.get(partitionPath);
      if (newFileAssignState.canAssign()) {
        newFileAssignState.assign();
      }
      final String key = StreamerUtil.generateBucketKey(partitionPath, newFileAssignState.fileId);
      return bucketInfoMap.get(key);
    }
    BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, FSUtils.createNewFileIdPfx(), partitionPath);
    final String key = StreamerUtil.generateBucketKey(partitionPath, bucketInfo.getFileIdPrefix());
    bucketInfoMap.put(key, bucketInfo);
    newFileAssignStates.put(partitionPath, new NewFileAssignState(bucketInfo.getFileIdPrefix(), insertRecordsPerBucket));
    return bucketInfo;
  }

  private List<SmallFile> getSmallFilesForPartition(String partitionPath) {
    if (partitionSmallFilesMap.containsKey(partitionPath)) {
      return partitionSmallFilesMap.get(partitionPath);
    }
    List<SmallFile> smallFiles = getSmallFiles(partitionPath);
    if (smallFiles.size() > 0) {
      LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);
      partitionSmallFilesMap.put(partitionPath, smallFiles);
      smallFiles.forEach(smallFile ->
          smallFileAssignStates.put(
              StreamerUtil.generateBucketKey(partitionPath, smallFile.location.getFileId()),
              new SmallFileAssignState(config.getParquetMaxFileSize(), smallFile, averageRecordSize)));
      return smallFiles;
    }
    return Collections.emptyList();
  }

  /**
   * Refresh the table state like TableFileSystemView and HoodieTimeline.
   */
  public void refreshTable() {
    this.table = HoodieFlinkTable.create(this.config, this.context);
  }

  public HoodieTable<?, ?, ?, ?> getTable() {
    return table;
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  protected List<SmallFile> getSmallFiles(String partitionPath) {

    // smallFiles only for partitionPath
    List<SmallFile> smallFileLocations = new ArrayList<>();

    HoodieTimeline commitTimeline = table.getMetaClient().getCommitsTimeline().filterCompletedInstants();

    if (!commitTimeline.empty()) { // if we have some commits
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      List<HoodieBaseFile> allFiles = table.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        if (file.getFileSize() < config.getParquetSmallFileLimit()) {
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

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  protected static long averageBytesPerRecord(HoodieTimeline commitTimeline, HoodieWriteConfig hoodieWriteConfig) {
    long avgSize = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    long fileSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
          long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
          if (totalBytesWritten > fileSizeThreshold && totalRecordsWritten > 0) {
            avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
            break;
          }
        }
      }
    } catch (Throwable t) {
      // make this fail safe.
      LOG.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }

  /**
   * Candidate bucket state for small file. It records the total number of records
   * that the bucket can append and the current number of assigned records.
   */
  private static class SmallFileAssignState {
    long assigned;
    long totalUnassigned;

    SmallFileAssignState(long parquetMaxFileSize, SmallFile smallFile, long averageRecordSize) {
      this.assigned = 0;
      this.totalUnassigned = (parquetMaxFileSize - smallFile.sizeBytes) / averageRecordSize;
    }

    public boolean canAssign() {
      return this.totalUnassigned > 0 && this.totalUnassigned > this.assigned;
    }

    /**
     * Remembers to invoke {@link #canAssign()} first.
     */
    public void assign() {
      Preconditions.checkState(canAssign(),
          "Can not assign insert to small file: assigned => "
              + this.assigned + " totalUnassigned => " + this.totalUnassigned);
      this.assigned++;
    }
  }

  /**
   * Candidate bucket state for a new file. It records the total number of records
   * that the bucket can append and the current number of assigned records.
   */
  private static class NewFileAssignState {
    long assigned;
    long totalUnassigned;
    final String fileId;

    NewFileAssignState(String fileId, long insertRecordsPerBucket) {
      this.fileId = fileId;
      this.assigned = 0;
      this.totalUnassigned = insertRecordsPerBucket;
    }

    public boolean canAssign() {
      return this.totalUnassigned > 0 && this.totalUnassigned > this.assigned;
    }

    /**
     * Remembers to invoke {@link #canAssign()} first.
     */
    public void assign() {
      Preconditions.checkState(canAssign(),
          "Can not assign insert to new file: assigned => "
              + this.assigned + " totalUnassigned => " + this.totalUnassigned);
      this.assigned++;
    }
  }
}
