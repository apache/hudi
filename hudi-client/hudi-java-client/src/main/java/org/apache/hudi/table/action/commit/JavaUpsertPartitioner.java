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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Packs incoming records to be upserted, into buckets.
 */
public class JavaUpsertPartitioner<T> implements Partitioner  {

  private static final Logger LOG = LogManager.getLogger(JavaUpsertPartitioner.class);

  /**
   * List of all small files to be corrected.
   */
  protected List<SmallFile> smallFiles = new ArrayList<>();
  /**
   * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
   */
  private int totalBuckets = 0;
  /**
   * Stat for the input and output workload. Describe the workload before and after being assigned buckets.
   */
  private WorkloadProfile workloadProfile;
  /**
   * Helps decide which bucket an incoming update should go to.
   */
  private HashMap<String, Integer> updateLocationToBucket;
  /**
   * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
   */
  private HashMap<String, List<InsertBucketCumulativeWeightPair>> partitionPathToInsertBucketInfos;
  /**
   * Remembers what type each bucket is for later.
   */
  private HashMap<Integer, BucketInfo> bucketInfoMap;

  protected final HoodieTable table;

  protected final HoodieWriteConfig config;

  public JavaUpsertPartitioner(WorkloadProfile workloadProfile, HoodieEngineContext context, HoodieTable table,
                               HoodieWriteConfig config) {
    updateLocationToBucket = new HashMap<>();
    partitionPathToInsertBucketInfos = new HashMap<>();
    bucketInfoMap = new HashMap<>();
    this.workloadProfile = workloadProfile;
    this.table = table;
    this.config = config;
    assignUpdates(workloadProfile);
    assignInserts(workloadProfile, context);

    LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
        + "Partition to insert buckets => " + partitionPathToInsertBucketInfos + ", \n"
        + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
  }

  private void assignUpdates(WorkloadProfile profile) {
    // each update location gets a partition
    Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = profile.getInputPartitionPathStatMap().entrySet();
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      WorkloadStat outputWorkloadStats = profile.getOutputPartitionPathStatMap().getOrDefault(partitionStat.getKey(), new WorkloadStat());
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        addUpdateBucket(partitionStat.getKey(), updateLocEntry.getKey());
        if (profile.hasOutputWorkLoadStats()) {
          HoodieRecordLocation hoodieRecordLocation = new HoodieRecordLocation(updateLocEntry.getValue().getKey(), updateLocEntry.getKey());
          outputWorkloadStats.addUpdates(hoodieRecordLocation, updateLocEntry.getValue().getValue());
        }
      }
      if (profile.hasOutputWorkLoadStats()) {
        profile.updateOutputPartitionPathStatMap(partitionStat.getKey(), outputWorkloadStats);
      }
    }
  }

  private int addUpdateBucket(String partitionPath, String fileIdHint) {
    int bucket = totalBuckets;
    updateLocationToBucket.put(fileIdHint, bucket);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdHint, partitionPath);
    bucketInfoMap.put(totalBuckets, bucketInfo);
    totalBuckets++;
    return bucket;
  }

  private void assignInserts(WorkloadProfile profile, HoodieEngineContext context) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    Set<String> partitionPaths = profile.getPartitionPaths();
    long averageRecordSize =
        averageBytesPerRecord(table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
            config);
    LOG.info("AvgRecordSize => " + averageRecordSize);

    Map<String, List<SmallFile>> partitionSmallFilesMap =
        getSmallFilesForPartitions(new ArrayList<String>(partitionPaths), context);

    for (String partitionPath : partitionPaths) {
      WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
      WorkloadStat outputWorkloadStats = profile.getOutputPartitionPathStatMap().getOrDefault(partitionPath, new WorkloadStat());
      if (pStat.getNumInserts() > 0) {

        List<SmallFile> smallFiles = partitionSmallFilesMap.getOrDefault(partitionPath, new ArrayList<>());
        this.smallFiles.addAll(smallFiles);

        LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

        long totalUnassignedInserts = pStat.getNumInserts();
        List<Integer> bucketNumbers = new ArrayList<>();
        List<Long> recordsPerBucket = new ArrayList<>();

        // first try packing this into one of the smallFiles
        for (SmallFile smallFile : smallFiles) {
          long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
              totalUnassignedInserts);
          if (recordsToAppend > 0) {
            // create a new bucket or re-use an existing bucket
            int bucket;
            if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
              bucket = updateLocationToBucket.get(smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
            } else {
              bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
            }
            if (profile.hasOutputWorkLoadStats()) {
              outputWorkloadStats.addInserts(smallFile.location, recordsToAppend);
            }
            bucketNumbers.add(bucket);
            recordsPerBucket.add(recordsToAppend);
            totalUnassignedInserts -= recordsToAppend;
          }
        }

        // if we have anything more, create new insert buckets, like normal
        if (totalUnassignedInserts > 0) {
          long insertRecordsPerBucket = config.getCopyOnWriteInsertSplitSize();
          if (config.shouldAutoTuneInsertSplits()) {
            insertRecordsPerBucket = config.getParquetMaxFileSize() / averageRecordSize;
          }

          int insertBuckets = (int) Math.ceil((1.0 * totalUnassignedInserts) / insertRecordsPerBucket);
          LOG.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
              + ", totalInsertBuckets => " + insertBuckets + ", recordsPerBucket => " + insertRecordsPerBucket);
          for (int b = 0; b < insertBuckets; b++) {
            bucketNumbers.add(totalBuckets);
            if (b < insertBuckets - 1) {
              recordsPerBucket.add(insertRecordsPerBucket);
            } else {
              recordsPerBucket.add(totalUnassignedInserts - (insertBuckets - 1) * insertRecordsPerBucket);
            }
            BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, FSUtils.createNewFileIdPfx(), partitionPath);
            bucketInfoMap.put(totalBuckets, bucketInfo);
            if (profile.hasOutputWorkLoadStats()) {
              outputWorkloadStats.addInserts(new HoodieRecordLocation(HoodieWriteStat.NULL_COMMIT, bucketInfo.getFileIdPrefix()), recordsPerBucket.get(recordsPerBucket.size() - 1));
            }
            totalBuckets++;
          }
        }

        // Go over all such buckets, and assign weights as per amount of incoming inserts.
        List<InsertBucketCumulativeWeightPair> insertBuckets = new ArrayList<>();
        double currentCumulativeWeight = 0;
        for (int i = 0; i < bucketNumbers.size(); i++) {
          InsertBucket bkt = new InsertBucket();
          bkt.bucketNumber = bucketNumbers.get(i);
          bkt.weight = (1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts();
          currentCumulativeWeight += bkt.weight;
          insertBuckets.add(new InsertBucketCumulativeWeightPair(bkt, currentCumulativeWeight));
        }
        LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
        partitionPathToInsertBucketInfos.put(partitionPath, insertBuckets);
      }
      if (profile.hasOutputWorkLoadStats()) {
        profile.updateOutputPartitionPathStatMap(partitionPath, outputWorkloadStats);
      }
    }
  }

  private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths, HoodieEngineContext context) {
    Map<String, List<SmallFile>> partitionSmallFilesMap = new HashMap<>();

    if (config.getParquetSmallFileLimit() <= 0) {
      return partitionSmallFilesMap;
    }

    if (partitionPaths != null && partitionPaths.size() > 0) {
      context.setJobStatus(this.getClass().getSimpleName(), "Getting small files from partitions: " + config.getTableName());
      partitionSmallFilesMap = context.mapToPair(partitionPaths,
          partitionPath -> new ImmutablePair<>(partitionPath, getSmallFiles(partitionPath)), 0);
    }
    return partitionSmallFilesMap;
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

  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoMap.get(bucketNumber);
  }

  public List<InsertBucketCumulativeWeightPair> getInsertBuckets(String partitionPath) {
    return partitionPathToInsertBucketInfos.get(partitionPath);
  }

  @Override
  public int getNumPartitions() {
    return totalBuckets;
  }

  @Override
  public int getPartition(Object key) {
    Pair<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
        (Pair<HoodieKey, Option<HoodieRecordLocation>>) key;
    if (keyLocation.getRight().isPresent()) {
      HoodieRecordLocation location = keyLocation.getRight().get();
      return updateLocationToBucket.get(location.getFileId());
    } else {
      String partitionPath = keyLocation.getLeft().getPartitionPath();
      List<InsertBucketCumulativeWeightPair> targetBuckets = partitionPathToInsertBucketInfos.get(partitionPath);
      // pick the target bucket to use based on the weights.
      final long totalInserts = Math.max(1, workloadProfile.getWorkloadStat(partitionPath).getNumInserts());
      final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation.getLeft().getRecordKey());
      final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;

      int index = Collections.binarySearch(targetBuckets, new InsertBucketCumulativeWeightPair(new InsertBucket(), r));

      if (index >= 0) {
        return targetBuckets.get(index).getKey().bucketNumber;
      }

      if ((-1 * index - 1) < targetBuckets.size()) {
        return targetBuckets.get((-1 * index - 1)).getKey().bucketNumber;
      }

      // return first one, by default
      return targetBuckets.get(0).getKey().bucketNumber;
    }
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
}
