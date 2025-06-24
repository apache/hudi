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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.estimator.RecordSizeEstimator;
import org.apache.hudi.estimator.RecordSizeEstimatorFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition).
 */
public class UpsertPartitioner<T> extends SparkHoodiePartitioner<T> {

  private static final Logger LOG = LoggerFactory.getLogger(UpsertPartitioner.class);

  /**
   * List of all small files to be corrected.
   */
  protected List<SmallFile> smallFiles = new ArrayList<>();
  /**
   * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
   */
  private int totalBuckets = 0;
  /**
   * Helps decide which bucket an incoming update should go to.
   */
  private final HashMap<String, Integer> updateLocationToBucket;
  /**
   * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
   */
  private final HashMap<String, List<InsertBucketCumulativeWeightPair>> partitionPathToInsertBucketInfos;
  /**
   * Remembers what type each bucket is for later.
   */
  protected final HashMap<Integer, BucketInfo> bucketInfoMap;

  protected final HoodieWriteConfig config;
  private final WriteOperationType operationType;
  private final RecordSizeEstimator recordSizeEstimator;

  public UpsertPartitioner(WorkloadProfile profile, HoodieEngineContext context, HoodieTable table,
                           HoodieWriteConfig config, WriteOperationType operationType) {
    super(profile, table);
    updateLocationToBucket = new HashMap<>();
    partitionPathToInsertBucketInfos = new HashMap<>();
    bucketInfoMap = new HashMap<>();
    this.config = config;
    this.operationType = operationType;
    this.recordSizeEstimator = RecordSizeEstimatorFactory.createRecordSizeEstimator(config);
    assignUpdates(profile);
    long totalInserts = profile.getInputPartitionPathStatMap().values().stream().mapToLong(stat -> stat.getNumInserts()).sum();
    if (!WriteOperationType.isPreppedWriteOperation(operationType) || totalInserts > 0) { // skip if its prepped write operation. or if totalInserts = 0.
      assignInserts(profile, context);
    }

    LOG.info("Total Buckets: {}, bucketInfoMap size: {}, partitionPathToInsertBucketInfos size: {}, updateLocationToBucket size: {}",
        totalBuckets, bucketInfoMap.size(), partitionPathToInsertBucketInfos.size(), updateLocationToBucket.size());
    LOG.debug("Buckets info => {}, Partition to insert buckets => {} UpdateLocations mapped to buckets => {}",
        bucketInfoMap, partitionPathToInsertBucketInfos, updateLocationToBucket);
  }

  private void assignUpdates(WorkloadProfile profile) {
    // each update location gets a partition
    Set<Entry<String, WorkloadStat>> partitionStatEntries = profile.getInputPartitionPathStatMap().entrySet();
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

  /**
   * Get the in pending clustering fileId for each partition path.
   * @return partition path to pending clustering file groups id
   */
  private Map<String, Set<String>> getPartitionPathToPendingClusteringFileGroupsId() {
    Map<String, Set<String>>  partitionPathToInPendingClusteringFileId =
        table.getFileSystemView().getFileGroupsInPendingClustering()
            .map(fileGroupIdAndInstantPair ->
                Pair.of(fileGroupIdAndInstantPair.getKey().getPartitionPath(), fileGroupIdAndInstantPair.getKey().getFileId()))
            .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
    return partitionPathToInPendingClusteringFileId;
  }

  /**
   * Exclude small file handling for clustering since update path is not supported.
   * @param pendingClusteringFileGroupsId  pending clustering file groups id of partition
   * @param smallFiles small files of partition
   * @return smallFiles not in clustering
   */
  private List<SmallFile> filterSmallFilesInClustering(final Set<String> pendingClusteringFileGroupsId, final List<SmallFile> smallFiles) {
    if (!pendingClusteringFileGroupsId.isEmpty()) {
      return smallFiles.stream()
          .filter(smallFile -> !pendingClusteringFileGroupsId.contains(smallFile.location.getFileId())).collect(Collectors.toList());
    } else {
      return smallFiles;
    }
  }

  private void assignInserts(WorkloadProfile profile, HoodieEngineContext context) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    Set<String> partitionPaths = profile.getPartitionPaths();
    /*
     * NOTE: we only use commit instants to calculate average record size because replacecommit can be
     * created by clustering, which has smaller average record size, which affects assigning inserts and
     * may result in OOM by making spark underestimate the actual input record sizes.
     */
    TimelineLayout layout = TimelineLayout.fromVersion(table.getActiveTimeline().getTimelineLayoutVersion());
    long averageRecordSize = recordSizeEstimator.averageBytesPerRecord(table.getMetaClient().getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
        .filterCompletedInstants(),  layout.getCommitMetadataSerDe());
    LOG.info("AvgRecordSize => {}", averageRecordSize);

    Map<String, List<SmallFile>> partitionSmallFilesMap =
        getSmallFilesForPartitions(new ArrayList<>(partitionPaths), context);

    Map<String, Set<String>> partitionPathToPendingClusteringFileGroupsId = getPartitionPathToPendingClusteringFileGroupsId();

    for (String partitionPath : partitionPaths) {
      WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
      WorkloadStat outputWorkloadStats = profile.getOutputPartitionPathStatMap().getOrDefault(partitionPath, new WorkloadStat());
      if (pStat.getNumInserts() > 0) {

        List<SmallFile> smallFiles =
            filterSmallFilesInClustering(partitionPathToPendingClusteringFileGroupsId.getOrDefault(partitionPath, Collections.emptySet()),
                partitionSmallFilesMap.getOrDefault(partitionPath, Collections.emptyList()));

        this.smallFiles.addAll(smallFiles);

        LOG.info("For partitionPath : {} Total Small Files => {}", partitionPath, smallFiles.size());
        LOG.debug("For partitionPath : {} Small Files => {}", partitionPath, smallFiles);

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
              LOG.debug("Assigning {} inserts to existing update bucket {}", recordsToAppend, bucket);
            } else {
              bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
              LOG.debug("Assigning {} inserts to new update bucket {}", recordsToAppend, bucket);
            }
            if (profile.hasOutputWorkLoadStats()) {
              outputWorkloadStats.addInserts(smallFile.location, recordsToAppend);
            }
            bucketNumbers.add(bucket);
            recordsPerBucket.add(recordsToAppend);
            totalUnassignedInserts -= recordsToAppend;
            if (totalUnassignedInserts <= 0) {
              // stop the loop when all the inserts are assigned
              break;
            }
          }
        }

        // if we have anything more, create new insert buckets, like normal
        if (totalUnassignedInserts > 0) {
          long insertRecordsPerBucket = config.getCopyOnWriteInsertSplitSize();
          if (config.shouldAutoTuneInsertSplits()) {
            insertRecordsPerBucket = (int) Math.ceil((1.0 * config.getParquetMaxFileSize()) / averageRecordSize);
          }

          int insertBuckets = (int) Math.ceil((1.0 * totalUnassignedInserts) / insertRecordsPerBucket);
          LOG.info("After small file assignment: unassignedInserts => {}, totalInsertBuckets => {}, recordsPerBucket => {}",
              totalUnassignedInserts, insertBuckets, insertRecordsPerBucket);
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
        LOG.info("Total insert buckets for partition path {} => {}", partitionPath, insertBuckets);
        partitionPathToInsertBucketInfos.put(partitionPath, insertBuckets);
      }
      if (profile.hasOutputWorkLoadStats()) {
        profile.updateOutputPartitionPathStatMap(partitionPath, outputWorkloadStats);
      }
    }
  }

  private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths, HoodieEngineContext context) {
    if (config.getParquetSmallFileLimit() <= 0 || (partitionPaths == null || partitionPaths.isEmpty())) {
      return Collections.emptyMap();
    }

    if (table.getMetaClient().getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      return Collections.emptyMap();
    }

    context.setJobStatus(this.getClass().getSimpleName(), "Getting small files from partitions: " + config.getTableName());
    long startTimeMs = System.currentTimeMillis();
    Map<String, List<SmallFile>> partitionSmallFilesMap = context.mapToPair(partitionPaths, paritionPath -> Pair.of(paritionPath, getSmallFiles(paritionPath)), partitionPaths.size());
    LOG.info("Fetched small files in {}ms", System.currentTimeMillis() - startTimeMs);
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
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.requestedTime()).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        if (file.getFileSize() < config.getParquetSmallFileLimit()) {
          SmallFile sf = new SmallFile();
          sf.location = new HoodieRecordLocation(file.getCommitTime(), file.getFileId());
          sf.sizeBytes = file.getFileSize();
          smallFileLocations.add(sf);
        }
      }
    }

    return smallFileLocations;
  }

  public List<BucketInfo> getBucketInfos() {
    return Collections.unmodifiableList(new ArrayList<>(bucketInfoMap.values()));
  }

  @Override
  public SparkBucketInfoGetter getSparkBucketInfoGetter() {
    return new MapBasedSparkBucketInfoGetter(bucketInfoMap);
  }

  public List<InsertBucketCumulativeWeightPair> getInsertBuckets(String partitionPath) {
    return partitionPathToInsertBucketInfos.get(partitionPath);
  }

  @Override
  public int numPartitions() {
    return totalBuckets;
  }

  @Override
  public int getNumPartitions() {
    return totalBuckets;
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
        (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    if (keyLocation._2().isPresent()) {
      HoodieRecordLocation location = keyLocation._2().get();
      return updateLocationToBucket.get(location.getFileId());
    } else {
      String partitionPath = keyLocation._1().getPartitionPath();
      List<InsertBucketCumulativeWeightPair> targetBuckets = partitionPathToInsertBucketInfos.get(partitionPath);
      // pick the target bucket to use based on the weights.
      final long totalInserts = Math.max(1, profile.getWorkloadStat(partitionPath).getNumInserts());
      final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
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
}
