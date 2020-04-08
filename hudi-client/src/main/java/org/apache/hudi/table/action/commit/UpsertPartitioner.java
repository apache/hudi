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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition).
 */
public class UpsertPartitioner<T extends HoodieRecordPayload<T>> extends Partitioner {

  private static final Logger LOG = LogManager.getLogger(UpsertPartitioner.class);

  /**
   * List of all small files to be corrected.
   */
  protected List<SmallFile> smallFiles = new ArrayList<>();
  /**
   * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
   */
  private int totalBuckets = 0;
  /**
   * Stat for the current workload. Helps in determining total inserts, upserts etc.
   */
  private WorkloadStat globalStat;
  /**
   * Helps decide which bucket an incoming update should go to.
   */
  private HashMap<String, Integer> updateLocationToBucket;
  /**
   * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
   */
  private HashMap<String, List<InsertBucket>> partitionPathToInsertBuckets;
  /**
   * Remembers what type each bucket is for later.
   */
  private HashMap<Integer, BucketInfo> bucketInfoMap;

  protected final HoodieTable<T> table;

  protected final HoodieWriteConfig config;

  public UpsertPartitioner(WorkloadProfile profile, JavaSparkContext jsc, HoodieTable<T> table,
      HoodieWriteConfig config) {
    updateLocationToBucket = new HashMap<>();
    partitionPathToInsertBuckets = new HashMap<>();
    bucketInfoMap = new HashMap<>();
    globalStat = profile.getGlobalStat();
    this.table = table;
    this.config = config;
    assignUpdates(profile);
    assignInserts(profile, jsc);

    LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
        + "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n"
        + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
  }

  private void assignUpdates(WorkloadProfile profile) {
    // each update location gets a partition
    Set<Entry<String, WorkloadStat>> partitionStatEntries = profile.getPartitionPathStatMap().entrySet();
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        addUpdateBucket(partitionStat.getKey(), updateLocEntry.getKey());
      }
    }
  }

  private int addUpdateBucket(String partitionPath, String fileIdHint) {
    int bucket = totalBuckets;
    updateLocationToBucket.put(fileIdHint, bucket);
    BucketInfo bucketInfo = new BucketInfo();
    bucketInfo.bucketType = BucketType.UPDATE;
    bucketInfo.fileIdPrefix = fileIdHint;
    bucketInfo.partitionPath = partitionPath;
    bucketInfoMap.put(totalBuckets, bucketInfo);
    totalBuckets++;
    return bucket;
  }

  private void assignInserts(WorkloadProfile profile, JavaSparkContext jsc) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    Set<String> partitionPaths = profile.getPartitionPaths();
    long averageRecordSize =
        averageBytesPerRecord(table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
            config.getCopyOnWriteRecordSizeEstimate());
    LOG.info("AvgRecordSize => " + averageRecordSize);

    Map<String, List<SmallFile>> partitionSmallFilesMap =
        getSmallFilesForPartitions(new ArrayList<String>(partitionPaths), jsc);

    for (String partitionPath : partitionPaths) {
      WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
      if (pStat.getNumInserts() > 0) {

        List<SmallFile> smallFiles = partitionSmallFilesMap.get(partitionPath);
        this.smallFiles.addAll(smallFiles);

        LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

        long totalUnassignedInserts = pStat.getNumInserts();
        List<Integer> bucketNumbers = new ArrayList<>();
        List<Long> recordsPerBucket = new ArrayList<>();

        // first try packing this into one of the smallFiles
        for (SmallFile smallFile : smallFiles) {
          long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
              totalUnassignedInserts);
          if (recordsToAppend > 0 && totalUnassignedInserts > 0) {
            // create a new bucket or re-use an existing bucket
            int bucket;
            if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
              bucket = updateLocationToBucket.get(smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
            } else {
              bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
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
            recordsPerBucket.add(totalUnassignedInserts / insertBuckets);
            BucketInfo bucketInfo = new BucketInfo();
            bucketInfo.bucketType = BucketType.INSERT;
            bucketInfo.partitionPath = partitionPath;
            bucketInfo.fileIdPrefix = FSUtils.createNewFileIdPfx();
            bucketInfoMap.put(totalBuckets, bucketInfo);
            totalBuckets++;
          }
        }

        // Go over all such buckets, and assign weights as per amount of incoming inserts.
        List<InsertBucket> insertBuckets = new ArrayList<>();
        for (int i = 0; i < bucketNumbers.size(); i++) {
          InsertBucket bkt = new InsertBucket();
          bkt.bucketNumber = bucketNumbers.get(i);
          bkt.weight = (1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts();
          insertBuckets.add(bkt);
        }
        LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
        partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
      }
    }
  }

  private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths, JavaSparkContext jsc) {

    Map<String, List<SmallFile>> partitionSmallFilesMap = new HashMap<>();
    if (partitionPaths != null && partitionPaths.size() > 0) {
      JavaRDD<String> partitionPathRdds = jsc.parallelize(partitionPaths, partitionPaths.size());
      partitionSmallFilesMap = partitionPathRdds.mapToPair((PairFunction<String, String, List<SmallFile>>)
          partitionPath -> new Tuple2<>(partitionPath, getSmallFiles(partitionPath))).collectAsMap();
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

  public List<InsertBucket> getInsertBuckets(String partitionPath) {
    return partitionPathToInsertBuckets.get(partitionPath);
  }

  @Override
  public int numPartitions() {
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
      List<InsertBucket> targetBuckets = partitionPathToInsertBuckets.get(keyLocation._1().getPartitionPath());
      // pick the target bucket to use based on the weights.
      double totalWeight = 0.0;
      final long totalInserts = Math.max(1, globalStat.getNumInserts());
      final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
      final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;
      for (InsertBucket insertBucket : targetBuckets) {
        totalWeight += insertBucket.weight;
        if (r <= totalWeight) {
          return insertBucket.bucketNumber;
        }
      }
      // return first one, by default
      return targetBuckets.get(0).bucketNumber;
    }
  }

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  protected static long averageBytesPerRecord(HoodieTimeline commitTimeline, int defaultRecordSizeEstimate) {
    long avgSize = defaultRecordSizeEstimate;
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
          if (totalBytesWritten > 0 && totalRecordsWritten > 0) {
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
