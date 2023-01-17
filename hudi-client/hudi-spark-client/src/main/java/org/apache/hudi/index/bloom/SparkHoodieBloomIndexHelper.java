/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.index.bloom;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieMetadataPayload.getBloomFilterIndexKey;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

/**
 * Helper for {@link HoodieBloomIndex} containing Spark-specific logic.
 */
public class SparkHoodieBloomIndexHelper extends BaseHoodieBloomIndexHelper {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBloomIndexHelper.class);

  private static final SparkHoodieBloomIndexHelper SINGLETON_INSTANCE =
      new SparkHoodieBloomIndexHelper();

  private SparkHoodieBloomIndexHelper() {}

  public static SparkHoodieBloomIndexHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  @Override
  public HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodiePairData<HoodieFileGroupId, String> fileComparisonPairs,
      Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      Map<String, Long> recordsPerPartition) {

    int inputParallelism = HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).partitions().size();
    int configuredBloomIndexParallelism = config.getBloomIndexParallelism();

    int joinParallelism = Math.max(inputParallelism, configuredBloomIndexParallelism);

    LOG.info("InputParallelism: ${" + inputParallelism + "}, IndexParallelism: ${"
        + configuredBloomIndexParallelism + "}");

    JavaRDD<List<HoodieKeyLookupResult>> keyLookupResultRDD;
    if (config.getBloomIndexUseMetadata()
        && hoodieTable.getMetaClient().getTableConfig().getMetadataPartitions()
        .contains(BLOOM_FILTERS.getPartitionPath())) {

      int targetParallelism = configuredBloomIndexParallelism > 0
          ? configuredBloomIndexParallelism
          : inputParallelism;

      JavaPairRDD<HoodieFileGroupId, String> fileComparisonsRDD =
          HoodieJavaRDD.getJavaRDD(fileComparisonPairs);

      // TODO fix comments

      // Step 1: Sort by file id
      AffineBloomIndexFileGroupPartitioner partitioner =
          new AffineBloomIndexFileGroupPartitioner(baseFileOnlyViewBroadcast, targetParallelism);

      JavaPairRDD<HoodieFileGroupId, HoodieBloomFilterKeyLookupResult> bloomFilterLookupResultRDD =
          fileComparisonsRDD.repartitionAndSortWithinPartitions(partitioner)
              // TODO to maintain invariant that one task reads just one file-group
              //    - remap into bloom index key
              //    - make sure spark partitioning hashing function and MT's one are the same
              //    - make sure # of tasks is a multiple of the # of file-groups
              .mapPartitionsToPair(new HoodieMetadataBloomFilterProbingFunction(hoodieTable));

      // Step 2: Use bloom filter to filter and the actual log file to get the record location
      keyLookupResultRDD = bloomFilterLookupResultRDD.repartition(inputParallelism)
          .mapPartitions(new HoodieFileProbingFunction(hoodieTable), true);

    } else if (config.useBloomIndexBucketizedChecking()) {
      Map<HoodieFileGroupId, Long> comparisonsPerFileGroup = computeComparisonsPerFileGroup(
          config, recordsPerPartition, partitionToFileInfo, fileComparisonsRDD, context);
      Partitioner partitioner = new BucketizedBloomCheckPartitioner(joinParallelism, comparisonsPerFileGroup,
          config.getBloomIndexKeysPerBucket());

      keyLookupResultRDD = fileComparisonsRDD.mapToPair(t -> new Tuple2<>(Pair.of(t._1, t._2), t))
          .repartitionAndSortWithinPartitions(partitioner)
          .map(Tuple2::_2)
          .mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true);
    } else {
      keyLookupResultRDD = fileComparisonsRDD.sortByKey(true, joinParallelism)
          .mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true);
    }

    return HoodieJavaPairRDD.of(keyLookupResultRDD.flatMap(List::iterator)
        .filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(Collectors.toList()).iterator()));
  }

  private static HoodieTableFileSystemView getBaseFileOnlyView(HoodieTable<?, ?, ?, ?> hoodieTable, Collection<String> partitionPaths) {
    try {
      List<String> fullPartitionPaths = partitionPaths.stream()
          .map(partitionPath ->
              String.format("%s/%s", hoodieTable.getMetaClient().getBasePathV2(), partitionPath))
          .collect(Collectors.toList());

      FileStatus[] allFiles =
          hoodieTable.getMetadataTable().getAllFilesInPartitions(fullPartitionPaths).values().stream()
              .flatMap(Arrays::stream)
              .toArray(FileStatus[]::new);

      return new HoodieTableFileSystemView(hoodieTable.getMetaClient(), hoodieTable.getActiveTimeline(), allFiles);
    } catch (IOException e) {
      LOG.error(String.format("Failed to fetch all files for partitions (%s)", partitionPaths));
      throw new HoodieIOException("Failed to fetch all files for partitions", e);
    }
  }

  static class AffineBloomIndexFileGroupPartitioner extends Partitioner {

    private final Broadcast<HoodieTableFileSystemView> latestBaseFilesBroadcast;

    private final int targetPartitions;

    AffineBloomIndexFileGroupPartitioner(Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast,
                                         int targetPartitions) {
      this.targetPartitions = targetPartitions;
      this.latestBaseFilesBroadcast = baseFileOnlyViewBroadcast;
    }

    @Override
    public int numPartitions() {
      return targetPartitions;
    }

    @Override
    public int getPartition(Object key) {
      HoodieFileGroupId partitionFileGroupId = (HoodieFileGroupId) key;
      String partitionPath = partitionFileGroupId.getPartitionPath();
      String fileGroupId = partitionFileGroupId.getFileId();

      String baseFileName =
          latestBaseFilesBroadcast.getValue()
              .getLatestBaseFile(partitionPath, fileGroupId)
              .orElseThrow(() -> new HoodieException(
                  String.format("File from file-group (%s) not found in partition path (%s)", fileGroupId, partitionPath)))
              .getFileName();

      String bloomIndexEncodedKey =
          getBloomFilterIndexKey(new PartitionIndexID(partitionPath), new FileIndexID(baseFileName));

      // NOTE: It's crucial that [[targetPartitions]] be congruent w/ the number of
      //       actual file-groups in the Bloom Index in MT
      return mapRecordKeyToFileGroupIndex(bloomIndexEncodedKey, targetPartitions);
    }
  }

  /**
   * Compute the estimated number of bloom filter comparisons to be performed on each file group.
   */
  private Map<HoodieFileGroupId, Long> computeComparisonsPerFileGroup(
      final HoodieWriteConfig config,
      final Map<String, Long> recordsPerPartition,
      final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      final JavaPairRDD<HoodieFileGroupId, String> fileComparisonsRDD,
      final HoodieEngineContext context) {
    Map<HoodieFileGroupId, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      context.setJobStatus(this.getClass().getSimpleName(), "Compute all comparisons needed between records and files: " + config.getTableName());
      fileToComparisons = fileComparisonsRDD.countByKey();
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.forEach((partitionPath, fileInfos) -> {
        for (BloomIndexFileInfo fileInfo : fileInfos) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(
              new HoodieFileGroupId(partitionPath, fileInfo.getFileId()), recordsPerPartition.get(partitionPath));
        }
      });
    }
    return fileToComparisons;
  }
}
