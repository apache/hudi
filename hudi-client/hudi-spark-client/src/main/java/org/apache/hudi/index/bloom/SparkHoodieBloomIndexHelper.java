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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.metadata.HoodieMetadataPayload.getBloomFilterIndexKey;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

/**
 * Helper for {@link HoodieBloomIndex} containing Spark-specific logic.
 */
public class SparkHoodieBloomIndexHelper extends BaseHoodieBloomIndexHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieBloomIndexHelper.class);

  private static final SparkHoodieBloomIndexHelper SINGLETON_INSTANCE =
      new SparkHoodieBloomIndexHelper();

  private SparkHoodieBloomIndexHelper() {
  }

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

    int inputParallelism = partitionRecordKeyPairs.deduceNumPartitions();
    int configuredBloomIndexParallelism = config.getBloomIndexParallelism();

    // NOTE: Target parallelism could be overridden by the config
    int targetParallelism =
        configuredBloomIndexParallelism > 0 ? configuredBloomIndexParallelism : inputParallelism;

    LOG.info("Input parallelism: {}, Index parallelism: {}", inputParallelism, targetParallelism);

    JavaPairRDD<HoodieFileGroupId, String> fileComparisonsRDD = HoodieJavaRDD.getJavaRDD(fileComparisonPairs);
    JavaRDD<List<HoodieKeyLookupResult>> keyLookupResultRDD;

    if (config.getBloomIndexUseMetadata()
        && hoodieTable.getMetaClient().getTableConfig().getMetadataPartitions()
        .contains(BLOOM_FILTERS.getPartitionPath())) {
      StorageConfiguration<?> storageConf = hoodieTable.getStorageConf();

      HoodieTableFileSystemView baseFileOnlyView =
          getBaseFileOnlyView(hoodieTable, partitionToFileInfo.keySet());

      Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast =
          ((HoodieSparkEngineContext) context).getJavaSparkContext().broadcast(baseFileOnlyView);

      // When leveraging MT we're aiming for following goals:
      //    - (G1) All requests to MT are made in batch (ie we're trying to fetch all the values
      //      for corresponding keys at once)
      //    - (G2) Each task reads no more than just _one_ file-group from the MT Bloom Filters
      //    partition
      //
      // Ta achieve G2, following invariant have to be maintained: Spark partitions have to be
      // affine w/ Metadata Table's file-groups, meaning that each Spark partition holds records
      // belonging to one and only file-group in MT Bloom Filters partition. To provide for that
      // we need to make sure
      //    - Spark's used [[Partitioner]] employs same hashing function as Metadata Table (as well
      //      as being applied to the same keys as the MT one)
      //    - Make sure that # of partitions is congruent to the # of file-groups (ie number of Spark
      //    partitions is a multiple of the # of the file-groups).
      //
      //    Last provision is necessary, so that for every key it's the case that
      //
      //        (hash(key) % N) % M = hash(key) % M, iff N % M = 0
      //
      //    Let's take an example of N = 8 and M = 4 (default # of file-groups in Bloom Filter
      //    partition). In that case Spark partitions for which `hash(key) % N` will be either 0
      //    or 4, will map to the same (first) file-group in MT
      int bloomFilterPartitionFileGroupCount =
          config.getMetadataConfig().getBloomFilterIndexFileGroupCount();
      int adjustedTargetParallelism =
          targetParallelism % bloomFilterPartitionFileGroupCount == 0
              ? targetParallelism
              // NOTE: We add 1 to make sure parallelism a) value always stays positive and b)
              //       {@code targetParallelism <= adjustedTargetParallelism}
              : (targetParallelism / bloomFilterPartitionFileGroupCount + 1) * bloomFilterPartitionFileGroupCount;

      AffineBloomIndexFileGroupPartitioner partitioner =
          new AffineBloomIndexFileGroupPartitioner(baseFileOnlyViewBroadcast, adjustedTargetParallelism);

      // First, we need to repartition and sort records using [[AffineBloomIndexFileGroupPartitioner]]
      // to make sure every Spark task accesses no more than just a single file-group in MT (allows
      // us to achieve G2).
      //
      // NOTE: Sorting records w/in individual partitions is required to make sure that we cluster
      //       together keys co-located w/in the MT files (sorted by keys)
      keyLookupResultRDD = fileComparisonsRDD.repartitionAndSortWithinPartitions(partitioner)
          .mapPartitionsToPair(new HoodieMetadataBloomFilterProbingFunction(baseFileOnlyViewBroadcast, hoodieTable))
          // Second, we use [[HoodieFileProbingFunction]] to open actual file and check whether it
          // contains the records with candidate keys that were filtered in by the Bloom Filter
          .mapPartitions(new HoodieFileProbingFunction(baseFileOnlyViewBroadcast, storageConf), true);

    } else if (config.useBloomIndexBucketizedChecking()) {
      Map<HoodieFileGroupId, Long> comparisonsPerFileGroup = computeComparisonsPerFileGroup(
          config, recordsPerPartition, partitionToFileInfo, fileComparisonsRDD, context);
      Partitioner partitioner = new BucketizedBloomCheckPartitioner(
          configuredBloomIndexParallelism, inputParallelism, comparisonsPerFileGroup,
          config.getBloomIndexKeysPerBucket(),
          config.useBloomIndexBucketizedCheckingWithDynamicParallelism());

      keyLookupResultRDD = fileComparisonsRDD.mapToPair(fileGroupAndRecordKey -> new Tuple2<>(fileGroupAndRecordKey, false))
          .repartitionAndSortWithinPartitions(partitioner, new FileGroupIdComparator())
          .map(Tuple2::_1)
          .mapPartitions(new HoodieSparkBloomIndexCheckFunction(hoodieTable, config), true);
    } else if (config.isBloomIndexFileGroupIdKeySortingEnabled()) {
      keyLookupResultRDD = fileComparisonsRDD.mapToPair(fileGroupAndRecordKey -> new Tuple2<>(fileGroupAndRecordKey, false))
          .sortByKey(new FileGroupIdAndRecordKeyComparator(), true, targetParallelism)
          .map(Tuple2::_1)
          .mapPartitions(new HoodieSparkBloomIndexCheckFunction(hoodieTable, config), true);
    } else {
      keyLookupResultRDD = fileComparisonsRDD.sortByKey(true, targetParallelism)
          .mapPartitions(new HoodieSparkBloomIndexCheckFunction(hoodieTable, config), true);
    }

    return HoodieJavaPairRDD.of(keyLookupResultRDD.flatMap(List::iterator)
        .filter(lr -> lr.getMatchingRecordKeysAndPositions().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeysAndPositions().stream()
            .map(recordKeyAndPosition -> new Tuple2<>(
                new HoodieKey(recordKeyAndPosition.getLeft(), lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId(),
                    recordKeyAndPosition.getRight())))
            .collect(Collectors.toList()).iterator()));
  }

  private static class FileGroupIdComparator implements Comparator<Tuple2<HoodieFileGroupId, String>>, Serializable {
    @Override
    public int compare(Tuple2<HoodieFileGroupId, String> o1, Tuple2<HoodieFileGroupId, String> o2) {
      return o1._1().compareTo(o2._1());
    }
  }

  private static class FileGroupIdAndRecordKeyComparator implements Comparator<Tuple2<HoodieFileGroupId, String>>, Serializable {
    @Override
    public int compare(Tuple2<HoodieFileGroupId, String> o1, Tuple2<HoodieFileGroupId, String> o2) {
      int fileGroupIdComparison = o1._1.compareTo(o2._1);
      if (fileGroupIdComparison != 0) {
        return fileGroupIdComparison;
      }
      return o1._2.compareTo(o2._2);
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

  private static HoodieTableFileSystemView getBaseFileOnlyView(HoodieTable<?, ?, ?, ?> hoodieTable, Collection<String> partitionPaths) {
    try {
      List<String> fullPartitionPaths = partitionPaths.stream()
          .map(partitionPath ->
              String.format("%s/%s", hoodieTable.getMetaClient().getBasePath(), partitionPath))
          .collect(Collectors.toList());

      List<StoragePathInfo> allFiles =
          hoodieTable.getMetadataTable().getAllFilesInPartitions(fullPartitionPaths).values()
              .stream()
              .flatMap(e -> e.stream())
              .collect(Collectors.toList());

      return new HoodieTableFileSystemView(hoodieTable.getMetaClient(), hoodieTable.getActiveTimeline(), allFiles);
    } catch (IOException e) {
      LOG.error(String.format("Failed to fetch all files for partitions (%s)", partitionPaths));
      throw new HoodieIOException("Failed to fetch all files for partitions", e);
    }
  }

  static class AffineBloomIndexFileGroupPartitioner extends Partitioner {

    private final Broadcast<HoodieTableFileSystemView> latestBaseFilesBroadcast;

    // TODO(HUDI-5619) remove when addressed
    private final Map<String, Map<String, String>> cachedLatestBaseFileNames =
        new HashMap<>(16);

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

      /*
      // TODO(HUDI-5619) uncomment when addressed
      String baseFileName =
          latestBaseFilesBroadcast.getValue()
              .getLatestBaseFile(partitionPath, fileGroupId)
              .orElseThrow(() -> new HoodieException(
                  String.format("File from file-group (%s) not found in partition path (%s)", fileGroupId, partitionPath)))
              .getFileName();
       */

      // NOTE: This is a workaround to alleviate performance impact of needing to process whole
      //       partition for every file-group being looked up.
      //       See HUDI-5619 for more details
      String baseFileName = cachedLatestBaseFileNames.computeIfAbsent(partitionPath, ignored ->
              latestBaseFilesBroadcast.getValue()
                  .getLatestBaseFiles(partitionPath)
                  .collect(
                      Collectors.toMap(HoodieBaseFile::getFileId, BaseFile::getFileName)
                  )
          )
          .get(fileGroupId);

      if (baseFileName == null) {
        throw new HoodieException(
            String.format("File from file-group (%s) not found in partition path (%s)", fileGroupId, partitionPath));
      }

      String bloomIndexEncodedKey =
          getBloomFilterIndexKey(new PartitionIndexID(HoodieTableMetadataUtil.getBloomFilterIndexPartitionIdentifier(partitionPath)), new FileIndexID(baseFileName));

      // NOTE: It's crucial that [[targetPartitions]] be congruent w/ the number of
      //       actual file-groups in the Bloom Index in MT
      SerializableBiFunction<String, Integer, Integer> mappingFunction = HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
      try {
        return mappingFunction.apply(bloomIndexEncodedKey, targetPartitions);
      } catch (Exception e) {
        throw new HoodieException("Error apply bloom index partitioner mapping function", e);
      }
    }
  }

  public static class HoodieSparkBloomIndexCheckFunction extends HoodieBloomIndexCheckFunction<Tuple2<HoodieFileGroupId, String>>
      implements FlatMapFunction<Iterator<Tuple2<HoodieFileGroupId, String>>, List<HoodieKeyLookupResult>> {

    public HoodieSparkBloomIndexCheckFunction(HoodieTable hoodieTable,
                                              HoodieWriteConfig config) {
      super(hoodieTable, config, t -> t._1, t -> t._2);
    }

    @Override
    public Iterator<List<HoodieKeyLookupResult>> call(Iterator<Tuple2<HoodieFileGroupId, String>> fileGroupIdRecordKeyPairIterator) {
      TaskContext taskContext = TaskContext.get();
      LOG.info("HoodieSparkBloomIndexCheckFunction with stageId : {}, stage attempt no: {}, taskId : {}, task attempt no : {}, task attempt id : {} ",
          taskContext.stageId(), taskContext.stageAttemptNumber(), taskContext.partitionId(), taskContext.attemptNumber(),
          taskContext.taskAttemptId());
      return new LazyKeyCheckIterator(fileGroupIdRecordKeyPairIterator);
    }
  }
}
