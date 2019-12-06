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

package org.apache.hudi.index.bloom;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
public class HoodieBloomIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  // we need to limit the join such that it stays within 1.5GB per Spark partition. (SPARK-1476)
  private static final int SPARK_MAXIMUM_BYTES_PER_PARTITION = 1500 * 1024 * 1024;
  // this is how much a triplet of (partitionPath, fileId, recordKey) costs.
  private static final int BYTES_PER_PARTITION_FILE_KEY_TRIPLET = 300;
  private static Logger logger = LogManager.getLogger(HoodieBloomIndex.class);
  private static int MAX_ITEMS_PER_SHUFFLE_PARTITION =
      SPARK_MAXIMUM_BYTES_PER_PARTITION / BYTES_PER_PARTITION_FILE_KEY_TRIPLET;

  public HoodieBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {

    // Step 0: cache the input record RDD
    if (config.getBloomIndexUseCaching()) {
      recordRDD.persist(config.getBloomIndexInputStorageLevel());
    }

    // Step 1: Extract out thinner JavaPairRDD of (partitionPath, recordKey)
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        recordRDD.mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record.getRecordKey()));

    // Lookup indexes for all the partition/recordkey pair
    JavaPairRDD<HoodieKey, HoodieRecordLocation> keyFilenamePairRDD =
        lookupIndex(partitionRecordKeyPairRDD, jsc, hoodieTable);

    // Cache the result, for subsequent stages.
    if (config.getBloomIndexUseCaching()) {
      keyFilenamePairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }
    if (logger.isDebugEnabled()) {
      long totalTaggedRecords = keyFilenamePairRDD.count();
      logger.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
    }

    // Step 4: Tag the incoming records, as inserts or updates, by joining with existing record keys
    // Cost: 4 sec.
    JavaRDD<HoodieRecord<T>> taggedRecordRDD = tagLocationBacktoRecords(keyFilenamePairRDD, recordRDD);

    if (config.getBloomIndexUseCaching()) {
      recordRDD.unpersist(); // unpersist the input Record RDD
      keyFilenamePairRDD.unpersist();
    }

    return taggedRecordRDD;
  }

  /**
   * Returns an RDD mapping each HoodieKey with a partitionPath/fileID which contains it. Option.Empty if the key is not
   * found.
   *
   * @param hoodieKeys keys to lookup
   * @param jsc spark context
   * @param hoodieTable hoodie table object
   */
  @Override
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        hoodieKeys.mapToPair(key -> new Tuple2<>(key.getPartitionPath(), key.getRecordKey()));

    // Lookup indexes for all the partition/recordkey pair
    JavaPairRDD<HoodieKey, HoodieRecordLocation> recordKeyLocationRDD =
        lookupIndex(partitionRecordKeyPairRDD, jsc, hoodieTable);
    JavaPairRDD<HoodieKey, String> keyHoodieKeyPairRDD = hoodieKeys.mapToPair(key -> new Tuple2<>(key, null));

    return keyHoodieKeyPairRDD.leftOuterJoin(recordKeyLocationRDD).mapToPair(keyLoc -> {
      Option<Pair<String, String>> partitionPathFileidPair;
      if (keyLoc._2._2.isPresent()) {
        partitionPathFileidPair = Option.of(Pair.of(keyLoc._1().getPartitionPath(), keyLoc._2._2.get().getFileId()));
      } else {
        partitionPathFileidPair = Option.empty();
      }
      return new Tuple2<>(keyLoc._1, partitionPathFileidPair);
    });
  }

  /**
   * Lookup the location for each record key and return the pair<record_key,location> for all record keys already
   * present and drop the record keys if not present.
   */
  private JavaPairRDD<HoodieKey, HoodieRecordLocation> lookupIndex(
      JavaPairRDD<String, String> partitionRecordKeyPairRDD, final JavaSparkContext jsc,
      final HoodieTable hoodieTable) {
    // Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = partitionRecordKeyPairRDD.countByKey();
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Tuple2<String, BloomIndexFileInfo>> fileInfoList =
        loadInvolvedFiles(affectedPartitionPathList, jsc, hoodieTable);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Tuple2::_1, mapping(Tuple2::_2, toList())));

    // Step 3: Obtain a RDD, for each incoming record, that already exists, with the file id,
    // that contains it.
    Map<String, Long> comparisonsPerFileGroup =
        computeComparisonsPerFileGroup(recordsPerPartition, partitionToFileInfo, partitionRecordKeyPairRDD);
    int safeParallelism = computeSafeParallelism(recordsPerPartition, comparisonsPerFileGroup);
    int joinParallelism = determineParallelism(partitionRecordKeyPairRDD.partitions().size(), safeParallelism);
    return findMatchingFilesForRecordKeys(partitionToFileInfo, partitionRecordKeyPairRDD, joinParallelism, hoodieTable,
        comparisonsPerFileGroup);
  }

  /**
   * Compute the estimated number of bloom filter comparisons to be performed on each file group.
   */
  private Map<String, Long> computeComparisonsPerFileGroup(final Map<String, Long> recordsPerPartition,
      final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {

    Map<String, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      fileToComparisons = explodeRecordRDDWithFileComparisons(partitionToFileInfo, partitionRecordKeyPairRDD)
          .mapToPair(t -> t).countByKey();
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.entrySet().stream().forEach(e -> {
        for (BloomIndexFileInfo fileInfo : e.getValue()) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(fileInfo.getFileId(), recordsPerPartition.get(e.getKey()));
        }
      });
    }
    return fileToComparisons;
  }

  /**
   * Compute the minimum parallelism needed to play well with the spark 2GB limitation.. The index lookup can be skewed
   * in three dimensions : #files, #partitions, #records
   * <p>
   * To be able to smoothly handle skews, we need to compute how to split each partitions into subpartitions. We do it
   * here, in a way that keeps the amount of each Spark join partition to < 2GB.
   * <p>
   * If {@link HoodieIndexConfig#BLOOM_INDEX_PARALLELISM_PROP} is specified as a NON-zero number, then that is used
   * explicitly.
   */
  int computeSafeParallelism(Map<String, Long> recordsPerPartition, Map<String, Long> comparisonsPerFileGroup) {
    long totalComparisons = comparisonsPerFileGroup.values().stream().mapToLong(Long::longValue).sum();
    long totalFiles = comparisonsPerFileGroup.size();
    long totalRecords = recordsPerPartition.values().stream().mapToLong(Long::longValue).sum();
    int parallelism = (int) (totalComparisons / MAX_ITEMS_PER_SHUFFLE_PARTITION + 1);
    logger.info(String.format(
        "TotalRecords %d, TotalFiles %d, TotalAffectedPartitions %d, TotalComparisons %d, " + "SafeParallelism %d",
        totalRecords, totalFiles, recordsPerPartition.size(), totalComparisons, parallelism));
    return parallelism;
  }

  /**
   * Its crucial to pick the right parallelism.
   * <p>
   * totalSubPartitions : this is deemed safe limit, to be nice with Spark. inputParallelism : typically number of input
   * file splits
   * <p>
   * We pick the max such that, we are always safe, but go higher if say a there are a lot of input files. (otherwise,
   * we will fallback to number of partitions in input and end up with slow performance)
   */
  private int determineParallelism(int inputParallelism, int totalSubPartitions) {
    // If bloom index parallelism is set, use it to to check against the input parallelism and
    // take the max
    int indexParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    int joinParallelism = Math.max(totalSubPartitions, indexParallelism);
    logger.info("InputParallelism: ${" + inputParallelism + "}, " + "IndexParallelism: ${"
        + config.getBloomIndexParallelism() + "}, " + "TotalSubParts: ${" + totalSubPartitions + "}, "
        + "Join Parallelism set to : " + joinParallelism);
    return joinParallelism;
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD.
   */
  @VisibleForTesting
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, final JavaSparkContext jsc,
      final HoodieTable hoodieTable) {

    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList =
        jsc.parallelize(partitions, Math.max(partitions.size(), 1)).flatMap(partitionPath -> {
          Option<HoodieInstant> latestCommitTime =
              hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant();
          List<Pair<String, String>> filteredFiles = new ArrayList<>();
          if (latestCommitTime.isPresent()) {
            filteredFiles = hoodieTable.getROFileSystemView()
                .getLatestDataFilesBeforeOrOn(partitionPath, latestCommitTime.get().getTimestamp())
                .map(f -> Pair.of(partitionPath, f.getFileId())).collect(toList());
          }
          return filteredFiles.iterator();
        }).collect();

    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      return jsc.parallelize(partitionPathFileIDList, Math.max(partitionPathFileIDList.size(), 1)).mapToPair(pf -> {
        try {
          HoodieRangeInfoHandle<T> rangeInfoHandle = new HoodieRangeInfoHandle<T>(config, hoodieTable, pf);
          String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1]));
        } catch (MetadataNotFoundException me) {
          logger.warn("Unable to find range metadata in file :" + pf);
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()));
        }
      }).collect();
    } else {
      return partitionPathFileIDList.stream()
          .map(pf -> new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Nope, don't need to do anything.
    return true;
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup.
   */
  @Override
  public boolean isGlobal() {
    return false;
  }

  /**
   * No indexes into log files yet.
   */
  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  /**
   * Bloom filters are stored, into the same data files.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  /**
   * For each incoming record, produce N output records, 1 each for each file against which the record's key needs to be
   * checked. For datasets, where the keys have a definite insert order (e.g: timestamp as prefix), the number of files
   * to be compared gets cut down a lot from range pruning.
   *
   * Sub-partition to ensure the records can be looked up against files & also prune file<=>record comparisons based on
   * recordKey ranges in the index info.
   */
  @VisibleForTesting
  JavaRDD<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedIndexFileFilter(partitionToFileIndexInfo);

    return partitionRecordKeyPairRDD.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair._2();
      String partitionPath = partitionRecordKeyPair._1();

      return indexFileFilter.getMatchingFiles(partitionPath, recordKey).stream()
          .map(matchingFile -> new Tuple2<>(matchingFile, new HoodieKey(recordKey, partitionPath)))
          .collect(Collectors.toList());
    }).flatMap(List::iterator);
  }

  /**
   * Find out <RowKey, filename> pair. All workload grouped by file-level.
   * <p>
   * Join PairRDD(PartitionPath, RecordKey) and PairRDD(PartitionPath, File) & then repartition such that each RDD
   * partition is a file, then for each file, we do (1) load bloom filter, (2) load rowKeys, (3) Tag rowKey
   * <p>
   * Make sure the parallelism is atleast the groupby parallelism for tagging location
   */
  @VisibleForTesting
  JavaPairRDD<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD, int shuffleParallelism, HoodieTable hoodieTable,
      Map<String, Long> fileGroupToComparisons) {
    JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        explodeRecordRDDWithFileComparisons(partitionToFileIndexInfo, partitionRecordKeyPairRDD);

    if (config.useBloomIndexBucketizedChecking()) {
      Partitioner partitioner = new BucketizedBloomCheckPartitioner(shuffleParallelism, fileGroupToComparisons,
          config.getBloomIndexKeysPerBucket());

      fileComparisonsRDD = fileComparisonsRDD.mapToPair(t -> new Tuple2<>(Pair.of(t._1, t._2.getRecordKey()), t))
          .repartitionAndSortWithinPartitions(partitioner).map(Tuple2::_2);
    } else {
      fileComparisonsRDD = fileComparisonsRDD.sortBy(Tuple2::_1, true, shuffleParallelism);
    }

    return fileComparisonsRDD.mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true)
        .flatMap(List::iterator).filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(Collectors.toList()).iterator());
  }

  HoodieRecord<T> getTaggedRecord(HoodieRecord<T> inputRecord, Option<HoodieRecordLocation> location) {
    HoodieRecord<T> record = inputRecord;
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then rowKeyRecordPairRDD
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      record = new HoodieRecord<>(inputRecord);
      record.unseal();
      record.setCurrentLocation(location.get());
      record.seal();
    }
    return record;
  }

  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord RDD.
   */
  protected JavaRDD<HoodieRecord<T>> tagLocationBacktoRecords(
      JavaPairRDD<HoodieKey, HoodieRecordLocation> keyFilenamePairRDD, JavaRDD<HoodieRecord<T>> recordRDD) {
    JavaPairRDD<HoodieKey, HoodieRecord<T>> keyRecordPairRDD =
        recordRDD.mapToPair(record -> new Tuple2<>(record.getKey(), record));
    // Here as the recordRDD might have more data than rowKeyRDD (some rowKeys' fileId is null),
    // so we do left outer join.
    return keyRecordPairRDD.leftOuterJoin(keyFilenamePairRDD).values()
        .map(v1 -> getTaggedRecord(v1._1, Option.ofNullable(v1._2.orNull())));
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    return writeStatusRDD;
  }
}
