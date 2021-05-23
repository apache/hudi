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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.io.HoodieRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkHoodieBloomIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBloomIndex.class);

  public SparkHoodieBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context,
                                              HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {

    // Step 0: cache the input record RDD
    if (config.getBloomIndexUseCaching()) {
      recordRDD.persist(SparkMemoryUtils.getBloomIndexInputStorageLevel(config.getProps()));
    }

    // Step 1: Extract out thinner JavaPairRDD of (partitionPath, recordKey)
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        recordRDD.mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record.getRecordKey()));

    // Lookup indexes for all the partition/recordkey pair
    JavaPairRDD<HoodieKey, HoodieRecordLocation> keyFilenamePairRDD =
        lookupIndex(partitionRecordKeyPairRDD, context, hoodieTable);

    // Cache the result, for subsequent stages.
    if (config.getBloomIndexUseCaching()) {
      keyFilenamePairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }
    if (LOG.isDebugEnabled()) {
      long totalTaggedRecords = keyFilenamePairRDD.count();
      LOG.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
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
   * Lookup the location for each record key and return the pair<record_key,location> for all record keys already
   * present and drop the record keys if not present.
   */
  private JavaPairRDD<HoodieKey, HoodieRecordLocation> lookupIndex(
      JavaPairRDD<String, String> partitionRecordKeyPairRDD, final HoodieEngineContext context,
      final HoodieTable hoodieTable) {
    // Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = partitionRecordKeyPairRDD.countByKey();
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Tuple2<String, BloomIndexFileInfo>> fileInfoList =
        loadInvolvedFiles(affectedPartitionPathList, context, hoodieTable);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Tuple2::_1, mapping(Tuple2::_2, toList())));

    // Step 3: Obtain a RDD, for each incoming record, that already exists, with the file id,
    // that contains it.
    JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        explodeRecordRDDWithFileComparisons(partitionToFileInfo, partitionRecordKeyPairRDD);
    Map<String, Long> comparisonsPerFileGroup =
        computeComparisonsPerFileGroup(recordsPerPartition, partitionToFileInfo, fileComparisonsRDD, context);
    int inputParallelism = partitionRecordKeyPairRDD.partitions().size();
    int joinParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    LOG.info("InputParallelism: ${" + inputParallelism + "}, IndexParallelism: ${"
        + config.getBloomIndexParallelism() + "}");
    return findMatchingFilesForRecordKeys(fileComparisonsRDD, joinParallelism, hoodieTable,
        comparisonsPerFileGroup);
  }

  /**
   * Compute the estimated number of bloom filter comparisons to be performed on each file group.
   */
  private Map<String, Long> computeComparisonsPerFileGroup(final Map<String, Long> recordsPerPartition,
                                                           final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
                                                           final JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD,
                                                           final HoodieEngineContext context) {
    Map<String, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      context.setJobStatus(this.getClass().getSimpleName(), "Compute all comparisons needed between records and files");
      fileToComparisons = fileComparisonsRDD.mapToPair(t -> t).countByKey();
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.forEach((key, value) -> {
        for (BloomIndexFileInfo fileInfo : value) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(fileInfo.getFileId(), recordsPerPartition.get(key));
        }
      });
    }
    return fileToComparisons;
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD.
   */
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, final HoodieEngineContext context,
                                                             final HoodieTable hoodieTable) {

    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(partitions, context, hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());

    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      context.setJobStatus(this.getClass().getName(), "Obtain key ranges for file slices (range pruning=on)");
      return context.map(partitionPathFileIDList, pf -> {
        try {
          HoodieRangeInfoHandle rangeInfoHandle = new HoodieRangeInfoHandle(config, hoodieTable, pf);
          String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1]));
        } catch (MetadataNotFoundException me) {
          LOG.warn("Unable to find range metadata in file :" + pf);
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()));
        }
      }, Math.max(partitionPathFileIDList.size(), 1));
    } else {
      return partitionPathFileIDList.stream()
          .map(pf -> new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
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
   * checked. For tables, where the keys have a definite insert order (e.g: timestamp as prefix), the number of files
   * to be compared gets cut down a lot from range pruning.
   * <p>
   * Sub-partition to ensure the records can be looked up against files & also prune file<=>record comparisons based on
   * recordKey ranges in the index info.
   */
  JavaRDD<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedIndexFileFilter(partitionToFileIndexInfo);

    return partitionRecordKeyPairRDD.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair._2();
      String partitionPath = partitionRecordKeyPair._1();

      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair -> new Tuple2<>(partitionFileIdPair.getRight(),
              new HoodieKey(recordKey, partitionPath)))
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
  JavaPairRDD<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD,
      int shuffleParallelism,
      HoodieTable hoodieTable,
      Map<String, Long> fileGroupToComparisons) {

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
        .map(v1 -> HoodieIndexUtils.getTaggedRecord(v1._1, Option.ofNullable(v1._2.orNull())));
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context,
                                             HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    return writeStatusRDD;
  }
}
