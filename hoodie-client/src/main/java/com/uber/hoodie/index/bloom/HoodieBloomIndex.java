/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bloom;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.MetadataNotFoundException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in
 * its metadata.
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
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = recordRDD
        .mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record.getRecordKey()));

    // Lookup indexes for all the partition/recordkey pair
    JavaPairRDD<String, String> rowKeyFilenamePairRDD = lookupIndex(partitionRecordKeyPairRDD, jsc, hoodieTable);

    // Cache the result, for subsequent stages.
    if (config.getBloomIndexUseCaching()) {
      rowKeyFilenamePairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }
    if (logger.isDebugEnabled()) {
      long totalTaggedRecords = rowKeyFilenamePairRDD.count();
      logger.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
    }

    // Step 4: Tag the incoming records, as inserts or updates, by joining with existing record keys
    // Cost: 4 sec.
    JavaRDD<HoodieRecord<T>> taggedRecordRDD = tagLocationBacktoRecords(rowKeyFilenamePairRDD,
        recordRDD);

    if (config.getBloomIndexUseCaching()) {
      recordRDD.unpersist(); // unpersist the input Record RDD
      rowKeyFilenamePairRDD.unpersist();
    }

    return taggedRecordRDD;
  }

  public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = hoodieKeys
        .mapToPair(key -> new Tuple2<>(key.getPartitionPath(), key.getRecordKey()));

    // Lookup indexes for all the partition/recordkey pair
    JavaPairRDD<String, String> rowKeyFilenamePairRDD = lookupIndex(partitionRecordKeyPairRDD, jsc, hoodieTable);

    JavaPairRDD<String, HoodieKey> rowKeyHoodieKeyPairRDD = hoodieKeys
        .mapToPair(key -> new Tuple2<>(key.getRecordKey(), key));

    return rowKeyHoodieKeyPairRDD.leftOuterJoin(rowKeyFilenamePairRDD).mapToPair(keyPathTuple -> {
      Optional<String> recordLocationPath;
      if (keyPathTuple._2._2.isPresent()) {
        String fileName = keyPathTuple._2._2.get();
        String partitionPath = keyPathTuple._2._1.getPartitionPath();
        recordLocationPath = Optional
            .of(new Path(new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath), fileName)
                .toUri().getPath());
      } else {
        recordLocationPath = Optional.absent();
      }
      return new Tuple2<>(keyPathTuple._2._1, recordLocationPath);
    });
  }

  /**
   * Lookup the location for each record key and return the pair<record_key,location> for all record
   * keys already present and drop the record keys if not present
   */
  private JavaPairRDD<String, String> lookupIndex(
      JavaPairRDD<String, String> partitionRecordKeyPairRDD, final JavaSparkContext
      jsc, final HoodieTable hoodieTable) {
    // Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = partitionRecordKeyPairRDD.countByKey();
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Tuple2<String, BloomIndexFileInfo>> fileInfoList = loadInvolvedFiles(affectedPartitionPathList, jsc,
        hoodieTable);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo = fileInfoList.stream()
        .collect(groupingBy(Tuple2::_1, mapping(Tuple2::_2, toList())));

    // Step 3: Obtain a RDD, for each incoming record, that already exists, with the file id,
    // that contains it.
    int parallelism = autoComputeParallelism(recordsPerPartition, partitionToFileInfo,
        partitionRecordKeyPairRDD);
    return findMatchingFilesForRecordKeys(partitionToFileInfo,
        partitionRecordKeyPairRDD, parallelism, hoodieTable.getMetaClient());
  }

  /**
   * The index lookup can be skewed in three dimensions : #files, #partitions, #records
   * <p>
   * To be able to smoothly handle skews, we need to compute how to split each partitions into
   * subpartitions. We do it here, in a way that keeps the amount of each Spark join partition to <
   * 2GB.
   * <p>
   * If {@link com.uber.hoodie.config.HoodieIndexConfig#BLOOM_INDEX_PARALLELISM_PROP} is specified
   * as a NON-zero number, then that is used explicitly.
   */
  private int autoComputeParallelism(final Map<String, Long> recordsPerPartition,
      final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {

    long totalComparisons = 0;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      totalComparisons = explodeRecordRDDWithFileComparisons(partitionToFileInfo,
          partitionRecordKeyPairRDD).count();
    } else {
      // if not pruning by ranges, then each file in a partition needs to compared against all
      // records for a partition.
      Map<String, Long> filesPerPartition = partitionToFileInfo.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> Long.valueOf(e.getValue().size())));
      long totalFiles = 0;
      long totalRecords = 0;
      for (String partitionPath : recordsPerPartition.keySet()) {
        long numRecords = recordsPerPartition.get(partitionPath);
        long numFiles =
            filesPerPartition.containsKey(partitionPath) ? filesPerPartition.get(partitionPath)
                : 1L;

        totalComparisons += numFiles * numRecords;
        totalFiles +=
            filesPerPartition.containsKey(partitionPath) ? filesPerPartition.get(partitionPath)
                : 0L;
        totalRecords += numRecords;
      }
      logger.info("TotalRecords: " + totalRecords + ", TotalFiles: " + totalFiles
          + ", TotalAffectedPartitions:" + recordsPerPartition.size());
    }

    // each partition will have an item per comparison.
    int parallelism = (int) (totalComparisons / MAX_ITEMS_PER_SHUFFLE_PARTITION + 1);
    logger.info(
        "Auto computed parallelism :" + parallelism + ", totalComparisons: " + totalComparisons);
    return parallelism;
  }

  /**
   * Its crucial to pick the right parallelism.
   * <p>
   * totalSubPartitions : this is deemed safe limit, to be nice with Spark. inputParallelism :
   * typically number of input file splits
   * <p>
   * We pick the max such that, we are always safe, but go higher if say a there are a lot of input
   * files. (otherwise, we will fallback to number of partitions in input and end up with slow
   * performance)
   */
  private int determineParallelism(int inputParallelism, int totalSubPartitions) {
    // If bloom index parallelism is set, use it to to check against the input parallelism and
    // take the max
    int indexParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    int joinParallelism = Math.max(totalSubPartitions, indexParallelism);
    logger.info("InputParallelism: ${" + inputParallelism + "}, " + "IndexParallelism: ${" + config
        .getBloomIndexParallelism() + "}, " + "TotalSubParts: ${" + totalSubPartitions + "}, "
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
    List<Tuple2<String, HoodieDataFile>> dataFilesList = jsc
        .parallelize(partitions, Math.max(partitions.size(), 1)).flatMapToPair(partitionPath -> {
          java.util.Optional<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
              .filterCompletedInstants().lastInstant();
          List<Tuple2<String, HoodieDataFile>> filteredFiles = new ArrayList<>();
          if (latestCommitTime.isPresent()) {
            filteredFiles = hoodieTable.getROFileSystemView()
                .getLatestDataFilesBeforeOrOn(partitionPath, latestCommitTime.get().getTimestamp())
                .map(f -> new Tuple2<>(partitionPath, f)).collect(toList());
          }
          return filteredFiles.iterator();
        }).collect();

    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      return jsc.parallelize(dataFilesList, Math.max(dataFilesList.size(), 1)).mapToPair(ft -> {
        try {
          String[] minMaxKeys = ParquetUtils
              .readMinMaxRecordKeys(hoodieTable.getHadoopConf(), ft._2().getFileStatus().getPath());
          return new Tuple2<>(ft._1(),
              new BloomIndexFileInfo(ft._2().getFileName(), minMaxKeys[0], minMaxKeys[1]));
        } catch (MetadataNotFoundException me) {
          logger.warn("Unable to find range metadata in file :" + ft._2());
          return new Tuple2<>(ft._1(), new BloomIndexFileInfo(ft._2().getFileName()));
        }
      }).collect();
    } else {
      return dataFilesList.stream()
          .map(ft -> new Tuple2<>(ft._1(), new BloomIndexFileInfo(ft._2().getFileName())))
          .collect(toList());
    }
  }


  @Override
  public boolean rollbackCommit(String commitTime) {
    // Nope, don't need to do anything.
    return true;
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup
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
   * if we dont have key ranges, then also we need to compare against the file. no other choice if
   * we do, then only compare the file if the record key falls in range.
   */
  private boolean shouldCompareWithFile(BloomIndexFileInfo indexInfo, String recordKey) {
    return !indexInfo.hasKeyRanges() || indexInfo.isKeyInRange(recordKey);
  }


  /**
   * For each incoming record, produce N output records, 1 each for each file against which the
   * record's key needs to be checked. For datasets, where the keys have a definite insert order
   * (e.g: timestamp as prefix), the number of files to be compared gets cut down a lot from range
   * pruning.
   */
  // sub-partition to ensure the records can be looked up against files & also prune
  // file<=>record comparisons based on recordKey
  // ranges in the index info.
  @VisibleForTesting
  JavaPairRDD<String, Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {
    return partitionRecordKeyPairRDD.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair._2();
      String partitionPath = partitionRecordKeyPair._1();

      List<BloomIndexFileInfo> indexInfos = partitionToFileIndexInfo.get(partitionPath);
      List<Tuple2<String, Tuple2<String, HoodieKey>>> recordComparisons = new ArrayList<>();
      if (indexInfos != null) { // could be null, if there are no files in a given partition yet.
        // for each candidate file in partition, that needs to be compared.
        for (BloomIndexFileInfo indexInfo : indexInfos) {
          if (shouldCompareWithFile(indexInfo, recordKey)) {
            recordComparisons.add(
                new Tuple2<>(String.format("%s#%s", indexInfo.getFileName(), recordKey),
                    new Tuple2<>(indexInfo.getFileName(),
                        new HoodieKey(recordKey, partitionPath))));
          }
        }
      }
      return recordComparisons;
    }).flatMapToPair(t -> t.iterator());
  }

  /**
   * Find out <RowKey, filename> pair. All workload grouped by file-level.
   * <p>
   * Join PairRDD(PartitionPath, RecordKey) and PairRDD(PartitionPath, File) & then repartition such
   * that each RDD partition is a file, then for each file, we do (1) load bloom filter, (2) load
   * rowKeys, (3) Tag rowKey
   * <p>
   * Make sure the parallelism is atleast the groupby parallelism for tagging location
   */
  @VisibleForTesting
  JavaPairRDD<String, String> findMatchingFilesForRecordKeys(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD, int totalSubpartitions, HoodieTableMetaClient metaClient) {

    int joinParallelism = determineParallelism(partitionRecordKeyPairRDD.partitions().size(),
        totalSubpartitions);

    JavaPairRDD<String, Tuple2<String, HoodieKey>> fileSortedTripletRDD =
        explodeRecordRDDWithFileComparisons(
            partitionToFileIndexInfo, partitionRecordKeyPairRDD)
            // sort further based on filename, such that all checking for the file can happen within
            // a single partition, on-the-fly
            .sortByKey(true, joinParallelism);

    return fileSortedTripletRDD.mapPartitionsWithIndex(
        new HoodieBloomIndexCheckFunction(metaClient, config.getBasePath()), true)
        .flatMap(indexLookupResults -> indexLookupResults.iterator())
        .filter(lookupResult -> lookupResult.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> {
          List<Tuple2<String, String>> vals = new ArrayList<>();
          for (String recordKey : lookupResult.getMatchingRecordKeys()) {
            vals.add(new Tuple2<>(recordKey, lookupResult.getFileName()));
          }
          return vals.iterator();
        });
  }

  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord RDD.
   */
  private JavaRDD<HoodieRecord<T>> tagLocationBacktoRecords(
      JavaPairRDD<String, String> rowKeyFilenamePairRDD, JavaRDD<HoodieRecord<T>> recordRDD) {
    JavaPairRDD<String, HoodieRecord<T>> rowKeyRecordPairRDD = recordRDD
        .mapToPair(record -> new Tuple2<>(record.getRecordKey(), record));

    // Here as the recordRDD might have more data than rowKeyRDD (some rowKeys' fileId is null),
    // so we do left outer join.
    return rowKeyRecordPairRDD.leftOuterJoin(rowKeyFilenamePairRDD).values().map(v1 -> {
      HoodieRecord<T> record = v1._1();
      if (v1._2().isPresent()) {
        String filename = v1._2().get();
        if (filename != null && !filename.isEmpty()) {
          // When you have a record in multiple files in the same partition, then rowKeyRecordPairRDD will have 2
          // entries with the same exact in memory copy of the HoodieRecord and the 2 separate filenames that the
          // record is found in. This will result in setting currentLocation 2 times and it will fail the second time.
          // This check will create a new in memory copy of the hoodie record.
          if (record.getCurrentLocation() != null) {
            record = new HoodieRecord<T>(record.getKey(), record.getData());
          }
          record.setCurrentLocation(new HoodieRecordLocation(FSUtils.getCommitTime(filename),
              FSUtils.getFileId(filename)));
        }
      }
      return record;
    });
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    return writeStatusRDD;
  }
}
