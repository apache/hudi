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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Helper for {@link HoodieBloomIndex} containing Spark-specific logic.
 */
public class SparkHoodieBloomIndexHelper extends BaseHoodieBloomIndexHelper {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBloomIndexHelper.class);

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
      HoodieData<ImmutablePair<String, HoodieKey>> fileComparisonPairs,
      Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      Map<String, Long> recordsPerPartition) {

    // Pair<fileId, key> => JavaRDD<Tuple2<fileID, key>>
    JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        HoodieJavaRDD.getJavaRDD(fileComparisonPairs)
            .map(pair -> new Tuple2<>(pair.getLeft(), pair.getRight()));

    Map<String, Long> comparisonsPerFileGroup = computeComparisonsPerFileGroup(
        config, recordsPerPartition, partitionToFileInfo, fileComparisonsRDD, context);
    int inputParallelism =
        HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).partitions().size();
    int joinParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    LOG.info("InputParallelism: ${" + inputParallelism + "}, IndexParallelism: ${"
        + config.getBloomIndexParallelism() + "}");

    if (config.useBloomIndexBucketizedChecking()) {
      Partitioner partitioner = new BucketizedBloomCheckPartitioner(joinParallelism, comparisonsPerFileGroup,
          config.getBloomIndexKeysPerBucket());

      fileComparisonsRDD = fileComparisonsRDD.mapToPair(t -> new Tuple2<>(Pair.of(t._1, t._2.getRecordKey()), t))
          .repartitionAndSortWithinPartitions(partitioner).map(Tuple2::_2);
    } else {

      // Step 1: Transform the fileId to name/hash pairs
      // fileId(uuid part of the file), HoodieKey => file_name, HoodieKey
      fileComparisonsRDD = fileComparisonsRDD.map(entry -> new Tuple2<String, HoodieKey>(
          hoodieTable.getBaseFileOnlyView().getLatestBaseFile(entry._2.getPartitionPath(), entry._1).get().getFileName(),
          entry._2));

      // Step 2: File pruning
      // Get the mapping of FileId to list of keys that might be available in the file
      // We need to make use of the column stats instead of the absence of RangeFiltering/ListFiltering
      // that used to happen at HoodieBloomIndex::explodeRecordsWithFileComparisons()
      // <<ColStatHash, filename> key>
      // RDD Tuple2<Tuple2<ColumnIDHash|FileIDHash>, filename>, HoodieKey>
      /*
      JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> columnStatHashAndKeyTuple = fileComparisonsRDD.map(entry -> {
        String fileName = entry._1;
        String columnName = HoodieRecord.RECORD_KEY_METADATA_FIELD; // this is the only col for index lookup
        String columnStatKeyHash =
            new ColumnID(columnName).asBase64EncodedString().concat(new FileID(fileName).asBase64EncodedString());
        return new Tuple2<Tuple2<String, String>, HoodieKey>(new Tuple2<>(columnStatKeyHash, fileName), entry._2);
      }).sortBy(entry -> entry._1._1, true, joinParallelism);

      JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> colStatFilteredFileNames =
          columnStatHashAndKeyTuple.mapPartitions(
              new HoodieBloomMetaIndexColStatFunction(hoodieTable, config), true);

      // Step 3: Sort by file hash
      // <<fileName, hash>, key>
      JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> sortedFileNameHashAndKeyTuples =
          colStatFilteredFileNames.map(entry -> new Tuple2<Tuple2<String, String>, HoodieKey>(new Tuple2<>(entry._1._2,
                  new FileID(entry._1._2).asBase64EncodedString()), entry._2))
              .sortBy(entry -> entry._1._2, true, joinParallelism);
       */

      // Step 3: Sort by file hash
      // <<fileName, hash>, key>
      JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> sortedFileNameHashAndKeyTuples =
          fileComparisonsRDD.map(entry -> new Tuple2<Tuple2<String, String>, HoodieKey>(new Tuple2<>(entry._1,
                  new FileID(entry._1).asBase64EncodedString()), entry._2))
              .sortBy(entry -> entry._1._2, true, joinParallelism);

      // Step 4: Use bloom filter to filter and the actual log file to get the record location
      final boolean isBloomFiltersBatchLoadEnabled = config.getMetadataConfig().isBloomFiltersBatchLoadEnabled();
      if (isBloomFiltersBatchLoadEnabled) {
        return HoodieJavaPairRDD.of(sortedFileNameHashAndKeyTuples.mapPartitionsWithIndex(
                new HoodieBloomMetaIndexGroupedFunction(hoodieTable, config), true)
            .flatMap(List::iterator).filter(lr -> lr.getMatchingRecordKeys().size() > 0)
            .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
                .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                    new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
                .collect(Collectors.toList()).iterator()));
      } else {
        return HoodieJavaPairRDD.of(sortedFileNameHashAndKeyTuples.mapPartitionsWithIndex(
                new HoodieBloomMetaIndexCheckFunction(hoodieTable, config), true)
            .flatMap(List::iterator).filter(lr -> lr.getMatchingRecordKeys().size() > 0)
            .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
                .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                    new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
                .collect(Collectors.toList()).iterator()));
      }
    }

    return HoodieJavaPairRDD.of(fileComparisonsRDD.mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true)
        .flatMap(List::iterator).filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(Collectors.toList()).iterator()));
  }

  /**
   * Compute the estimated number of bloom filter comparisons to be performed on each file group.
   */
  private Map<String, Long> computeComparisonsPerFileGroup(
      final HoodieWriteConfig config,
      final Map<String, Long> recordsPerPartition,
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
}
