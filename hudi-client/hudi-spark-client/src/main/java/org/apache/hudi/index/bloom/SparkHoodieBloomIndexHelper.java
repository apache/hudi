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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.metadata.HoodieMetadataPayload;
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

import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

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
      HoodieData<Pair<Pair<String, String>, HoodieKey>> fileComparisonPairs,
      Map<String, Map<String, BloomIndexFileInfo>> partitionToFileInfo,
      Map<String, Long> recordsPerPartition) {
    JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> fileComparisonsRDD =
        HoodieJavaRDD.getJavaRDD(fileComparisonPairs)
            .map(pair -> new Tuple2<>(
                new Tuple2<>(pair.getLeft().getLeft(), pair.getLeft().getRight()),
                pair.getRight()));

    int inputParallelism = HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).partitions().size();
    int joinParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    LOG.info("InputParallelism: ${" + inputParallelism + "}, IndexParallelism: ${"
        + config.getBloomIndexParallelism() + "}");

    JavaRDD<List<HoodieKeyLookupResult>> keyLookupResultRDD;
    if (config.getBloomIndexUseMetadata()
        && hoodieTable.getMetaClient().getTableConfig().getMetadataPartitions()
        .contains(BLOOM_FILTERS.getPartitionPath())) {
      // Step 1: Sort by bloom filter key (Hash ID of partition and file name) in metadata table
      JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> sortedFileIdAndKeyPairs =
          fileComparisonsRDD
              .sortBy(t -> HoodieMetadataPayload.getBloomFilterIndexKey(
                  new PartitionIndexID(t._1._1), new FileIndexID(t._1._2)), true, joinParallelism);

      // Step 2: Use bloom filter to filter and the actual log file to get the record location
      keyLookupResultRDD = sortedFileIdAndKeyPairs.coalesce(config.getBloomIndexMetadataReadParallelism())
          .mapPartitionsWithIndex(new HoodieMetadataBloomIndexCheckFunction(
              hoodieTable,
              config.getBasePath(),
              config.getBloomIndexMetadataBloomFilterReadBatchSize()), true);
    } else if (config.useBloomIndexBucketizedChecking()) {
      Map<String, Long> comparisonsPerFileGroup = computeComparisonsPerFileGroup(
          config, recordsPerPartition, partitionToFileInfo, fileComparisonsRDD, context);
      Partitioner partitioner = new BucketizedBloomCheckPartitioner(joinParallelism, comparisonsPerFileGroup,
          config.getBloomIndexKeysPerBucket());

      keyLookupResultRDD = fileComparisonsRDD
          .mapToPair(t -> new Tuple2<>(Pair.of(t._1._1, t._2.getRecordKey()), new Tuple2<>(t._1._1, t._2)))
          .repartitionAndSortWithinPartitions(partitioner)
          .map(Tuple2::_2)
          .mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true);
    } else {
      keyLookupResultRDD = fileComparisonsRDD
          .map(t -> new Tuple2<>(t._1._1, t._2))
          .sortBy(t -> t._1, true, joinParallelism)
          .mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true);
    }

    return HoodieJavaPairRDD.of(keyLookupResultRDD.flatMap(List::iterator)
        .filter(lr -> lr.getMatchingRecordKeys().size() > 0)
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
      final Map<String, Map<String, BloomIndexFileInfo>> partitionToFileInfo,
      final JavaRDD<Tuple2<Tuple2<String, String>, HoodieKey>> fileComparisonsRDD,
      final HoodieEngineContext context) {
    Map<String, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      context.setJobStatus(this.getClass().getSimpleName(), "Compute all comparisons needed between records and files: " + config.getTableName());
      fileToComparisons = fileComparisonsRDD.mapToPair(t -> new Tuple2<>(t._1._1, t._2)).countByKey();
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.forEach((key, infoMap) -> {
        for (BloomIndexFileInfo fileInfo : infoMap.values()) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(fileInfo.getFileId(), recordsPerPartition.get(key));
        }
      });
    }
    return fileToComparisons;
  }
}
