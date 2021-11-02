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
      fileComparisonsRDD = fileComparisonsRDD.sortBy(Tuple2::_1, true, joinParallelism);
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
