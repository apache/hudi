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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Helper for {@link HoodieBloomIndex} containing Spark-specific logic.
 */
public class SparkHoodieMetadataBloomIndexHelper extends MetadataBaseHoodieBloomIndexHelper {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieMetadataBloomIndexHelper.class);
  private static final SparkHoodieMetadataBloomIndexHelper SINGLETON_INSTANCE =
      new SparkHoodieMetadataBloomIndexHelper();

  private SparkHoodieMetadataBloomIndexHelper() {
  }

  public static SparkHoodieMetadataBloomIndexHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  protected HoodieData<Pair<String, IndexFileFilter>> buildPartitionIndexFileFilter(
      HoodieWriteConfig config, final HoodieEngineContext context, final HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs) {
    final String keyField = hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
    final int inputParallelism = HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).partitions().size();
    final int joinParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    LOG.info("Building partition index file filter, input parallelism: " + inputParallelism
        + ", bloom index parallelism: " + config.getBloomIndexParallelism());
    JavaRDD<String> partitionsRDD = HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).keys().distinct().repartition(joinParallelism);

    JavaRDD<Pair<String, IndexFileFilter>> partitionIndexFileFilterRDD = partitionsRDD.mapPartitionsWithIndex(
        new Function2<Integer, Iterator<String>, Iterator<Pair<String, IndexFileFilter>>>() {
          @Override
          public Iterator<Pair<String, IndexFileFilter>> call(Integer integer, Iterator<String> partitionItr) throws Exception {
            HoodieTimer timer = new HoodieTimer().startTimer();
            final List<Pair<String, IndexFileFilter>> result = new ArrayList<>();
            final List<Pair<String, String>> columnStatKeys = new ArrayList<>();
            long partitionCount = 0;
            while (partitionItr.hasNext()) {
              String partitionName = partitionItr.next();
              List<String> partitionFileNameList = HoodieIndexUtils.getLatestBaseFilesForPartition(partitionName,
                      hoodieTable).stream().map(baseFile -> baseFile.getFileName())
                  .collect(toList());
              partitionCount++;

              if (partitionFileNameList.isEmpty()) {
                result.add(new ImmutablePair<String, IndexFileFilter>(partitionName,
                    new IntervalTreeBasedIndexFileFilter(partitionName, Collections.emptyList())));
                continue;
              }

              for (String fileName : partitionFileNameList) {
                Pair<String, String> partitionFileNameKey = Pair.of(partitionName, fileName);
                columnStatKeys.add(partitionFileNameKey);
              }
            }
            LOG.debug("Loaded base files for " + partitionCount + " partitons, time taken: " + timer.endTimer());

            try {
              timer = new HoodieTimer().startTimer();
              Map<Pair<String, String>, HoodieMetadataColumnStats> fileToColumnStatMap = hoodieTable
                  .getMetadataTable().getColumnStats(columnStatKeys, keyField);
              LOG.debug("Loaded column ranges for " + columnStatKeys.size() + " keys, time taken: " + timer.endTimer());

              Map<String, List<BloomIndexFileInfo>> partitionToFilesBloomInfoMap = fileToColumnStatMap.entrySet()
                  .stream().map(entry -> {
                    final BloomIndexFileInfo bloomIndexFileInfo = new BloomIndexFileInfo(
                        FSUtils.getFileId(entry.getKey().getRight()),
                        entry.getValue().getMinValue(),
                        entry.getValue().getMaxValue());
                    return new ImmutablePair<>(entry.getKey().getLeft(), bloomIndexFileInfo);
                  }).collect(Collectors.groupingBy(p -> p.getKey(), Collectors.mapping(p -> p.getValue(), toList())));

              result.addAll(partitionToFilesBloomInfoMap.entrySet().stream().map(entry -> {
                return new ImmutablePair<String, IndexFileFilter>(entry.getKey(),
                    new IntervalTreeBasedIndexFileFilter(entry.getKey(), entry.getValue()));
              }).collect(Collectors.toList()));
            } catch (MetadataNotFoundException me) {
              throw new HoodieMetadataException("Unable to find column range metadata!", me);
            }
            return result.iterator();
          }
        }, true);
    return HoodieJavaRDD.of(partitionIndexFileFilterRDD);
  }

  @Override
  public HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodieData<ImmutablePair<String, HoodieKey>> fileCandidateKeyPairs) {
    JavaRDD<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        HoodieJavaRDD.getJavaRDD(fileCandidateKeyPairs)
            .map(pair -> new Tuple2<>(pair.getLeft(), pair.getRight()));

    final int inputParallelism = HoodieJavaPairRDD.getJavaPairRDD(partitionRecordKeyPairs).partitions().size();
    final int joinParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    LOG.debug("Find matching files from bloom filter index, input parallelism: " + inputParallelism
        + ", bloom index parallelism: " + config.getBloomIndexParallelism());

    // Step 1: Sort by file id
    JavaRDD<Tuple2<String, HoodieKey>> sortedFileIdAndKeyPairs =
        fileComparisonsRDD.sortBy(Tuple2::_1, true, joinParallelism);

    // Step 2: Use bloom filter to filter and the actual log file to get the record location
    JavaRDD<List<HoodieKeyLookupResult>> keyLookupResultRDD = sortedFileIdAndKeyPairs.mapPartitionsWithIndex(
        new HoodieMetadataBloomIndexCheckFunction(hoodieTable), true);

    // Step 3: Do the actual key check in the file
    return HoodieJavaPairRDD.of(keyLookupResultRDD.flatMap(List::iterator)
        .filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(Collectors.toList()).iterator()));
  }
}
