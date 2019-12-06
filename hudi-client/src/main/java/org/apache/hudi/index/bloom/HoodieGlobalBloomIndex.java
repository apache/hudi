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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * This filter will only work with hoodie dataset since it will only load partitions with .hoodie_partition_metadata
 * file in it.
 */
public class HoodieGlobalBloomIndex<T extends HoodieRecordPayload> extends HoodieBloomIndex<T> {

  public HoodieGlobalBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD from all partitions in the table.
   */
  @Override
  @VisibleForTesting
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, final JavaSparkContext jsc,
      final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    try {
      List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
          config.shouldAssumeDatePartitioning());
      return super.loadInvolvedFiles(allPartitionPaths, jsc, hoodieTable);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load all partitions", e);
    }
  }

  /**
   * For each incoming record, produce N output records, 1 each for each file against which the record's key needs to be
   * checked. For datasets, where the keys have a definite insert order (e.g: timestamp as prefix), the number of files
   * to be compared gets cut down a lot from range pruning.
   *
   * Sub-partition to ensure the records can be looked up against files & also prune file<=>record comparisons based on
   * recordKey ranges in the index info. the partition path of the incoming record (partitionRecordKeyPairRDD._2()) will
   * be ignored since the search scope should be bigger than that
   */

  @Override
  @VisibleForTesting
  JavaRDD<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {
    Map<String, String> indexToPartitionMap = new HashMap<>();
    for (Entry<String, List<BloomIndexFileInfo>> entry : partitionToFileIndexInfo.entrySet()) {
      entry.getValue().forEach(indexFile -> indexToPartitionMap.put(indexFile.getFileId(), entry.getKey()));
    }

    IndexFileFilter indexFileFilter =
        config.getBloomIndexPruneByRanges() ? new IntervalTreeBasedGlobalIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedGlobalIndexFileFilter(partitionToFileIndexInfo);

    return partitionRecordKeyPairRDD.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair._2();
      String partitionPath = partitionRecordKeyPair._1();

      return indexFileFilter.getMatchingFiles(partitionPath, recordKey).stream()
          .map(file -> new Tuple2<>(file, new HoodieKey(recordKey, indexToPartitionMap.get(file))))
          .collect(Collectors.toList());
    }).flatMap(List::iterator);
  }


  /**
   * Tagging for global index should only consider the record key.
   */
  @Override
  protected JavaRDD<HoodieRecord<T>> tagLocationBacktoRecords(
      JavaPairRDD<HoodieKey, HoodieRecordLocation> keyFilenamePairRDD, JavaRDD<HoodieRecord<T>> recordRDD) {
    JavaPairRDD<String, HoodieRecord<T>> rowKeyRecordPairRDD =
        recordRDD.mapToPair(record -> new Tuple2<>(record.getRecordKey(), record));

    // Here as the recordRDD might have more data than rowKeyRDD (some rowKeys' fileId is null),
    // so we do left outer join.
    return rowKeyRecordPairRDD.leftOuterJoin(keyFilenamePairRDD.mapToPair(p -> new Tuple2<>(p._1.getRecordKey(), p._2)))
        .values().map(value -> getTaggedRecord(value._1, Option.ofNullable(value._2.orNull())));
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
