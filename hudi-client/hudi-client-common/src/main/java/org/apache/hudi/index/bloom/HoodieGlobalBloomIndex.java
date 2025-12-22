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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.index.HoodieIndexUtils.tagGlobalLocationBackToRecords;

/**
 * This filter will only work with hoodie table since it will only load partitions
 * with .hoodie_partition_metadata file in it.
 */
public class HoodieGlobalBloomIndex extends HoodieBloomIndex {
  public HoodieGlobalBloomIndex(HoodieWriteConfig config, BaseHoodieBloomIndexHelper bloomIndexHelper) {
    super(config, bloomIndexHelper);
  }

  /**
   * Load all involved files as <Partition, filename> pairs from all partitions in the table.
   */
  @Override
  List<Pair<String, BloomIndexFileInfo>> loadColumnRangesFromFiles(List<String> partitions, final HoodieEngineContext context,
                                                                   final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(context, metaClient.getStorage(), config.getMetadataConfig(), metaClient.getBasePath());
    return super.loadColumnRangesFromFiles(allPartitionPaths, context, hoodieTable);
  }

  /**
   * For each incoming record, produce N output records, 1 each for each file against which the record's key needs to be
   * checked. For tables, where the keys have a definite insert order (e.g: timestamp as prefix), the number of files
   * to be compared gets cut down a lot from range pruning.
   * <p>
   * Sub-partition to ensure the records can be looked up against files & also prune file<=>record comparisons based on
   * recordKey ranges in the index info. the partition path of the incoming record (partitionRecordKeyPairs._2()) will
   * be ignored since the search scope should be bigger than that
   */

  @Override
  HoodiePairData<HoodieFileGroupId, String> explodeRecordsWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      HoodiePairData<String, String> partitionRecordKeyPairs) {

    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedGlobalIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedGlobalIndexFileFilter(partitionToFileIndexInfo);

    return partitionRecordKeyPairs.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair.getRight();
      String partitionPath = partitionRecordKeyPair.getLeft();

      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair ->
              new ImmutablePair<>(
                  new HoodieFileGroupId(partitionFileIdPair.getLeft(), partitionFileIdPair.getRight()), recordKey));
    })
        .flatMapToPair(Stream::iterator);
  }

  /**
   * Tagging for global index should only consider the record key.
   */
  @Override
  protected <R> HoodieData<HoodieRecord<R>> tagLocationBacktoRecords(
      HoodiePairData<HoodieKey, HoodieRecordLocation> keyLocationPairs,
      HoodieData<HoodieRecord<R>> records,
      HoodieTable hoodieTable) {
    HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations = keyLocationPairs
        .mapToPair(p -> Pair.of(p.getLeft().getRecordKey(),
            HoodieRecordGlobalLocation.fromLocal(p.getLeft().getPartitionPath(), p.getRight())));
    boolean mayContainDuplicateLookup = hoodieTable.getMetaClient().getTableType() == MERGE_ON_READ;
    boolean shouldUpdatePartitionPath = config.getGlobalBloomIndexUpdatePartitionPath() && hoodieTable.isPartitioned();
    return tagGlobalLocationBackToRecords(records, keyAndExistingLocations,
        mayContainDuplicateLookup, shouldUpdatePartitionPath, config, hoodieTable);
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
