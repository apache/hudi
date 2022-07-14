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

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), metaClient.getBasePath());
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
  HoodiePairData<String, HoodieKey> explodeRecordsWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      HoodiePairData<HoodieKey, ? extends HoodieRecord> records) {

    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedGlobalIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedGlobalIndexFileFilter(partitionToFileIndexInfo);

    return records.map(keyRecordPair -> {
      HoodieKey key = keyRecordPair.getKey();

      return indexFileFilter.getMatchingFilesAndPartition(key.getPartitionPath(), key.getRecordKey())
          .stream()
          .map(partitionFileIdPair -> (Pair<String, HoodieKey>) new ImmutablePair<>(partitionFileIdPair.getRight(), key))
          .collect(Collectors.toList());
    })
        .flatMapToPair(List::iterator);
  }

  /**
   * Tagging for global index should only consider the record key.
   */
  @Override
  protected <R> HoodiePairData<HoodieKey, HoodieRecord<R>> tagLocationBackToRecords(
      HoodiePairData<HoodieKey, HoodieRecordLocation> keyLocationPairs,
      HoodiePairData<HoodieKey, HoodieRecord<R>> records) {

    // Here as the records might have more data than rowKeys (some rowKeys' fileId is null), so we do left outer join.
    return records.leftOuterJoin(keyLocationPairs)
      .mapValues(record -> {
        final HoodieRecord<R> hoodieRecord = record.getLeft();
        final Option<HoodieRecordLocation> recordLocationOpt = record.getRight();
        if (recordLocationOpt.isPresent()) {
          HoodieKey key = hoodieRecord.getKey();

          // Record key matched to file
          /*
          // TODO this conditional is always false
          if (config.getBloomIndexUpdatePartitionPath()
              && !recordLocationOpt.get().getPartitionPath().equals(hoodieRecord.getPartitionPath())) {
            // Create an empty record to delete the record in the old partition
            HoodieRecord<R> deleteRecord = new HoodieAvroRecord(recordLocationOpt.get().getRight(),
                new EmptyHoodieRecordPayload());
            deleteRecord.setCurrentLocation(recordLocationOpt.get().getLeft());
            deleteRecord.seal();
            // Tag the incoming record for inserting to the new partition
            HoodieRecord<R> insertRecord = HoodieIndexUtils.getTaggedRecord(hoodieRecord, Option.empty());
            return Stream.of(deleteRecord, insertRecord).iterator();
          } else {
            // Ignore the incoming record's partition, regardless of whether it differs from its old partition or not.
            // When it differs, the record will still be updated at its old partition.
            return Collections.singletonList(
                (HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(new HoodieAvroRecord(recordLocationOpt.get().getRight(), (HoodieRecordPayload) hoodieRecord.getData()),
                    Option.ofNullable(recordLocationOpt.get().getLeft()))).iterator();
          }
          */
          return (HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(hoodieRecord, recordLocationOpt);
        } else {
          return (HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(hoodieRecord, Option.empty());
        }
      });
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
