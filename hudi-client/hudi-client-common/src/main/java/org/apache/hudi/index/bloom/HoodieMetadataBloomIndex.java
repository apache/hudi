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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata table based bloom filter and column stats indexing. This index implementation
 * queries the metadata table for the base file column stats index for range pruning and
 * the bloom filters index to find the candidate keys.
 */
public class HoodieMetadataBloomIndex<T extends HoodieRecordPayload<T>> extends HoodieBaseBloomIndex<T> {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataBloomIndex.class);

  private final MetadataBaseHoodieBloomIndexHelper bloomIndexHelper;

  public HoodieMetadataBloomIndex(HoodieWriteConfig config, MetadataBaseHoodieBloomIndexHelper bloomIndexHelper) {
    super(config);
    this.bloomIndexHelper = bloomIndexHelper;
  }

  @Override
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records, HoodieEngineContext context,
                                                 HoodieTable hoodieTable) throws HoodieIndexException {
    final HoodiePairData<HoodieKey, HoodieRecordLocation> keyFilenamePairs = lookupIndex(records, context, hoodieTable);
    return tagLocationBackToRecords(keyFilenamePairs, records);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
                                                HoodieTable hoodieTable) throws HoodieIndexException {
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  /**
   * Lookup columns stats and bloom filters index for the existence of a record.
   *
   * @param records     - Records to lookup
   * @param context     - Engine context
   * @param hoodieTable - Table reference
   * @return Pair data of key and its current record location for the already existing keys
   */
  private HoodiePairData<HoodieKey, HoodieRecordLocation> lookupIndex(HoodieData<HoodieRecord<T>> records,
                                                                      final HoodieEngineContext context,
                                                                      final HoodieTable hoodieTable) {
    setContextDetails(context, "Extract partition paths from records");
    HoodiePairData<String, String> partitionRecordKeyPairs = records.mapToPair(
        record -> new ImmutablePair<>(record.getPartitionPath(), record.getRecordKey()));

    setContextDetails(context, "Build IntervalTree for the affected partitions from column range index");
    HoodieData<Pair<String, IndexFileFilter>> partitionIndexFileFilter =
        bloomIndexHelper.buildPartitionIndexFileFilter(config, context, hoodieTable, partitionRecordKeyPairs);

    setContextDetails(context, "Find candidate files and record keys from range pruning");
    HoodieData<ImmutablePair<String, HoodieKey>> fileCandidateKeysPairs =
        findCandidateFilesForRecordKeys(partitionIndexFileFilter, partitionRecordKeyPairs);

    setContextDetails(context, "Find matching files for record keys");
    return bloomIndexHelper.findMatchingFilesForRecordKeys(config, context, hoodieTable,
        partitionRecordKeyPairs, fileCandidateKeysPairs);
  }

  /**
   * For each record key, use the partition IndexFileFilter to get its
   * list of candidate files where the records might be available.
   */
  HoodieData<ImmutablePair<String, HoodieKey>> findCandidateFilesForRecordKeys(
      final HoodieData<Pair<String, IndexFileFilter>> partitionIndexFileFilter,
      HoodiePairData<String, String> partitionRecordKeyPairs) {
    final Map<String, IndexFileFilter> partitionFileFilters = partitionIndexFileFilter.collectAsList()
        .stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return partitionRecordKeyPairs.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair.getRight();
      String partitionPath = partitionRecordKeyPair.getLeft();
      ValidationUtils.checkState(partitionFileFilters.containsKey(partitionPath));

      final IndexFileFilter indexFileFilter = partitionFileFilters.get(partitionPath);
      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair -> new ImmutablePair<>(partitionFileIdPair.getRight(),
              new HoodieKey(recordKey, partitionPath)));
    }).flatMap(list -> list.iterator());
  }

  private void setContextDetails(final HoodieEngineContext context, final String message) {
    context.setJobStatus(this.getClass().getName(), "MetadataBloomIndex: " + message);
  }
}


