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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;
import static org.apache.hudi.metadata.HoodieMetadataPayload.unwrapStatisticValueWrapper;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
public class HoodieBloomIndex extends HoodieIndex<Object, Object> {
  private static final Logger LOG = LogManager.getLogger(HoodieBloomIndex.class);

  private final BaseHoodieBloomIndexHelper bloomIndexHelper;

  public HoodieBloomIndex(HoodieWriteConfig config, BaseHoodieBloomIndexHelper bloomIndexHelper) {
    super(config);
    this.bloomIndexHelper = bloomIndexHelper;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    // Step 0: cache the input records if needed
    if (config.getBloomIndexUseCaching()) {
      records.persist(new HoodieConfig(config.getProps())
          .getString(HoodieIndexConfig.BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE));
    }

    // Step 1: Extract out thinner pairs of (partitionPath, recordKey)
    HoodiePairData<String, String> partitionRecordKeyPairs = records.mapToPair(
        record -> new ImmutablePair<>(record.getPartitionPath(), record.getRecordKey()));

    // Step 2: Lookup indexes for all the partition/recordkey pair
    HoodiePairData<HoodieKey, HoodieRecordLocation> keyFilenamePairs =
        lookupIndex(partitionRecordKeyPairs, context, hoodieTable);

    // Cache the result, for subsequent stages.
    if (config.getBloomIndexUseCaching()) {
      keyFilenamePairs.persist("MEMORY_AND_DISK_SER");
    }
    if (LOG.isDebugEnabled()) {
      long totalTaggedRecords = keyFilenamePairs.count();
      LOG.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
    }

    // Step 3: Tag the incoming records, as inserts or updates, by joining with existing record keys
    HoodieData<HoodieRecord<R>> taggedRecords = tagLocationBacktoRecords(keyFilenamePairs, records);

    if (config.getBloomIndexUseCaching()) {
      records.unpersist();
      keyFilenamePairs.unpersist();
    }

    return taggedRecords;
  }

  /**
   * Lookup the location for each record key and return the pair<record_key,location> for all record keys already
   * present and drop the record keys if not present.
   */
  private HoodiePairData<HoodieKey, HoodieRecordLocation> lookupIndex(
      HoodiePairData<String, String> partitionRecordKeyPairs, final HoodieEngineContext context,
      final HoodieTable hoodieTable) {
    // Step 1: Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = partitionRecordKeyPairs.countByKey();
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Pair<String, BloomIndexFileInfo>> fileInfoList = getBloomIndexFileInfoForPartitions(context, hoodieTable, affectedPartitionPathList);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toList())));

    // Step 3: Obtain a HoodieData, for each incoming record, that already exists, with the file id,
    // that contains it.
    HoodiePairData<String, HoodieKey> fileComparisonPairs =
        explodeRecordsWithFileComparisons(partitionToFileInfo, partitionRecordKeyPairs);

    return bloomIndexHelper.findMatchingFilesForRecordKeys(config, context, hoodieTable,
        partitionRecordKeyPairs, fileComparisonPairs, partitionToFileInfo, recordsPerPartition);
  }

  private List<Pair<String, BloomIndexFileInfo>> getBloomIndexFileInfoForPartitions(HoodieEngineContext context,
                                                                                    HoodieTable hoodieTable,
                                                                                    List<String> affectedPartitionPathList) {
    List<Pair<String, BloomIndexFileInfo>> fileInfoList = new ArrayList<>();

    if (config.getBloomIndexPruneByRanges()) {
      // load column ranges from metadata index if column stats index is enabled and column_stats metadata partition is available
      if (config.getBloomIndexUseMetadata()
          && hoodieTable.getMetaClient().getTableConfig().getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath())) {
        fileInfoList = loadColumnRangesFromMetaIndex(affectedPartitionPathList, context, hoodieTable);
      }
      // fallback to loading column ranges from files
      if (isNullOrEmpty(fileInfoList)) {
        fileInfoList = loadColumnRangesFromFiles(affectedPartitionPathList, context, hoodieTable);
      }
    } else {
      fileInfoList = getFileInfoForLatestBaseFiles(affectedPartitionPathList, context, hoodieTable);
    }

    return fileInfoList;
  }

  /**
   * Load all involved files as <Partition, filename> pair List.
   */
  List<Pair<String, BloomIndexFileInfo>> loadColumnRangesFromFiles(
      List<String> partitions, final HoodieEngineContext context, final HoodieTable hoodieTable) {
    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(partitions, context, hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());

    context.setJobStatus(this.getClass().getName(), "Obtain key ranges for file slices (range pruning=on): " + config.getTableName());
    return context.map(partitionPathFileIDList, pf -> {
      try {
        HoodieRangeInfoHandle rangeInfoHandle = new HoodieRangeInfoHandle(config, hoodieTable, pf);
        String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
        return Pair.of(pf.getKey(), new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1]));
      } catch (MetadataNotFoundException me) {
        LOG.warn("Unable to find range metadata in file :" + pf);
        return Pair.of(pf.getKey(), new BloomIndexFileInfo(pf.getValue()));
      }
    }, Math.max(partitionPathFileIDList.size(), 1));
  }

  /**
   * Get BloomIndexFileInfo for all the latest base files for the requested partitions.
   *
   * @param partitions  - List of partitions to get the base files for
   * @param context     - Engine context
   * @param hoodieTable - Hoodie Table
   * @return List of partition and file column range info pairs
   */
  private List<Pair<String, BloomIndexFileInfo>> getFileInfoForLatestBaseFiles(
      List<String> partitions, final HoodieEngineContext context, final HoodieTable hoodieTable) {
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(partitions, context,
        hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());
    return partitionPathFileIDList.stream()
        .map(pf -> Pair.of(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
  }

  /**
   * Load the column stats index as BloomIndexFileInfo for all the involved files in the partition.
   *
   * @param partitions  - List of partitions for which column stats need to be loaded
   * @param context     - Engine context
   * @param hoodieTable - Hoodie table
   * @return List of partition and file column range info pairs
   */
  protected List<Pair<String, BloomIndexFileInfo>> loadColumnRangesFromMetaIndex(
      List<String> partitions, final HoodieEngineContext context, final HoodieTable<?, ?, ?, ?> hoodieTable) {
    // also obtain file ranges, if range pruning is enabled
    context.setJobStatus(this.getClass().getName(), "Load meta index key ranges for file slices: " + config.getTableName());

    final String keyField = hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
    return context.flatMap(partitions, partitionName -> {
      // Partition and file name pairs
      List<Pair<String, String>> partitionFileNameList = HoodieIndexUtils.getLatestBaseFilesForPartition(partitionName,
              hoodieTable).stream().map(baseFile -> Pair.of(partitionName, baseFile.getFileName()))
          .sorted()
          .collect(toList());
      if (partitionFileNameList.isEmpty()) {
        return Stream.empty();
      }
      try {
        Map<Pair<String, String>, HoodieMetadataColumnStats> fileToColumnStatsMap =
            hoodieTable.getMetadataTable().getColumnStats(partitionFileNameList, keyField);
        List<Pair<String, BloomIndexFileInfo>> result = new ArrayList<>();
        for (Map.Entry<Pair<String, String>, HoodieMetadataColumnStats> entry : fileToColumnStatsMap.entrySet()) {
          result.add(Pair.of(entry.getKey().getLeft(),
              new BloomIndexFileInfo(
                  FSUtils.getFileId(entry.getKey().getRight()),
                  // NOTE: Here we assume that the type of the primary key field is string
                  (String) unwrapStatisticValueWrapper(entry.getValue().getMinValue()),
                  (String) unwrapStatisticValueWrapper(entry.getValue().getMaxValue())
              )));
        }
        return result.stream();
      } catch (MetadataNotFoundException me) {
        throw new HoodieMetadataException("Unable to find column range metadata for partition:" + partitionName, me);
      }
    }, Math.max(partitions.size(), 1));
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
  HoodiePairData<String, HoodieKey> explodeRecordsWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      HoodiePairData<String, String> partitionRecordKeyPairs) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedIndexFileFilter(partitionToFileIndexInfo);

    return partitionRecordKeyPairs.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair.getRight();
      String partitionPath = partitionRecordKeyPair.getLeft();

      // TODO(HUDI-4249) avoid collections
      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair -> new ImmutablePair<>(partitionFileIdPair.getRight(),
              new HoodieKey(recordKey, partitionPath)))
          .collect(Collectors.toList());
    })
        .flatMap(List::iterator)
        .mapToPair(t -> t);
  }

  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord List.
   */
  protected <R> HoodieData<HoodieRecord<R>> tagLocationBacktoRecords(
      HoodiePairData<HoodieKey, HoodieRecordLocation> keyFilenamePair,
      HoodieData<HoodieRecord<R>> records) {
    HoodiePairData<HoodieKey, HoodieRecord<R>> keyRecordPairs =
        records.mapToPair(record -> new ImmutablePair<>(record.getKey(), record));
    // Here as the records might have more data than keyFilenamePairs (some row keys' fileId is null),
    // so we do left outer join.
    return keyRecordPairs.leftOuterJoin(keyFilenamePair).values()
        .map(v -> HoodieIndexUtils.getTaggedRecord(v.getLeft(), Option.ofNullable(v.getRight().orElse(null))));
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatusData, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatusData;
  }
}
