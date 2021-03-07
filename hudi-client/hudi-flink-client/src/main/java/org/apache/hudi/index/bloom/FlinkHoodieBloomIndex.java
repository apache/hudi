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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.io.HoodieRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.beust.jcommander.internal.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
@SuppressWarnings("checkstyle:LineLength")
public class FlinkHoodieBloomIndex<T extends HoodieRecordPayload> extends FlinkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(FlinkHoodieBloomIndex.class);

  public FlinkHoodieBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records, HoodieEngineContext context,
                                           HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    // Step 1: Extract out thinner Map of (partitionPath, recordKey)
    Map<String, List<String>> partitionRecordKeyMap = new HashMap<>();
    records.forEach(record -> {
      if (partitionRecordKeyMap.containsKey(record.getPartitionPath())) {
        partitionRecordKeyMap.get(record.getPartitionPath()).add(record.getRecordKey());
      } else {
        List<String> recordKeys = Lists.newArrayList();
        recordKeys.add(record.getRecordKey());
        partitionRecordKeyMap.put(record.getPartitionPath(), recordKeys);
      }
    });

    // Step 2: Lookup indexes for all the partition/recordkey pair
    Map<HoodieKey, HoodieRecordLocation> keyFilenamePairMap =
        lookupIndex(partitionRecordKeyMap, context, hoodieTable);

    if (LOG.isDebugEnabled()) {
      long totalTaggedRecords = keyFilenamePairMap.values().size();
      LOG.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
    }

    // Step 3: Tag the incoming records, as inserts or updates, by joining with existing record keys
    List<HoodieRecord<T>> taggedRecords = tagLocationBacktoRecords(keyFilenamePairMap, records);

    return taggedRecords;
  }

  /**
   * Lookup the location for each record key and return the pair<record_key,location> for all record keys already
   * present and drop the record keys if not present.
   */
  private Map<HoodieKey, HoodieRecordLocation> lookupIndex(
      Map<String, List<String>> partitionRecordKeyMap, final HoodieEngineContext context,
      final HoodieTable hoodieTable) {
    // Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = new HashMap<>();
    partitionRecordKeyMap.keySet().forEach(k -> recordsPerPartition.put(k, Long.valueOf(partitionRecordKeyMap.get(k).size())));
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Pair<String, BloomIndexFileInfo>> fileInfoList =
        loadInvolvedFiles(affectedPartitionPathList, context, hoodieTable);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toList())));

    // Step 3: Obtain a List, for each incoming record, that already exists, with the file id,
    // that contains it.
    List<Pair<String, HoodieKey>> fileComparisons =
            explodeRecordsWithFileComparisons(partitionToFileInfo, partitionRecordKeyMap);
    return findMatchingFilesForRecordKeys(fileComparisons, hoodieTable);
  }

  /**
   * Load all involved files as <Partition, filename> pair List.
   */
  //TODO duplicate code with spark, we can optimize this method later
  List<Pair<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, final HoodieEngineContext context,
                                                             final HoodieTable hoodieTable) {
    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(partitions, context, hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());

    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      context.setJobStatus(this.getClass().getName(), "Obtain key ranges for file slices (range pruning=on)");
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
    } else {
      return partitionPathFileIDList.stream()
          .map(pf -> Pair.of(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }
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
  List<Pair<String, HoodieKey>> explodeRecordsWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      Map<String, List<String>> partitionRecordKeyMap) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedIndexFileFilter(partitionToFileIndexInfo);

    List<Pair<String, HoodieKey>> fileRecordPairs = new ArrayList<>();
    partitionRecordKeyMap.keySet().forEach(partitionPath ->  {
      List<String> hoodieRecordKeys = partitionRecordKeyMap.get(partitionPath);
      hoodieRecordKeys.forEach(hoodieRecordKey -> {
        indexFileFilter.getMatchingFilesAndPartition(partitionPath, hoodieRecordKey).forEach(partitionFileIdPair -> {
          fileRecordPairs.add(Pair.of(partitionFileIdPair.getRight(),
                  new HoodieKey(hoodieRecordKey, partitionPath)));
        });
      });
    });
    return fileRecordPairs;
  }

  /**
   * Find out <RowKey, filename> pair.
   */
  Map<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      List<Pair<String, HoodieKey>> fileComparisons,
      HoodieTable hoodieTable) {

    fileComparisons = fileComparisons.stream().sorted((o1, o2) -> o1.getLeft().compareTo(o2.getLeft())).collect(toList());

    List<HoodieKeyLookupHandle.KeyLookupResult> keyLookupResults = new ArrayList<>();

    Iterator<List<HoodieKeyLookupHandle.KeyLookupResult>> iterator = new HoodieFlinkBloomIndexCheckFunction(hoodieTable, config).apply(fileComparisons.iterator());
    while (iterator.hasNext()) {
      keyLookupResults.addAll(iterator.next());
    }

    Map<HoodieKey, HoodieRecordLocation> hoodieRecordLocationMap = new HashMap<>();

    keyLookupResults = keyLookupResults.stream().filter(lr -> lr.getMatchingRecordKeys().size() > 0).collect(toList());
    keyLookupResults.forEach(lookupResult -> {
      lookupResult.getMatchingRecordKeys().forEach(r -> {
        hoodieRecordLocationMap.put(new HoodieKey(r, lookupResult.getPartitionPath()), new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId()));
      });
    });

    return hoodieRecordLocationMap;
  }


  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord List.
   */
  protected List<HoodieRecord<T>> tagLocationBacktoRecords(
          Map<HoodieKey, HoodieRecordLocation> keyFilenamePair, List<HoodieRecord<T>> records) {
    Map<HoodieKey, HoodieRecord<T>> keyRecordPairMap = new HashMap<>();
    records.forEach(r -> keyRecordPairMap.put(r.getKey(), r));
    // Here as the record might have more data than rowKey (some rowKeys' fileId is null),
    // so we do left outer join.
    List<Pair<HoodieRecord<T>, HoodieRecordLocation>> newList = new ArrayList<>();
    keyRecordPairMap.keySet().forEach(k -> {
      if (keyFilenamePair.containsKey(k)) {
        newList.add(Pair.of(keyRecordPairMap.get(k), keyFilenamePair.get(k)));
      } else {
        newList.add(Pair.of(keyRecordPairMap.get(k), null));
      }
    });
    List<HoodieRecord<T>> res = Lists.newArrayList();
    for (Pair<HoodieRecord<T>, HoodieRecordLocation> v : newList) {
      res.add(HoodieIndexUtils.getTaggedRecord(v.getLeft(), Option.ofNullable(v.getRight())));
    }
    return res;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusList, HoodieEngineContext context,
                                          HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    return writeStatusList;
  }
}
