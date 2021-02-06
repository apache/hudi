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

package org.apache.hudi.index.simple;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.table.HoodieTable;

import avro.shaded.com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * A simple index which reads interested fields(record key and partition path) from base files and
 * compares with incoming records to find the tagged location.
 *
 * @param <T> type of payload
 */
public class FlinkHoodieSimpleIndex<T extends HoodieRecordPayload> extends FlinkHoodieIndex<T> {

  public FlinkHoodieSimpleIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatuses, HoodieEngineContext context,
                                          HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException {
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

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> hoodieRecords, HoodieEngineContext context,
                                           HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException {
    return tagLocationInternal(hoodieRecords, context, hoodieTable);
  }

  /**
   * Tags records location for incoming records.
   */
  private List<HoodieRecord<T>> tagLocationInternal(List<HoodieRecord<T>> hoodieRecords, HoodieEngineContext context,
                                                    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    Map<HoodieKey, HoodieRecord<T>> keyedInputRecords = context.mapToPair(hoodieRecords, record -> Pair.of(record.getKey(), record), 0);
    Map<HoodieKey, HoodieRecordLocation> existingLocationsOnTable = fetchRecordLocationsForAffectedPartitions(keyedInputRecords.keySet(), context, hoodieTable, config.getSimpleIndexParallelism());
    List<HoodieRecord<T>> taggedRecords = new LinkedList<>();

    for (Map.Entry<HoodieKey, HoodieRecord<T>> hoodieKeyHoodieRecordEntry : keyedInputRecords.entrySet()) {
      HoodieKey key = hoodieKeyHoodieRecordEntry.getKey();
      HoodieRecord<T> record = hoodieKeyHoodieRecordEntry.getValue();
      if (existingLocationsOnTable.containsKey(key)) {
        taggedRecords.add(HoodieIndexUtils.getTaggedRecord(record, Option.ofNullable(existingLocationsOnTable.get(key))));
      }
    }
    return taggedRecords;
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param keySet      {@link HoodieKey}s for which locations are fetched
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link Map} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  private Map<HoodieKey, HoodieRecordLocation> fetchRecordLocationsForAffectedPartitions(Set<HoodieKey> keySet,
                                                                                         HoodieEngineContext context,
                                                                                         HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable,
                                                                                         int parallelism) {
    List<String> affectedPartitionPathList = keySet.stream().map(HoodieKey::getPartitionPath).distinct().collect(Collectors.toList());
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = getLatestBaseFilesForAllPartitions(affectedPartitionPathList, context, hoodieTable);
    return fetchRecordLocations(context, hoodieTable, parallelism, latestBaseFiles);
  }

  private Map<HoodieKey, HoodieRecordLocation> fetchRecordLocations(HoodieEngineContext context,
                                                                    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable,
                                                                    int parallelism,
                                                                    List<Pair<String, HoodieBaseFile>> latestBaseFiles) {

    List<HoodieKeyLocationFetchHandle<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>>> hoodieKeyLocationFetchHandles =
        context.map(latestBaseFiles, partitionPathBaseFile -> new HoodieKeyLocationFetchHandle<>(config, hoodieTable, partitionPathBaseFile), parallelism);
    Map<HoodieKey, HoodieRecordLocation> recordLocations = new HashMap<>();
    hoodieKeyLocationFetchHandles.stream()
        .flatMap(handle -> Lists.newArrayList(handle.locations()).stream())
        .forEach(x -> x.forEach(y -> recordLocations.put(y.getKey(), y.getRight())));
    return recordLocations;
  }
}
