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
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLookupHandle.KeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Helper for {@link HoodieBloomIndex} containing engine-specific logic,
 * with default logic supporting Java engines.
 */
public class HoodieBloomIndexHelper implements Serializable {

  private static final HoodieBloomIndexHelper SINGLETON_INSTANCE = new HoodieBloomIndexHelper();

  protected HoodieBloomIndexHelper() {
  }

  public static HoodieBloomIndexHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  /**
   * Find out <RowKey, filename> pair.
   *
   * @param config                  Write config.
   * @param context                 {@link HoodieEngineContext} instance to use.
   * @param hoodieTable             {@link HoodieTable} instance to use.
   * @param partitionRecordKeyPairs Pairs of partition path and record key.
   * @param fileComparisonPairs     Pairs of filename and record key based on file comparisons.
   * @param partitionToFileInfo     Partition path to {@link BloomIndexFileInfo} map.
   * @param recordsPerPartition     Number of records per partition in a map.
   * @return {@link HoodiePairData} of {@link HoodieKey} and {@link HoodieRecordLocation} pairs.
   */
  public HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodieData<ImmutablePair<String, HoodieKey>> fileComparisonPairs,
      Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
      Map<String, Long> recordsPerPartition) {

    List<Pair<String, HoodieKey>> fileComparisonPairList =
        HoodieList.getList(fileComparisonPairs).stream()
            .sorted(Comparator.comparing(ImmutablePair::getLeft)).collect(toList());

    List<KeyLookupResult> keyLookupResults = new ArrayList<>();

    Iterator<List<KeyLookupResult>> iterator = new HoodieBaseBloomIndexCheckFunction(
        hoodieTable, config).apply(fileComparisonPairList.iterator());
    while (iterator.hasNext()) {
      keyLookupResults.addAll(iterator.next());
    }

    keyLookupResults = keyLookupResults.stream().filter(
        lr -> lr.getMatchingRecordKeys().size() > 0).collect(toList());
    return context.parallelize(keyLookupResults).flatMap(lookupResult ->
        lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new ImmutablePair<>(lookupResult, recordKey)).iterator()
    ).mapToPair(pair -> {
      KeyLookupResult lookupResult = pair.getLeft();
      String recordKey = pair.getRight();
      return new ImmutablePair<>(
          new HoodieKey(recordKey, lookupResult.getPartitionPath()),
          new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId()));
    });
  }
}
