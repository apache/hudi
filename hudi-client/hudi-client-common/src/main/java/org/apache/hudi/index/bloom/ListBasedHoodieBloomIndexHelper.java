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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Helper for {@link HoodieBloomIndex} containing Java {@link List}-based logic.
 */
public class ListBasedHoodieBloomIndexHelper extends BaseHoodieBloomIndexHelper {

  private static final ListBasedHoodieBloomIndexHelper SINGLETON_INSTANCE = new ListBasedHoodieBloomIndexHelper();

  protected ListBasedHoodieBloomIndexHelper() {
  }

  public static ListBasedHoodieBloomIndexHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  @Override
  public HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodieData<Pair<String, HoodieKey>> fileComparisonPairs,
      Map<String, List<BloomIndexFileInfo>> partitionToFileInfo, Map<String, Long> recordsPerPartition) {
    List<Pair<String, HoodieKey>> fileComparisonPairList =
        HoodieListData.getList(fileComparisonPairs).stream()
            .sorted(Comparator.comparing(Pair::getLeft)).collect(toList());

    List<HoodieKeyLookupResult> keyLookupResults = new ArrayList<>();
    Iterator<List<HoodieKeyLookupResult>> iterator = new HoodieBaseBloomIndexCheckFunction(
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
      HoodieKeyLookupResult lookupResult = pair.getLeft();
      String recordKey = pair.getRight();
      return new ImmutablePair<>(
          new HoodieKey(recordKey, lookupResult.getPartitionPath()),
          new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId()));
    });
  }
}
