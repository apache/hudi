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
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Helper for {@link HoodieBloomIndex} containing Java {@link List}-based logic.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ListBasedHoodieBloomIndexHelper extends BaseHoodieBloomIndexHelper {

  private static final ListBasedHoodieBloomIndexHelper SINGLETON_INSTANCE = new ListBasedHoodieBloomIndexHelper();

  public static ListBasedHoodieBloomIndexHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  @Override
  public HoodiePairData<HoodieKey, HoodieRecordLocation> findMatchingFilesForRecordKeys(
      HoodieWriteConfig config, HoodieEngineContext context, HoodieTable hoodieTable,
      HoodiePairData<String, String> partitionRecordKeyPairs,
      HoodiePairData<HoodieFileGroupId, String> fileComparisonPairs,
      Map<String, List<BloomIndexFileInfo>> partitionToFileInfo, Map<String, Long> recordsPerPartition) {
    List<Pair<HoodieFileGroupId, String>> fileComparisonPairList =
        fileComparisonPairs.collectAsList().stream()
            .sorted(Comparator.comparing(Pair::getLeft)).collect(toList());

    List<HoodieKeyLookupResult> keyLookupResults =
        CollectionUtils.toStream(
            new HoodieBloomIndexCheckFunction<Pair<HoodieFileGroupId, String>>(hoodieTable, config, Pair::getLeft, Pair::getRight)
                .apply(fileComparisonPairList.iterator())
            )
            .flatMap(Collection::stream)
            .filter(lr -> lr.getMatchingRecordKeysAndPositions().size() > 0)
            .collect(toList());

    return context.parallelize(keyLookupResults).flatMap(lookupResult ->
        lookupResult.getMatchingRecordKeysAndPositions().stream()
            .map(recordKey -> new ImmutablePair<>(lookupResult, recordKey)).iterator()
    ).mapToPair(pair -> {
      HoodieKeyLookupResult lookupResult = pair.getLeft();
      String recordKey = pair.getRight().getLeft();
      long recordPosition = pair.getRight().getRight();
      return new ImmutablePair<>(
          new HoodieKey(recordKey, lookupResult.getPartitionPath()),
          new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId(), recordPosition));
    });
  }
}
