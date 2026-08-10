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

package org.apache.hudi.index;

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Looks up record-index keys that have already been grouped into one shard of a partitioned record index.
 */
public class PartitionedRecordIndexFileGroupLookupFunction
    implements PairFlatMapFunction<Iterator<Pair<String, String>>, String, HoodieRecordGlobalLocation> {

  private final HoodieTableMetadata metadataTable;

  public PartitionedRecordIndexFileGroupLookupFunction(HoodieTableMetadata metadataTable) {
    this.metadataTable = metadataTable;
  }

  @Override
  public Iterator<Tuple2<String, HoodieRecordGlobalLocation>> call(Iterator<Pair<String, String>> partitionPathRecordKeyIterator) {
    String partitionName = null;
    List<String> keysToLookup = new ArrayList<>();
    while (partitionPathRecordKeyIterator.hasNext()) {
      Pair<String, String> partitionPathRecordKey = partitionPathRecordKeyIterator.next();
      keysToLookup.add(partitionPathRecordKey.getRight());
      if (partitionName == null) {
        partitionName = partitionPathRecordKey.getLeft();
      }
    }

    if (keysToLookup.isEmpty()) {
      return Collections.emptyIterator();
    }

    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
        metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(keysToLookup), Option.of(partitionName));
    try {
      Map<String, HoodieRecordGlobalLocation> recordIndexInfo = recordIndexData.collectAsList().stream()
          .collect(HashMap::new, (map, pair) -> map.put(pair.getKey(), pair.getValue()), HashMap::putAll);
      return recordIndexInfo.entrySet().stream()
          .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
    } finally {
      recordIndexData.unpersistWithDependencies();
    }
  }
}
