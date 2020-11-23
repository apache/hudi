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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.HoodieIndex;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FlinkWriteHelper<T extends HoodieRecordPayload,R> extends AbstractWriteHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>, R> {

  private FlinkWriteHelper() {
  }

  private static class WriteHelperHolder {
    private static final FlinkWriteHelper FLINK_WRITE_HELPER = new FlinkWriteHelper();
  }

  public static FlinkWriteHelper newInstance() {
    return WriteHelperHolder.FLINK_WRITE_HELPER;
  }

  @Override
  public List<HoodieRecord<T>> deduplicateRecords(List<HoodieRecord<T>> records,
                                                     HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> index,
                                                     int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    Map<Object, List<Pair<Object, HoodieRecord<T>>>> keyedRecords = records.stream().map(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return Pair.of(key, record);
    }).collect(Collectors.groupingBy(Pair::getLeft));

    return keyedRecords.values().stream().map(x -> x.stream().map(Pair::getRight).reduce((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec1.getData().preCombine(rec2.getData());
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }).orElse(null)).filter(Objects::nonNull).collect(Collectors.toList());
  }
}
