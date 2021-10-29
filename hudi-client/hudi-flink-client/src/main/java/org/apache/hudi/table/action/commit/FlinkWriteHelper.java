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
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Overrides the {@link #write} method to not look up index and partition the records, because
 * with {@code org.apache.hudi.operator.partitioner.BucketAssigner}, each hoodie record
 * is tagged with a bucket ID (partition path + fileID) in streaming way. The FlinkWriteHelper only hands over
 * the records to the action executor {@link BaseCommitActionExecutor} to execute.
 *
 * <p>Computing the records batch locations all at a time is a pressure to the engine,
 * we should avoid that in streaming system.
 */
public class FlinkWriteHelper<T extends HoodieRecordPayload, R> extends AbstractWriteHelper<T, List<HoodieRecord<T>>,
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
  public HoodieWriteMetadata<List<WriteStatus>> write(String instantTime, List<HoodieRecord<T>> inputRecords, HoodieEngineContext context,
                                                      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table, boolean shouldCombine, int shuffleParallelism,
                                                      BaseCommitActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, R> executor, boolean performTagging) {
    try {
      Instant lookupBegin = Instant.now();
      Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());

      HoodieWriteMetadata<List<WriteStatus>> result = executor.execute(inputRecords);
      result.setIndexLookupDuration(indexLookupDuration);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  @Override
  protected List<HoodieRecord<T>> tag(List<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context, HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table) {
    return HoodieList.getList(
        table.getIndex().tagLocation(HoodieList.of(dedupedRecords), context, table));
  }

  @Override
  public List<HoodieRecord<T>> deduplicateRecords(
      List<HoodieRecord<T>> records, HoodieIndex<T, ?, ?, ?> index, int parallelism) {
    Map<Object, List<Pair<Object, HoodieRecord<T>>>> keyedRecords = records.stream().map(record -> {
      // If index used is global, then records are expected to differ in their partitionPath
      final Object key = record.getKey().getRecordKey();
      return Pair.of(key, record);
    }).collect(Collectors.groupingBy(Pair::getLeft));

    return keyedRecords.values().stream().map(x -> x.stream().map(Pair::getRight).reduce((rec1, rec2) -> {
      final T data1 = rec1.getData();
      final T data2 = rec2.getData();

      @SuppressWarnings("unchecked") final T reducedData = (T) data2.preCombine(data1);
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      boolean choosePrev = data1.equals(reducedData);
      HoodieKey reducedKey = choosePrev ? rec1.getKey() : rec2.getKey();
      HoodieOperation operation = choosePrev ? rec1.getOperation() : rec2.getOperation();
      HoodieRecord<T> hoodieRecord = new HoodieRecord<>(reducedKey, reducedData, operation);
      // reuse the location from the first record.
      hoodieRecord.setCurrentLocation(rec1.getCurrentLocation());
      return hoodieRecord;
    }).orElse(null)).filter(Objects::nonNull).collect(Collectors.toList());
  }
}
