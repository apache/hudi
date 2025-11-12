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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;

import java.time.Duration;
import java.util.Iterator;
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
public class FlinkWriteHelper<T, R> extends BaseWriteHelper<T, Iterator<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>, R> {

  private FlinkWriteHelper() {
    super(ignored -> -1);
  }

  private static class WriteHelperHolder {
    private static final FlinkWriteHelper FLINK_WRITE_HELPER = new FlinkWriteHelper();
  }

  public static FlinkWriteHelper newInstance() {
    return WriteHelperHolder.FLINK_WRITE_HELPER;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> write(String instantTime, Iterator<HoodieRecord<T>> inputRecords, HoodieEngineContext context,
                                                      HoodieTable<T, Iterator<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table, boolean shouldCombine, int configuredShuffleParallelism,
                                                      BaseCommitActionExecutor<T, Iterator<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, R> executor, WriteOperationType operationType) {
    try {
      HoodieWriteMetadata<List<WriteStatus>> result = executor.execute(inputRecords);
      result.setIndexLookupDuration(Duration.ZERO);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  @Override
  protected Iterator<HoodieRecord<T>> tag(Iterator<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context, HoodieTable<T, Iterator<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table) {
    return table.getIndex()
        .tagLocation(HoodieListData.eager(CollectionUtils.toList(dedupedRecords)), context, table)
        .collectAsList().iterator();
  }

  @Override
  public Iterator<HoodieRecord<T>> deduplicateRecords(Iterator<HoodieRecord<T>> records,
                                                      HoodieIndex<?, ?> index,
                                                      int parallelism,
                                                      String schemaStr,
                                                      TypedProperties props,
                                                      BufferedRecordMerger<T> recordMerger,
                                                      HoodieReaderContext<T> readerContext,
                                                      String[] orderingFieldNames) {
    // If index used is global, then records are expected to differ in their partitionPath
    Map<Object, List<HoodieRecord<T>>> keyedRecords = CollectionUtils.toStream(records)
        .collect(Collectors.groupingBy(record -> record.getKey().getRecordKey()));

    // caution that the avro schema is not serializable
    final Schema schema = new Schema.Parser().parse(schemaStr);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema);
    return keyedRecords.values().stream().map(x -> x.stream().reduce((previous, next) ->
      reduceRecords(props, recordMerger, orderingFieldNames, previous, next, schema, readerContext.getRecordContext(), deleteContext)
    ).orElse(null)).filter(Objects::nonNull).iterator();
  }
}
