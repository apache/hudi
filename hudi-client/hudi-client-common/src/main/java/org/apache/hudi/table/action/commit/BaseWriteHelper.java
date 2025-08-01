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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;

public abstract class BaseWriteHelper<T, I, K, O, R> extends ParallelismHelper<I> {

  protected BaseWriteHelper(SerializableFunctionUnchecked<I, Integer> partitionNumberExtractor) {
    super(partitionNumberExtractor);
  }

  public HoodieWriteMetadata<O> write(String instantTime,
                                      I inputRecords,
                                      HoodieEngineContext context,
                                      HoodieTable<T, I, K, O> table,
                                      boolean shouldCombine,
                                      int configuredShuffleParallelism,
                                      BaseCommitActionExecutor<T, I, K, O, R> executor,
                                      WriteOperationType operationType) {
    try {
      HoodieTimer sourceReadAndIndexTimer = HoodieTimer.start();
      // De-dupe/merge if needed
      I dedupedRecords =
          combineOnCondition(shouldCombine, inputRecords, configuredShuffleParallelism, table);

      I taggedRecords = dedupedRecords;
      if (table.getIndex().requiresTagging(operationType)) {
        // perform index loop up to get existing location of records
        context.setJobStatus(this.getClass().getSimpleName(), "Tagging: " + table.getConfig().getTableName());
        taggedRecords = tag(dedupedRecords, context, table);
      }

      HoodieWriteMetadata<O> result = executor.execute(taggedRecords, Option.of(sourceReadAndIndexTimer));
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  protected abstract I tag(
      I dedupedRecords, HoodieEngineContext context, HoodieTable<T, I, K, O> table);

  public I combineOnCondition(
      boolean condition, I records, int configuredParallelism, HoodieTable<T, I, K, O> table) {
    int targetParallelism = deduceShuffleParallelism(records, configuredParallelism);
    return condition ? deduplicateRecords(records, table, targetParallelism) : records;
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records     hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return Collection of HoodieRecord already be deduplicated
   */
  public I deduplicateRecords(I records, HoodieTable<T, I, K, O> table, int parallelism) {
    HoodieReaderContext<T> readerContext =
        (HoodieReaderContext<T>) table.getContext().<T>getReaderContextFactoryDuringWrite(table.getMetaClient(), table.getConfig().getRecordMerger().getRecordType(), table.getConfig().getProps())
            .getContext();
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    readerContext.initRecordMerger(table.getConfig().getProps());
    List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode(), table.getConfig().getProps(), table.getMetaClient());
    HoodieRecordMerger recordMerger = HoodieRecordUtils.mergerToPreCombineMode(table.getConfig().getRecordMerger());
    RecordMergeMode recordMergeMode = HoodieTableConfig.inferCorrectMergingBehavior(null, table.getConfig().getPayloadClass(), null,
        String.join(",", orderingFieldNames), tableConfig.getTableVersion()).getLeft();
    BufferedRecordMerger<T> bufferedRecordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        recordMergeMode,
        false,
        Option.ofNullable(recordMerger),
        orderingFieldNames,
        Option.ofNullable(table.getConfig().getPayloadClass()),
        new SerializableSchema(table.getConfig().getSchema()).get(),
        table.getConfig().getProps(),
        tableConfig.getPartialUpdateMode());
    return deduplicateRecords(
        records,
        table.getIndex(),
        parallelism,
        table.getConfig().getSchema(),
        table.getConfig().getProps(),
        bufferedRecordMerger,
        readerContext,
        orderingFieldNames.toArray(new String[0]));
  }

  public abstract I deduplicateRecords(I records,
                                       HoodieIndex<?, ?> index,
                                       int parallelism,
                                       String schema,
                                       TypedProperties props,
                                       BufferedRecordMerger<T> merger,
                                       HoodieReaderContext<T> readerContext,
                                       String[] orderingFieldNames);

  public static <T> Option<BufferedRecord<T>> merge(HoodieRecord<T> newRecord,
                                                    HoodieRecord<T> oldRecord,
                                                    Schema newSchema,
                                                    Schema oldSchema,
                                                    RecordContext<T> recordContext,
                                                    String[] orderingFieldNames,
                                                    BufferedRecordMerger<T> recordMerger,
                                                    TypedProperties properties) throws IOException {
    // Construct new buffered record.
    BufferedRecord<T> bufferedRec1 = BufferedRecord.forRecordWithContext(newRecord, newSchema, recordContext, properties, orderingFieldNames);
    // Construct old buffered record.
    BufferedRecord<T> bufferedRec2 = BufferedRecord.forRecordWithContext(oldRecord, oldSchema, recordContext, properties, orderingFieldNames);
    // Run merge.
    return recordMerger.deltaMerge(bufferedRec1, bufferedRec2);
  }
}
