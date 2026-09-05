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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

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
        if (shouldWriteUpdatesAsDeletesAndInserts(table, operationType)) {
          taggedRecords = updatesAsDeletesAndInserts(taggedRecords, table, configuredShuffleParallelism);
        }
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

  private boolean shouldWriteUpdatesAsDeletesAndInserts(HoodieTable<T, I, K, O> table, WriteOperationType operationType) {
    if (operationType != WriteOperationType.UPSERT
        || table.getMetaClient().getTableType() != HoodieTableType.MERGE_ON_READ
        || !table.getConfig().shouldWriteUpdatesAsDeletesAndInserts()) {
      return false;
    }
    RecordMergeMode mergeMode = table.getMetaClient().getTableConfig().getRecordMergeMode();
    if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING) {
      // The decomposed delete unconditionally tombstones the current version, so a late-arriving
      // update with a lower ordering value would incorrectly win under event-time ordering
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()
              + " requires commit-time ordering merge semantics but the table uses " + mergeMode);
    }
    return true;
  }

  /**
   * Rewrites each tagged update into a positional delete to the record's current file group plus an
   * untagged insert of the new version, so the insert partitioner routes the new version to a file
   * group chosen for inserts. See {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS}.
   */
  protected I updatesAsDeletesAndInserts(I taggedRecords, HoodieTable<T, I, K, O> table, int configuredShuffleParallelism) {
    throw new HoodieNotSupportedException(
        HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " is not supported by " + this.getClass().getName());
  }

  /**
   * Picks the copy of a multi-tagged record whose location is the latest. Under this write mode a
   * key moves file groups on every update and older file groups keep a tombstoned physical copy,
   * so an index that scans base files can tag the incoming record once per copy; only the latest
   * location is live.
   */
  protected static <T> HoodieRecord<T> latestLocation(HoodieRecord<T> left, HoodieRecord<T> right) {
    String leftInstant =
        left.isCurrentLocationKnown() ? left.getCurrentLocation().getInstantTime() : "";
    String rightInstant =
        right.isCurrentLocationKnown() ? right.getCurrentLocation().getInstantTime() : "";
    return leftInstant.compareTo(rightInstant) >= 0 ? left : right;
  }

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
        (HoodieReaderContext<T>) table.getContext().<T>getReaderContextFactoryForWrite(table.getMetaClient(), table.getConfig().getRecordMerger().getRecordType(), table.getConfig().getProps())
            .getContext();
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    readerContext.initRecordMergerForIngestion(table.getConfig().getProps());
    List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode(), table.getMetaClient());
    HoodieSchema recordSchema;
    if (StringUtils.nonEmpty(table.getConfig().getPartialUpdateSchema())) {
      recordSchema = HoodieSchema.parse(table.getConfig().getPartialUpdateSchema());
    } else {
      recordSchema = HoodieSchema.parse(table.getConfig().getWriteSchema());
    }
    recordSchema = HoodieSchemaCache.intern(recordSchema);
    TypedProperties mergedProperties = readerContext.getMergeProps(table.getConfig().getProps());
    BufferedRecordMerger<T> bufferedRecordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        readerContext.getMergeMode(),
        false,
        readerContext.getRecordMerger().map(HoodieRecordUtils::mergerToPreCombineMode),
        Option.ofNullable(table.getConfig().getPayloadClass()),
        recordSchema,
        mergedProperties,
        tableConfig.getPartialUpdateMode());
    return deduplicateRecords(
        records,
        table.getIndex(),
        parallelism,
        table.getConfig().getSchema(),
        mergedProperties,
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

  protected static <T> HoodieRecord<T> reduceRecords(TypedProperties props, BufferedRecordMerger<T> recordMerger, String[] orderingFieldNames,
                                                     HoodieRecord<T> previous, HoodieRecord<T> next, HoodieSchema schema, RecordContext<T> recordContext, DeleteContext deleteContext) {
    try {
      // NOTE: The order of previous and next is uncertain within a batch in "reduceByKey".
      // If the return value is empty, it means the previous should be chosen.
      BufferedRecord<T> newBufferedRecord = BufferedRecords.fromHoodieRecord(next, schema, recordContext, props, orderingFieldNames, deleteContext);
      // Construct old buffered record.
      BufferedRecord<T> oldBufferedRecord = BufferedRecords.fromHoodieRecord(previous, schema, recordContext, props, orderingFieldNames, deleteContext);
      // Run merge.
      Option<BufferedRecord<T>> merged = recordMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
      // NOTE: For merge mode based merging, it returns non-null.
      //       For mergers / payloads based merging, it may return null.
      HoodieRecord<T> reducedRecord = merged.map(bufferedRecord -> recordContext.constructHoodieRecord(bufferedRecord, next.getPartitionPath())).orElse(previous);
      boolean choosePrevious = merged.isEmpty();
      HoodieKey reducedKey = choosePrevious ? previous.getKey() : next.getKey();
      return reducedRecord.newInstance(reducedKey);
    } catch (IOException e) {
      throw new HoodieException(String.format("Error to merge two records, %s, %s", previous, next), e);
    }
  }
}
