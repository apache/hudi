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
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

public abstract class BaseWriteHelper<T, I, K, O, R> extends ParallelismHelper<I> {
  protected boolean shouldCheckCustomDeleteMarker = false;
  protected boolean shouldCheckBuiltInDeleteMarker = false;

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
    HoodieReaderContext<T> readerContext = table.getContext().<T>getReaderContextFactory(table.getMetaClient()).getContext();
    Option<String> orderingFieldNameOpt = getOrderingFieldName(readerContext, table.getConfig().getProps(), table.getMetaClient());
    BufferedRecordMerger<T> recordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        table.getConfig().getRecordMergeMode(),
        false,
        Option.ofNullable(table.getConfig().getRecordMerger()),
        orderingFieldNameOpt,
        Option.ofNullable(table.getConfig().getPayloadClass()),
        new SerializableSchema(table.getConfig().getSchema()).get(),
        table.getConfig().getProps(),
        table.getMetaClient().getTableConfig().getPartialUpdateMode());
    this.shouldCheckCustomDeleteMarker = readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().isPresent();
    this.shouldCheckBuiltInDeleteMarker = readerContext.getSchemaHandler().hasBuiltInDelete();
    return deduplicateRecords(
        records,
        table.getIndex(),
        parallelism,
        table.getConfig().getSchema(),
        table.getConfig().getProps(),
        recordMerger,
        readerContext,
        orderingFieldNameOpt);
  }

  public abstract I deduplicateRecords(I records,
                                       HoodieIndex<?, ?> index,
                                       int parallelism,
                                       String schema,
                                       TypedProperties props,
                                       BufferedRecordMerger<T> merger,
                                       HoodieReaderContext<T> readerContext,
                                       Option<String> orderingFieldNameOpt);

  public static Option<String> getOrderingFieldName(HoodieReaderContext readerContext,
                                                    TypedProperties props,
                                                    HoodieTableMetaClient metaClient) {
    return readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Option.empty()
        : Option.ofNullable(ConfigUtils.getOrderingField(props))
        .or(() -> {
          String preCombineField = metaClient.getTableConfig().getPreCombineField();
          if (StringUtils.isNullOrEmpty(preCombineField)) {
            return Option.empty();
          }
          return Option.of(preCombineField);
        });
  }

  /**
   * Check if the value of column "_hoodie_is_deleted" is true.
   */
  public static <T> boolean isBuiltInDeleteRecord(T record,
                                                  HoodieReaderContext<T> readerContext,
                                                  Schema schema) {
    if (!readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().isPresent()) {
      return false;
    }
    Object columnValue = readerContext.getValue(record, schema, HOODIE_IS_DELETED_FIELD);
    return columnValue != null && readerContext.getTypeConverter().castToBoolean(columnValue);
  }

  /**
   * Check if a record is a DELETE marked by the '_hoodie_operation' field.
   */
  public static <T> boolean isDeleteHoodieOperation(T record,
                                                    HoodieReaderContext<T> readerContext) {
    int hoodieOperationPos = readerContext.getSchemaHandler().getHoodieOperationPos();
    if (hoodieOperationPos < 0) {
      return false;
    }
    String hoodieOperation = readerContext.getMetaFieldValue(record, hoodieOperationPos);
    return hoodieOperation != null && HoodieOperation.isDeleteRecord(hoodieOperation);
  }

  /**
   * Check if a record is a DELETE marked by a custom delete marker.
   */
  public static <T> boolean isCustomDeleteRecord(T record,
                                                 HoodieReaderContext<T> readerContext,
                                                 Schema schema) {
    if (!readerContext.getSchemaHandler().hasBuiltInDelete()) {
      return false;
    }
    Pair<String, String> markerKeyValue =
        readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().get();
    Object deleteMarkerValue =
        readerContext.getValue(record, schema, markerKeyValue.getLeft());
    return deleteMarkerValue != null
        && markerKeyValue.getRight().equals(deleteMarkerValue.toString());
  }

  public static <T> Option<BufferedRecord<T>> merge(HoodieRecord<T> newRecord,
                                                    HoodieRecord<T> oldRecord,
                                                    Schema newSchema,
                                                    Schema oldSchema,
                                                    HoodieReaderContext<T> readerContext,
                                                    Option<String> orderingFieldNameOpt,
                                                    BufferedRecordMerger<T> recordMerger) throws IOException {
    // Construct new buffered record.
    boolean isDelete1 = isBuiltInDeleteRecord(newRecord.getData(), readerContext, newSchema)
        || isCustomDeleteRecord(newRecord.getData(), readerContext, newSchema)
        || isDeleteHoodieOperation(newRecord.getData(), readerContext);
    BufferedRecord<T> bufferedRec1 = BufferedRecord.forRecordWithContext(
        newRecord.getData(), newSchema, readerContext, orderingFieldNameOpt, isDelete1);
    // Construct old buffered record.
    boolean isDelete2 = isBuiltInDeleteRecord(oldRecord.getData(), readerContext, oldSchema)
        || isCustomDeleteRecord(oldRecord.getData(), readerContext, oldSchema)
        || isDeleteHoodieOperation(oldRecord.getData(), readerContext);
    BufferedRecord<T> bufferedRec2 = BufferedRecord.forRecordWithContext(
        oldRecord.getData(), oldSchema, readerContext, orderingFieldNameOpt, isDelete2);
    // Run merge.
    return recordMerger.deltaMerge(bufferedRec1, bufferedRec2);
  }
}
