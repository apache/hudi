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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.function.SerializableBiFunction;
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
import org.apache.hudi.exception.HoodieMultipleMatchesException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
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
      // Records matching the same existing record are only told apart after tagging, so they are combined then
      boolean failOnMultipleMatches = shouldFailOnMultipleMatches(table, operationType);
      // De-dupe/merge if needed
      I dedupedRecords =
          combineOnCondition(shouldCombine && !failOnMultipleMatches, inputRecords, configuredShuffleParallelism, table);

      I taggedRecords = dedupedRecords;
      if (table.getIndex().requiresTagging(operationType)) {
        // perform index loop up to get existing location of records
        context.setJobStatus(this.getClass().getSimpleName(), "Tagging: " + table.getConfig().getTableName());
        taggedRecords = tag(dedupedRecords, context, table);
        if (failOnMultipleMatches) {
          taggedRecords = failOnMultipleMatches(taggedRecords, shouldCombine, table);
        }
        if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ
            && table.getConfig().isSparkSqlMergeIntoReadingTarget()) {
          taggedRecords = mergeLocatedSqlMergeIntoRecords(taggedRecords, instantTime, context, table, configuredShuffleParallelism);
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

  /**
   * Replaces each located record of a Spark SQL MERGE INTO with what its matched clauses make of the record it
   * matches, for a write that appends updates to log files, which evaluates the clauses of a located record
   * without that record. See {@link HoodieWriteConfig#SPARK_SQL_MERGE_INTO_READS_TARGET_KEY}.
   */
  protected I mergeLocatedSqlMergeIntoRecords(I taggedRecords, String instantTime, HoodieEngineContext context,
                                              HoodieTable<T, I, K, O> table, int configuredShuffleParallelism) {
    throw new HoodieNotSupportedException(
        HoodieWriteConfig.SPARK_SQL_MERGE_INTO_READS_TARGET_KEY + " is not supported by " + this.getClass().getName());
  }

  private static boolean shouldFailOnMultipleMatches(HoodieTable<?, ?, ?, ?> table, WriteOperationType operationType) {
    // A bucket index locates every record of an existing bucket, including one with a new key
    return operationType == WriteOperationType.UPSERT
        && table.getConfig().shouldFailUpsertOnMultipleMatches()
        && table.getIndex().requiresTagging(operationType)
        && !(table.getIndex() instanceof HoodieBucketIndex);
  }

  /**
   * Fails with a {@link HoodieMultipleMatchesException} when more than one tagged record matches the same existing
   * record, that is, when two records with the same key are located at the same row of a file group. Combines the
   * records of a key the table does not hold yet when {@code combineNewKeys} is set. See
   * {@link HoodieWriteConfig#UPSERT_FAIL_ON_MULTIPLE_MATCHES}.
   */
  protected I failOnMultipleMatches(I taggedRecords, boolean combineNewKeys, HoodieTable<T, I, K, O> table) {
    throw new HoodieNotSupportedException(
        HoodieWriteConfig.UPSERT_FAIL_ON_MULTIPLE_MATCHES.key() + " is not supported by " + this.getClass().getName());
  }

  /**
   * The exception for more than one incoming record matching the existing record with the given key and location.
   */
  public static HoodieMultipleMatchesException multipleMatchesException(String recordKey, String partitionPath, String fileId) {
    return new HoodieMultipleMatchesException(recordKey, "More than one incoming record matches the existing record with key "
        + recordKey + " in partition " + partitionPath + " and file group " + fileId
        + ", so the upsert cannot apply them as one update. Set " + HoodieWriteConfig.UPSERT_FAIL_ON_MULTIPLE_MATCHES.key()
        + " to false to combine them instead.");
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
    RecordMergerSetup<T> setup = new RecordMergerSetup<>(table);
    return deduplicateRecords(
        records,
        table.getIndex(),
        parallelism,
        table.getConfig().getSchema(),
        setup.props,
        setup.merger,
        setup.readerContext,
        setup.orderingFieldNames);
  }

  /**
   * The function {@link #deduplicateRecords} combines two records with the same key with.
   */
  protected static <T> SerializableBiFunction<HoodieRecord<T>, HoodieRecord<T>, HoodieRecord<T>> recordCombiner(
      HoodieTable<T, ?, ?, ?> table) {
    RecordMergerSetup<T> setup = new RecordMergerSetup<>(table);
    TypedProperties props = setup.props;
    BufferedRecordMerger<T> merger = setup.merger;
    String[] orderingFieldNames = setup.orderingFieldNames;
    HoodieSchema schema = HoodieSchema.parse(table.getConfig().getSchema());
    RecordContext<T> recordContext = setup.readerContext.getRecordContext();
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema);
    return (previous, next) -> reduceRecords(props, merger, orderingFieldNames, previous, next, schema, recordContext, deleteContext);
  }

  /**
   * The record merger of the table for combining incoming records with the same key, and its inputs.
   */
  private static class RecordMergerSetup<T> {
    private final HoodieReaderContext<T> readerContext;
    private final TypedProperties props;
    private final BufferedRecordMerger<T> merger;
    private final String[] orderingFieldNames;

    private RecordMergerSetup(HoodieTable<T, ?, ?, ?> table) {
      this.readerContext =
          (HoodieReaderContext<T>) table.getContext().<T>getReaderContextFactoryForWrite(table.getMetaClient(), table.getConfig().getRecordMerger().getRecordType(), table.getConfig().getProps())
              .getContext();
      HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
      readerContext.initRecordMergerForIngestion(table.getConfig().getProps());
      List<String> orderingFields = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode(), table.getMetaClient());
      HoodieSchema recordSchema;
      if (StringUtils.nonEmpty(table.getConfig().getPartialUpdateSchema())) {
        recordSchema = HoodieSchema.parse(table.getConfig().getPartialUpdateSchema());
      } else {
        recordSchema = HoodieSchema.parse(table.getConfig().getWriteSchema());
      }
      recordSchema = HoodieSchemaCache.intern(recordSchema);
      this.props = readerContext.getMergeProps(table.getConfig().getProps());
      this.merger = BufferedRecordMergerFactory.create(
          readerContext,
          readerContext.getMergeMode(),
          false,
          readerContext.getRecordMerger().map(HoodieRecordUtils::mergerToPreCombineMode),
          Option.ofNullable(table.getConfig().getPayloadClass()),
          recordSchema,
          props,
          tableConfig.getPartialUpdateMode());
      this.orderingFieldNames = orderingFields.toArray(new String[0]);
    }
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
