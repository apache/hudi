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
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMultipleMatchesException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.io.IOException;
import java.util.EnumSet;
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
        if (shouldWriteUpdatesAsDeletesAndInserts(table, operationType)) {
          taggedRecords = updatesAsDeletesAndInserts(taggedRecords, table, configuredShuffleParallelism);
        } else if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ
            && table.getConfig().isSparkSqlMergeIntoReadingTarget()) {
          taggedRecords = mergeLocatedSqlMergeIntoRecords(taggedRecords, table, configuredShuffleParallelism);
        }
        taggedRecords = resolveReplacedRecords(taggedRecords, table);
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

  /**
   * Index types that tag a record with its row position in the base file and write merge-on-read
   * inserts to base files, which the update-as-delete-insert write mode needs to keep log files to
   * positional deletes. The write reads the position of a record the index locates without one from
   * the base file of the located file group, see {@link HoodieIndexUtils#tagMissingRecordPositionsIfNeeded}.
   */
  private static final EnumSet<HoodieIndex.IndexType> POSITION_PRODUCING_INDEX_TYPES = EnumSet.of(
      HoodieIndex.IndexType.BLOOM, HoodieIndex.IndexType.SIMPLE,
      HoodieIndex.IndexType.GLOBAL_BLOOM, HoodieIndex.IndexType.GLOBAL_SIMPLE,
      HoodieIndex.IndexType.RECORD_INDEX, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX);

  protected abstract I tag(
      I dedupedRecords, HoodieEngineContext context, HoodieTable<T, I, K, O> table);

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

  private boolean shouldWriteUpdatesAsDeletesAndInserts(HoodieTable<T, I, K, O> table, WriteOperationType operationType) {
    if (operationType != WriteOperationType.UPSERT
        || table.getMetaClient().getTableType() != HoodieTableType.MERGE_ON_READ
        || !table.getConfig().shouldWriteUpdatesAsDeletesAndInserts()) {
      return false;
    }
    validateUpdatesAsDeletesAndInserts(table.getConfig(), table.getMetaClient(), operationType);
    return true;
  }

  /**
   * Validates the requirements of {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} for a
   * write that can append to the log files of a merge-on-read table.
   */
  public static void validateUpdatesAsDeletesAndInserts(HoodieWriteConfig config, HoodieTableMetaClient metaClient,
                                                        WriteOperationType operationType) {
    HoodieTableVersion writeVersion = config.getWriteVersion();
    if (writeVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      // Log files carry record positions only from write table version 8 on
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()
              + " requires write table version 8 or higher to write record positions but the write table version is "
              + writeVersion.versionCode());
    }
    RecordMergeMode mergeMode = metaClient.getTableConfig().getRecordMergeMode();
    if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING) {
      // The decomposed delete unconditionally tombstones the current version, so a late-arriving
      // update with a lower ordering value would incorrectly win under event-time ordering
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()
              + " requires commit-time ordering merge semantics but the table uses " + mergeMode);
    }
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      // A custom index class takes precedence over the index type checked below
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " does not support a custom index class but "
              + HoodieIndexConfig.INDEX_CLASS_NAME.key() + " is " + config.getIndexClass());
    }
    HoodieIndex.IndexType indexType = config.getIndexType();
    if (!POSITION_PRODUCING_INDEX_TYPES.contains(indexType)) {
      // Without a base file position a delete cannot be represented as a positional delete, and an
      // index that routes by key hash or can index log files appends inserts to logs
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()
              + " requires an index that produces record positions " + POSITION_PRODUCING_INDEX_TYPES
              + " but the index type is " + indexType);
    }
    if (!config.shouldWriteRecordPositions()) {
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()
              + " requires " + HoodieWriteConfig.WRITE_RECORD_POSITIONS.key() + " to be enabled");
    }
    HoodieFileFormat baseFileFormat = metaClient.getTableConfig().getBaseFileFormat();
    if (baseFileFormat == HoodieFileFormat.HFILE) {
      // A positional delete needs the row position of each record in its base file, which HFile does not provide
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " does not support the base file format "
              + baseFileFormat);
    }
    if (config.shouldWritePartialUpdates()) {
      // The new version of an updated record is inserted into a base file, so it has to carry every column
      throw new HoodieNotSupportedException(
          HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " does not support partial updates but "
              + HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key() + " is set");
    }
    if ((operationType == WriteOperationType.UPSERT || operationType == WriteOperationType.DELETE)
        && !hasRecordKeyFields(metaClient.getTableConfig())
        && KeyGenUtils.isAutoGeneratedRecordKeysEnabled(config.getProps())
        && !config.getProps().getBoolean(HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY, false)) {
      // An insert locates no existing row, and a prepped write or a Spark SQL MERGE INTO brings the record key and
      // the location of each row it read, so only an upsert or delete that looks rows up needs the record key
      throw keylessTableNotSupported();
    }
  }

  /**
   * Whether the table declares a primary key in {@link HoodieTableConfig#RECORDKEY_FIELDS}.
   */
  public static boolean hasRecordKeyFields(HoodieTableConfig tableConfig) {
    return tableConfig.getRecordKeyFields().map(fields -> fields.length > 0).orElse(false);
  }

  /**
   * The refusal of {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} for a write that updates or deletes
   * rows of a table without a primary key other than through Spark SQL UPDATE, DELETE and MERGE INTO, which write
   * each row they read once, with its location. The writer keys the rows with generated record keys, so an upsert
   * or delete by record key cannot locate the current version of a row and would leave it in place next to the new
   * version.
   */
  public static HoodieNotSupportedException keylessTableNotSupported() {
    return new HoodieNotSupportedException(
        HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " requires a table with a primary key to upsert "
            + "or delete rows by record key, but the table declares no " + HoodieTableConfig.RECORDKEY_FIELDS.key()
            + " and its rows carry generated record keys, so the write cannot locate the current version of a row and "
            + "would leave it next to the new one. Use Spark SQL UPDATE, DELETE or MERGE INTO, create the table with a "
            + "primary key, or set " + HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " to false.");
  }

  /**
   * Resolves the values of the format meta columns for the records that records of the write replace in other file
   * groups, see {@link HoodieIndexUtils#resolveReplacedRecords}. Returns the records as they are for an engine that
   * does not move records to other file groups.
   */
  protected I resolveReplacedRecords(I taggedRecords, HoodieTable<T, I, K, O> table) {
    return taggedRecords;
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
   * Rewrites each located record that is not a delete into a positional delete to its location plus an untagged
   * insert of the record, see {@link #updatesAsDeletesAndInserts}.
   */
  protected I splitLocatedRecords(I locatedRecords, HoodieTable<T, I, K, O> table) {
    throw new HoodieNotSupportedException(
        HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " is not supported by " + this.getClass().getName());
  }

  /**
   * Splits the updates of a prepped upsert the way {@link #write} splits the updates the index locates, see
   * {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS}. The caller located each prepped record at the
   * row it read, so the records are not reduced to one per record key first.
   */
  public I preppedUpdatesAsDeletesAndInserts(I preppedRecords, HoodieTable<T, I, K, O> table) {
    return resolveReplacedRecords(splitLocatedRecords(preppedRecords, table), table);
  }

  /**
   * Replaces each located record of a Spark SQL MERGE INTO with what its matched clauses make of the record it
   * matches, for a write that appends updates to log files, which evaluates the clauses of a located record
   * without that record. See {@link HoodieWriteConfig#SPARK_SQL_MERGE_INTO_READS_TARGET_KEY}.
   */
  protected I mergeLocatedSqlMergeIntoRecords(I taggedRecords, HoodieTable<T, I, K, O> table, int configuredShuffleParallelism) {
    throw new HoodieNotSupportedException(
        HoodieWriteConfig.SPARK_SQL_MERGE_INTO_READS_TARGET_KEY + " is not supported by " + this.getClass().getName());
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
      HoodieRecord<T> chosenRecord = merged.isEmpty() ? previous : next;
      HoodieRecord<T> result = reducedRecord.newInstance(chosenRecord.getKey());
      if (chosenRecord.isCurrentLocationKnown()) {
        // A record the writer located before the write, as a Spark SQL MERGE INTO does on a table without a record
        // key, shares its record key and location with the records it is combined with
        result.setCurrentLocation(chosenRecord.getCurrentLocation());
      }
      return result;
    } catch (IOException e) {
      throw new HoodieException(String.format("Error to merge two records, %s, %s", previous, next), e);
    }
  }
}
