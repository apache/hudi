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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

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
        if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ
            && table.getConfig().isSparkSqlMergeIntoReadingTarget()) {
          taggedRecords = mergeLocatedSqlMergeIntoRecords(taggedRecords, instantTime, context, table, configuredShuffleParallelism);
        }
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
    if (operationType != WriteOperationType.INSERT && !hasRecordKeyFields(metaClient.getTableConfig())
        && KeyGenUtils.isAutoGeneratedRecordKeysEnabled(config.getProps())) {
      // An insert locates no existing row, so only updates and deletes need the record key
      throw keylessTableNotSupported();
    }
  }

  /**
   * Fails a delta commit of the {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} write mode when a
   * compaction requested after the commit started covers a file group the commit wrote positional deletes into.
   * The plan of such a compaction misses the deletes, so the compacted base file keeps the deleted rows under a later
   * instant than their new copies, and the next update would locate the dead copy and leave the live one next to its
   * new version. It runs under the commit lock, so it holds whatever config the compaction scheduler has.
   */
  public static void validateNoCompactionRequestedAfterDeletes(HoodieTableMetaClient metaClient, String instantTime,
                                                               HoodieCommitMetadata metadata) {
    Set<HoodieFileGroupId> fileGroupsWithDeletes = new HashSet<>();
    metadata.getPartitionToWriteStats().forEach((partitionPath, stats) -> stats.stream()
        .filter(stat -> stat.getNumDeletes() > 0)
        .forEach(stat -> fileGroupsWithDeletes.add(new HoodieFileGroupId(partitionPath, stat.getFileId()))));
    if (fileGroupsWithDeletes.isEmpty()) {
      return;
    }
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Stream<Pair<String, Stream<HoodieFileGroupId>>> pendingCompactions = timeline.filterPendingCompactionTimeline()
        .filter(instant -> compareTimestamps(instant.requestedTime(), GREATER_THAN, instantTime))
        .getInstantsAsStream()
        .map(instant -> Pair.of(instant.requestedTime(), CompactionUtils.getCompactionPlan(metaClient, instant.requestedTime())
            .getOperations().stream().map(operation -> new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId()))));
    Stream<Pair<String, Stream<HoodieFileGroupId>>> completedCompactions = timeline.getCommitTimeline().filterCompletedInstants()
        .filter(instant -> HoodieTimeline.COMMIT_ACTION.equals(instant.getAction())
            && compareTimestamps(instant.requestedTime(), GREATER_THAN, instantTime))
        .getInstantsAsStream()
        .map(instant -> Pair.of(instant.requestedTime(), readCommitMetadata(timeline, instant)))
        .filter(compaction -> compaction.getRight().getOperationType() == WriteOperationType.COMPACT)
        .map(compaction -> Pair.of(compaction.getLeft(), compaction.getRight().getPartitionToWriteStats().entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(stat -> new HoodieFileGroupId(entry.getKey(), stat.getFileId())))));
    Stream.concat(pendingCompactions, completedCompactions).forEach(compaction -> compaction.getRight()
        .filter(fileGroupsWithDeletes::contains)
        .findFirst()
        .ifPresent(fileGroupId -> {
          throw new HoodieWriteConflictException("Delta commit " + instantTime + " wrote positional deletes into file group "
              + fileGroupId + ", which compaction " + compaction.getLeft() + " requested after the commit started "
              + "rewrites without them in " + HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " write mode");
        }));
  }

  private static HoodieCommitMetadata readCommitMetadata(HoodieActiveTimeline timeline, HoodieInstant instant) {
    try {
      return timeline.readCommitMetadata(instant);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read the commit metadata of " + instant, e);
    }
  }

  /**
   * Whether the table declares a primary key in {@link HoodieTableConfig#RECORDKEY_FIELDS}.
   */
  public static boolean hasRecordKeyFields(HoodieTableConfig tableConfig) {
    return tableConfig.getRecordKeyFields().map(fields -> fields.length > 0).orElse(false);
  }

  /**
   * The refusal of {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} for an update or delete of a table
   * without a primary key. The writer keys its rows with generated record keys, so an update or delete cannot
   * locate the current version of a row by its key and would leave it in place next to the new version.
   */
  public static HoodieNotSupportedException keylessTableNotSupported() {
    return new HoodieNotSupportedException(
        HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " requires a table with a primary key to update "
            + "or delete rows, but the table declares no " + HoodieTableConfig.RECORDKEY_FIELDS.key() + " and its rows "
            + "carry generated record keys, so an update or delete cannot locate the current version of a row and would "
            + "leave it next to the new one. Create the table with a primary key, or set "
            + HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() + " to false.");
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
