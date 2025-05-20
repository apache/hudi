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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

public abstract class FileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final Option<String> orderingFieldName;
  protected final RecordMergeMode recordMergeMode;
  protected final Option<HoodieRecordMerger> recordMerger;
  protected final Option<String> payloadClass;
  protected final TypedProperties props;
  protected final ExternalSpillableMap<Serializable, BufferedRecord<T>> records;
  protected final HoodieReadStats readStats;
  protected final boolean shouldCheckCustomDeleteMarker;
  protected final boolean shouldCheckBuiltInDeleteMarker;
  protected ClosableIterator<T> baseFileIterator;
  protected Iterator<BufferedRecord<T>> logRecordIterator;
  protected T nextRecord;
  protected boolean enablePartialMerging = false;
  protected InternalSchema internalSchema;
  protected HoodieTableMetaClient hoodieTableMetaClient;
  private long totalLogRecords = 0;

  protected FileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                  HoodieTableMetaClient hoodieTableMetaClient,
                                  RecordMergeMode recordMergeMode,
                                  TypedProperties props,
                                  HoodieReadStats readStats,
                                  Option<String> orderingFieldName) {
    this.readerContext = readerContext;
    this.readerSchema = AvroSchemaCache.intern(readerContext.getSchemaHandler().getRequiredSchema());
    this.recordMergeMode = recordMergeMode;
    this.recordMerger = readerContext.getRecordMerger();
    if (recordMerger.isPresent() && recordMerger.get().getMergingStrategy().equals(PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      this.payloadClass = Option.of(hoodieTableMetaClient.getTableConfig().getPayloadClass());
    } else {
      this.payloadClass = Option.empty();
    }
    this.orderingFieldName = orderingFieldName;
    this.props = props;
    this.internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    this.hoodieTableMetaClient = hoodieTableMetaClient;
    long maxMemorySizeInBytes = props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
    String spillableMapBasePath = props.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath());
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(props.getString(SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
    boolean isBitCaskDiskMapCompressionEnabled = props.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    this.readStats = readStats;
    try {
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator<>(),
          new HoodieRecordSizeEstimator<>(readerSchema), diskMapType, new DefaultSerializer<>(), isBitCaskDiskMapCompressionEnabled, getClass().getSimpleName());
    } catch (IOException e) {
      throw new HoodieIOException("IOException when creating ExternalSpillableMap at " + spillableMapBasePath, e);
    }
    this.shouldCheckCustomDeleteMarker =
        readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().isPresent();
    this.shouldCheckBuiltInDeleteMarker =
        readerContext.getSchemaHandler().hasBuiltInDelete();
  }

  /**
   * Here we assume that delete marker column type is of string.
   * This should be sufficient for most cases.
   */
  protected final boolean isCustomDeleteRecord(T record) {
    if (!shouldCheckCustomDeleteMarker) {
      return false;
    }

    Pair<String, String> markerKeyValue =
        readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().get();
    Object deleteMarkerValue =
        readerContext.getValue(record, readerSchema, markerKeyValue.getLeft());
    return deleteMarkerValue != null
        && markerKeyValue.getRight().equals(deleteMarkerValue.toString());
  }

  /**
   * Check if the value of column "_hoodie_is_deleted" is true.
   */
  protected final boolean isBuiltInDeleteRecord(T record) {
    if (!shouldCheckBuiltInDeleteMarker) {
      return false;
    }

    Object columnValue = readerContext.getValue(
        record, readerSchema, HOODIE_IS_DELETED_FIELD);
    return columnValue != null && readerContext.castToBoolean(columnValue);
  }

  @Override
  public void setBaseFileIterator(ClosableIterator<T> baseFileIterator) {
    this.baseFileIterator = baseFileIterator;
  }

  /**
   * This allows hasNext() to be called multiple times without incrementing the iterator by more than 1
   * record. It does come with the caveat that hasNext() must be called every time before next(). But
   * that is pretty expected behavior and every user should be doing that anyway.
   */
  protected abstract boolean doHasNext() throws IOException;

  @Override
  public final boolean hasNext() throws IOException {
    return nextRecord != null || doHasNext();
  }

  @Override
  public final T next() {
    T record = nextRecord;
    nextRecord = null;
    return record;
  }

  @Override
  public Map<Serializable, BufferedRecord<T>> getLogRecords() {
    return records;
  }

  @Override
  public int size() {
    return records.size();
  }

  public long getTotalLogRecords() {
    return totalLogRecords;
  }

  @Override
  public Iterator<BufferedRecord<T>> getLogRecordIterator() {
    return records.values().iterator();
  }

  @Override
  public void close() {
    records.close();
  }

  /**
   * Merge two log data records if needed.
   *
   * @param newRecord                  The new incoming record
   * @param existingRecord             The existing record
   * @return the {@link BufferedRecord} that needs to be updated, returns empty to skip the update.
   */
  protected Option<BufferedRecord<T>> doProcessNextDataRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord)
      throws IOException {
    totalLogRecords++;
    if (existingRecord != null) {
      if (enablePartialMerging) {
        // TODO(HUDI-7843): decouple the merging logic from the merger
        //  and use the record merge mode to control how to merge partial updates
        // Merge and store the combined record
        Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.get().partialMerge(
            readerContext.constructHoodieRecord(existingRecord),
            readerContext.getSchemaFromBufferRecord(existingRecord),
            readerContext.constructHoodieRecord(newRecord),
            readerContext.getSchemaFromBufferRecord(newRecord),
            readerSchema,
            props);
        if (!combinedRecordAndSchemaOpt.isPresent()) {
          return Option.empty();
        }
        Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
        HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

        // If pre-combine returns existing record, no need to update it
        if (combinedRecord.getData() != existingRecord.getRecord()) {
          return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), readerContext, props));
        }
        return Option.empty();
      } else {
        switch (recordMergeMode) {
          case COMMIT_TIME_ORDERING:
            return Option.of(newRecord);
          case EVENT_TIME_ORDERING:
            if (shouldKeepNewerRecord(existingRecord, newRecord)) {
              return Option.of(newRecord);
            }
            return Option.empty();
          case CUSTOM:
          default:
            if (existingRecord.isDelete() || newRecord.isDelete()) {
              if (shouldKeepNewerRecord(existingRecord, newRecord)) {
                // IMPORTANT:
                // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
                // return Option.empty when the old payload data is empty(a delete) and ignores its ordering value directly.
                return Option.of(newRecord);
              } else {
                return Option.empty();
              }
            }
            // Merge and store the combined record
            if (payloadClass.isPresent()) {
              Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = getMergedRecord(existingRecord, newRecord);
              if (combinedRecordAndSchemaOpt.isPresent()) {
                T combinedRecordData = readerContext.convertAvroRecord((IndexedRecord) combinedRecordAndSchemaOpt.get().getLeft().getData());
                // If pre-combine does not return existing record, update it
                if (combinedRecordData != existingRecord.getRecord()) {
                  Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
                  return Option.of(BufferedRecord.forRecordWithContext(combinedRecordData, combinedRecordAndSchema.getRight(), readerContext, orderingFieldName, false));
                }
              }
              return Option.empty();
            } else {
              Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.get().merge(
                  readerContext.constructHoodieRecord(existingRecord),
                  readerContext.getSchemaFromBufferRecord(existingRecord),
                  readerContext.constructHoodieRecord(newRecord),
                  readerContext.getSchemaFromBufferRecord(newRecord),
                  props);

              if (!combinedRecordAndSchemaOpt.isPresent()) {
                return Option.empty();
              }

              Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
              HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

              // If pre-combine returns existing record, no need to update it
              if (combinedRecord.getData() != existingRecord.getRecord()) {
                return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), readerContext, props));
              }
              return Option.empty();
            }
        }
      }
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      return Option.of(newRecord);
    }
  }

  /**
   * Merge a delete record with another record (data, or delete).
   *
   * @param deleteRecord               The delete record
   * @param existingRecord             The existing {@link BufferedRecord}
   *
   * @return The option of new delete record that needs to be updated with.
   */
  protected Option<DeleteRecord> doProcessNextDeletedRecord(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
    totalLogRecords++;
    if (existingRecord != null) {
      switch (recordMergeMode) {
        case COMMIT_TIME_ORDERING:
          return Option.of(deleteRecord);
        case EVENT_TIME_ORDERING:
        case CUSTOM:
        default:
          if (existingRecord.isCommitTimeOrderingDelete()) {
            return Option.empty();
          }
          Comparable existingOrderingVal = existingRecord.getOrderingValue();
          Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
          // Checks the ordering value does not equal to 0
          // because we use 0 as the default value which means natural order
          boolean chooseExisting = !deleteOrderingVal.equals(0)
              && ReflectionUtils.isSameClass(existingOrderingVal, deleteOrderingVal)
              && existingOrderingVal.compareTo(deleteOrderingVal) > 0;
          if (chooseExisting) {
            // The DELETE message is obsolete if the old message has greater orderingVal.
            return Option.empty();
          }
      }
    }
    // Do delete.
    return Option.of(deleteRecord);
  }

  /**
   * Create a record iterator for a data block. The records are filtered by a key set specified by {@code keySpecOpt}.
   *
   * @param dataBlock
   * @param keySpecOpt
   * @return
   */
  protected Pair<ClosableIterator<T>, Schema> getRecordsIterator(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    ClosableIterator<T> blockRecordsIterator;
    if (keySpecOpt.isPresent()) {
      KeySpec keySpec = keySpecOpt.get();
      blockRecordsIterator = dataBlock.getEngineRecordIterator(readerContext, keySpec.getKeys(), keySpec.isFullKey());
    } else {
      blockRecordsIterator = dataBlock.getEngineRecordIterator(readerContext);
    }
    Pair<Function<T, T>, Schema> schemaTransformerWithEvolvedSchema = getSchemaTransformerWithEvolvedSchema(dataBlock);
    return Pair.of(new CloseableMappingIterator<>(
        blockRecordsIterator, schemaTransformerWithEvolvedSchema.getLeft()), schemaTransformerWithEvolvedSchema.getRight());
  }

  /**
   * Get final Read Schema for support evolution.
   * step1: find the fileSchema for current dataBlock.
   * step2: determine whether fileSchema is compatible with the final read internalSchema.
   * step3: merge fileSchema and read internalSchema to produce final read schema.
   *
   * @param dataBlock current processed block
   * @return final read schema.
   */
  protected Option<Pair<Function<T, T>, Schema>> composeEvolvedSchemaTransformer(
      HoodieDataBlock dataBlock) {
    if (internalSchema.isEmptySchema()) {
      return Option.empty();
    }

    long currentInstantTime = Long.parseLong(dataBlock.getLogBlockHeader().get(INSTANT_TIME));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(currentInstantTime, hoodieTableMetaClient);
    Pair<InternalSchema, Map<String, String>> mergedInternalSchema = new InternalSchemaMerger(fileSchema, internalSchema,
        true, false, false).mergeSchemaGetRenamed();
    Schema mergedAvroSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema.getLeft(), readerSchema.getFullName());
    // `mergedAvroSchema` maybe not equal with `readerSchema`, case: drop a column `f_x`, and then add a new column with same name `f_x`,
    // then the new added column in `mergedAvroSchema` will have a suffix: `f_xsuffix`, distinguished from the original column `f_x`, see
    // InternalSchemaMerger#buildRecordType() for details.
    // Delete and add a field with the same name, reads should not return previously inserted datum of dropped field of the same name,
    // so we use `mergedAvroSchema` as the target schema for record projecting.
    return Option.of(Pair.of(readerContext.projectRecord(dataBlock.getSchema(), mergedAvroSchema, mergedInternalSchema.getRight()), mergedAvroSchema));
  }

  /**
   * Merge two records using the configured record merger.
   *
   * @param olderRecord  old {@link BufferedRecord}
   * @param newerRecord  newer {@link BufferedRecord}
   * @return a value pair, left is boolean value `isDelete`, and right is engine row.
   * @throws IOException
   */
  protected Pair<Boolean, T> merge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
    if (enablePartialMerging) {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().partialMerge(
          readerContext.constructHoodieRecord(olderRecord), readerContext.getSchemaFromBufferRecord(olderRecord),
          readerContext.constructHoodieRecord(newerRecord), readerContext.getSchemaFromBufferRecord(newerRecord),
          readerSchema, props);

      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          T data = (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData();
          return Pair.of(false, data);
        }
        return Pair.of(false, (T) hoodieRecord.getData());
      }
      return Pair.of(true, null);
    } else {
      switch (recordMergeMode) {
        case COMMIT_TIME_ORDERING:
          return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
        case EVENT_TIME_ORDERING:
          if (newerRecord.isCommitTimeOrderingDelete()) {
            return Pair.of(true, newerRecord.getRecord());
          }
          Comparable newOrderingValue = newerRecord.getOrderingValue();
          Comparable oldOrderingValue = olderRecord.getOrderingValue();
          if (!olderRecord.isCommitTimeOrderingDelete()
              && oldOrderingValue.compareTo(newOrderingValue) > 0) {
            return Pair.of(olderRecord.isDelete(), olderRecord.getRecord());
          }
          return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
        case CUSTOM:
        default:
          if (payloadClass.isPresent()) {
            if (olderRecord.isDelete() || newerRecord.isDelete()) {
              if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
                // IMPORTANT:
                // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
                // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
                return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
              } else {
                return Pair.of(olderRecord.isDelete(), olderRecord.getRecord());
              }
            }
            Option<Pair<HoodieRecord, Schema>> mergedRecord =
                getMergedRecord(olderRecord, newerRecord);
            if (mergedRecord.isPresent()
                && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
              IndexedRecord indexedRecord;
              if (!mergedRecord.get().getRight().equals(readerSchema)) {
                indexedRecord = (IndexedRecord) mergedRecord.get().getLeft().rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData();
              } else {
                indexedRecord = (IndexedRecord) mergedRecord.get().getLeft().getData();
              }
              return Pair.of(false, readerContext.convertAvroRecord(indexedRecord));
            }
            return Pair.of(true, null);
          } else {
            if (olderRecord.isDelete() || newerRecord.isDelete()) {
              if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
                // IMPORTANT:
                // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
                // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
                return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
              } else {
                return Pair.of(olderRecord.isDelete(), olderRecord.getRecord());
              }
            }
            Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
                readerContext.constructHoodieRecord(olderRecord), readerContext.getSchemaFromBufferRecord(olderRecord),
                readerContext.constructHoodieRecord(newerRecord), readerContext.getSchemaFromBufferRecord(newerRecord), props);
            if (mergedRecord.isPresent()
                && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
              HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
              if (!mergedRecord.get().getRight().equals(readerSchema)) {
                return Pair.of(false, (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData());
              }
              return Pair.of(false, (T) hoodieRecord.getData());
            }
            return Pair.of(true, null);
          }
      }
    }
  }

  /**
   * Decides whether to keep the incoming record with ordering value comparison.
   */
  private boolean shouldKeepNewerRecord(BufferedRecord<T> oldRecord, BufferedRecord<T> newRecord) {
    if (newRecord.isCommitTimeOrderingDelete()) {
      // handle records coming from DELETE statements(the orderingVal is constant 0)
      return true;
    }
    return newRecord.getOrderingValue().compareTo(oldRecord.getOrderingValue()) >= 0;
  }

  private Option<Pair<HoodieRecord, Schema>> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
    ValidationUtils.checkArgument(!Objects.equals(payloadClass, OverwriteWithLatestAvroPayload.class.getCanonicalName())
        && !Objects.equals(payloadClass, DefaultHoodieRecordPayload.class.getCanonicalName()));
    HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(readerContext, olderRecord);
    HoodieRecord newHoodieRecord = constructHoodieAvroRecord(readerContext, newerRecord);
    Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
        oldHoodieRecord, getSchemaForAvroPayloadMerge(oldHoodieRecord, olderRecord),
        newHoodieRecord, getSchemaForAvroPayloadMerge(newHoodieRecord, newerRecord), props);
    return mergedRecord;
  }

  /**
   * Constructs a new {@link HoodieAvroRecord} for payload based merging
   *
   * @param readerContext reader context
   * @param bufferedRecord buffered record
   * @return A new instance of {@link HoodieRecord}.
   */
  private HoodieRecord constructHoodieAvroRecord(HoodieReaderContext<T> readerContext, BufferedRecord<T> bufferedRecord) {
    GenericRecord record = null;
    if (!bufferedRecord.isDelete()) {
      Schema recordSchema = readerContext.getSchemaFromBufferRecord(bufferedRecord);
      record = readerContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
    }
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), null);
    return new HoodieAvroRecord<>(hoodieKey,
        HoodieRecordUtils.loadPayload(payloadClass.get(), record, bufferedRecord.getOrderingValue()), null);
  }

  private Schema getSchemaForAvroPayloadMerge(HoodieRecord record, BufferedRecord<T> bufferedRecord) throws IOException {
    if (record.isDelete(readerSchema, props)) {
      return readerSchema;
    }
    return readerContext.getSchemaFromBufferRecord(bufferedRecord);
  }

  protected boolean hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) throws IOException {
    if (logRecordInfo != null) {
      BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(baseRecord, readerSchema, readerContext, orderingFieldName, false);
      Pair<Boolean, T> isDeleteAndRecord = merge(bufferedRecord, logRecordInfo);
      if (!isDeleteAndRecord.getLeft()) {
        // Updates
        nextRecord = readerContext.seal(isDeleteAndRecord.getRight());
        readStats.incrementNumUpdates();
        return true;
      } else {
        // Deletes
        readStats.incrementNumDeletes();
        return false;
      }
    }

    // Inserts
    nextRecord = readerContext.seal(baseRecord);
    readStats.incrementNumInserts();
    return true;
  }

  protected boolean hasNextLogRecord() {
    if (logRecordIterator == null) {
      logRecordIterator = records.values().iterator();
    }

    while (logRecordIterator.hasNext()) {
      BufferedRecord<T> nextRecordInfo = logRecordIterator.next();
      if (!nextRecordInfo.isDelete()) {
        nextRecord = nextRecordInfo.getRecord();
        readStats.incrementNumInserts();
        return true;
      } else {
        readStats.incrementNumDeletes();
      }
    }
    return false;
  }

  protected Pair<Function<T, T>, Schema> getSchemaTransformerWithEvolvedSchema(HoodieDataBlock dataBlock) {
    Option<Pair<Function<T, T>, Schema>> schemaEvolutionTransformerOpt =
        composeEvolvedSchemaTransformer(dataBlock);

    // In case when schema has been evolved original persisted records will have to be
    // transformed to adhere to the new schema
    Function<T, T> transformer =
        schemaEvolutionTransformerOpt.map(Pair::getLeft)
            .orElse(Function.identity());

    Schema evolvedSchema = schemaEvolutionTransformerOpt.map(Pair::getRight)
        .orElseGet(dataBlock::getSchema);
    return Pair.of(transformer, evolvedSchema);
  }

  static boolean isCommitTimeOrderingValue(Comparable orderingValue) {
    return orderingValue == null || orderingValue.equals(DEFAULT_ORDERING_VALUE);
  }

  static Comparable getOrderingValue(HoodieReaderContext readerContext,
                                     DeleteRecord deleteRecord) {
    return isCommitTimeOrderingValue(deleteRecord.getOrderingValue())
        ? DEFAULT_ORDERING_VALUE
        : readerContext.convertValueToEngineType(deleteRecord.getOrderingValue());
  }

  private boolean isDeleteRecord(Option<T> record, Schema schema) {
    if (record.isEmpty()) {
      return true;
    }

    Object operation = readerContext.getValue(record.get(), schema, OPERATION_METADATA_FIELD);
    if (operation != null && HoodieOperation.isDeleteRecord(operation.toString())) {
      return true;
    }

    Object deleteMarker = readerContext.getValue(record.get(), schema, HOODIE_IS_DELETED_FIELD);
    return deleteMarker instanceof Boolean && (boolean) deleteMarker;
  }
}
