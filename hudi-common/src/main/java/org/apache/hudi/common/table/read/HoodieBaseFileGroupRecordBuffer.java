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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_SCHEMA;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static org.apache.hudi.common.table.read.HoodieFileGroupReader.getRecordMergeMode;

public abstract class HoodieBaseFileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final Option<String> partitionNameOverrideOpt;
  protected final Option<String[]> partitionPathFieldOpt;
  protected final RecordMergeMode recordMergeMode;
  protected final HoodieRecordMerger recordMerger;
  protected final TypedProperties payloadProps;
  protected final ExternalSpillableMap<Serializable, Pair<Option<T>, Map<String, Object>>> records;
  protected ClosableIterator<T> baseFileIterator;
  protected Iterator<Pair<Option<T>, Map<String, Object>>> logRecordIterator;
  protected T nextRecord;
  protected boolean enablePartialMerging = false;
  protected InternalSchema internalSchema;
  protected HoodieTableMetaClient hoodieTableMetaClient;

  public HoodieBaseFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                         HoodieTableMetaClient hoodieTableMetaClient,
                                         Option<String> partitionNameOverrideOpt,
                                         Option<String[]> partitionPathFieldOpt,
                                         HoodieRecordMerger recordMerger,
                                         TypedProperties payloadProps) {
    this.readerContext = readerContext;
    this.readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    this.partitionNameOverrideOpt = partitionNameOverrideOpt;
    this.partitionPathFieldOpt = partitionPathFieldOpt;
    this.recordMergeMode = getRecordMergeMode(payloadProps);
    this.recordMerger = recordMerger;
    //Custom merge mode should produce the same results for any merger so we won't fail if there is a mismatch
    if (recordMerger.getRecordMergeMode() != this.recordMergeMode && this.recordMergeMode != RecordMergeMode.CUSTOM) {
      throw new IllegalStateException("Record merger is " + recordMerger.getClass().getName() + " but merge mode is " + this.recordMergeMode);
    }
    this.payloadProps = payloadProps;
    this.internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    this.hoodieTableMetaClient = hoodieTableMetaClient;
    long maxMemorySizeInBytes = payloadProps.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
    String spillableMapBasePath = payloadProps.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath());
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(payloadProps.getString(SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
    boolean isBitCaskDiskMapCompressionEnabled = payloadProps.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    try {
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator<>(),
          new HoodieRecordSizeEstimator<>(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);
    } catch (IOException e) {
      throw new HoodieIOException("IOException when creating ExternalSpillableMap at " + spillableMapBasePath, e);
    }
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
  public Map<Serializable, Pair<Option<T>, Map<String, Object>>> getLogRecords() {
    return records;
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public Iterator<Pair<Option<T>, Map<String, Object>>> getLogRecordIterator() {
    return records.values().iterator();
  }

  @Override
  public void close() {
    records.clear();
  }

  /**
   * Compares two {@link Comparable}s.  If both are numbers, converts them to {@link Long} for comparison.
   * If one of the {@link Comparable}s is a String, assumes that both are String values for comparison.
   *
   * @param readerContext {@link HoodieReaderContext} instance.
   * @param o1 {@link Comparable} object.
   * @param o2 other {@link Comparable} object to compare to.
   * @return comparison result.
   */
  @VisibleForTesting
  static int compareTo(HoodieReaderContext readerContext, Comparable o1, Comparable o2) {
    // TODO(HUDI-7848): fix the delete records to contain the correct ordering value type
    //  so this util with the number comparison is not necessary.
    try {
      return o1.compareTo(o2);
    } catch (ClassCastException e) {
      boolean isO1LongOrInteger = (o1 instanceof Long || o1 instanceof Integer);
      boolean isO2LongOrInteger = (o2 instanceof Long || o2 instanceof Integer);
      boolean isO1DoubleOrFloat = (o1 instanceof Double || o1 instanceof Float);
      boolean isO2DoubleOrFloat = (o2 instanceof Double || o2 instanceof Float);
      if (isO1LongOrInteger && isO2LongOrInteger) {
        Long o1LongValue = ((Number) o1).longValue();
        Long o2LongValue = ((Number) o2).longValue();
        return o1LongValue.compareTo(o2LongValue);
      } else if ((isO1LongOrInteger && isO2DoubleOrFloat)
          || (isO1DoubleOrFloat && isO2LongOrInteger)) {
        Double o1DoubleValue = ((Number) o1).doubleValue();
        Double o2DoubleValue = ((Number) o2).doubleValue();
        return o1DoubleValue.compareTo(o2DoubleValue);
      } else {
        return readerContext.compareTo(o1, o2);
      }
    } catch (Throwable e) {
      throw new HoodieException("Cannot compare values: "
          + o1 + "(" + o1.getClass() + "), " + o2 + "(" + o2.getClass() + ")", e);
    }
  }

  /**
   * Merge two log data records if needed.
   *
   * @param record
   * @param metadata
   * @param existingRecordMetadataPair
   * @return
   * @throws IOException
   */
  protected Option<Pair<T, Map<String, Object>>> doProcessNextDataRecord(T record,
                                                                         Map<String, Object> metadata,
                                                                         Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair) throws IOException {
    if (existingRecordMetadataPair != null) {
      if (enablePartialMerging) {
        // TODO(HUDI-7843): decouple the merging logic from the merger
        //  and use the record merge mode to control how to merge partial updates
        // Merge and store the combined record
        // Note that the incoming `record` is from an older commit, so it should be put as
        // the `older` in the merge API
        Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.partialMerge(
            readerContext.constructHoodieRecord(Option.of(record), metadata),
            (Schema) metadata.get(INTERNAL_META_SCHEMA),
            readerContext.constructHoodieRecord(
                existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight()),
            (Schema) existingRecordMetadataPair.getRight().get(INTERNAL_META_SCHEMA),
            readerSchema,
            payloadProps);
        if (!combinedRecordAndSchemaOpt.isPresent()) {
          return Option.empty();
        }
        Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
        HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

        // If pre-combine returns existing record, no need to update it
        if (combinedRecord.getData() != existingRecordMetadataPair.getLeft().get()) {
          return Option.of(Pair.of(
              combinedRecord.getData(),
              readerContext.updateSchemaAndResetOrderingValInMetadata(metadata, combinedRecordAndSchema.getRight())));
        }
        return Option.empty();
      } else {
        switch (recordMergeMode) {
          case OVERWRITE_WITH_LATEST:
            return Option.empty();
          case EVENT_TIME_ORDERING:
            Comparable existingOrderingValue = readerContext.getOrderingValue(
                existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight(), readerSchema, payloadProps);
            if (isDeleteRecordWithNaturalOrder(existingRecordMetadataPair.getLeft(), existingOrderingValue)) {
              return Option.empty();
            }
            Comparable incomingOrderingValue = readerContext.getOrderingValue(
                Option.of(record), metadata, readerSchema, payloadProps);
            if (compareTo(readerContext, incomingOrderingValue, existingOrderingValue) > 0) {
              return Option.of(Pair.of(record, metadata));
            }
            return Option.empty();
          case CUSTOM:
          default:
            // Merge and store the combined record
            // Note that the incoming `record` is from an older commit, so it should be put as
            // the `older` in the merge API
            Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.merge(
                readerContext.constructHoodieRecord(Option.of(record), metadata),
                (Schema) metadata.get(INTERNAL_META_SCHEMA),
                readerContext.constructHoodieRecord(
                    existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight()),
                (Schema) existingRecordMetadataPair.getRight().get(INTERNAL_META_SCHEMA),
                payloadProps);

            if (!combinedRecordAndSchemaOpt.isPresent()) {
              return Option.empty();
            }

            Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
            HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

            // If pre-combine returns existing record, no need to update it
            if (combinedRecord.getData() != existingRecordMetadataPair.getLeft().get()) {
              return Option.of(Pair.of(combinedRecord.getData(), metadata));
            }
            return Option.empty();
        }
      }
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      return Option.of(Pair.of(record, metadata));
    }
  }

  /**
   * Merge a delete record with another record (data, or delete).
   *
   * @param deleteRecord
   * @param existingRecordMetadataPair
   * @return
   */
  protected Option<DeleteRecord> doProcessNextDeletedRecord(DeleteRecord deleteRecord,
                                                            Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair) {
    if (existingRecordMetadataPair != null) {
      switch (recordMergeMode) {
        case OVERWRITE_WITH_LATEST:
          return Option.empty();
        case EVENT_TIME_ORDERING:
        case CUSTOM:
        default:
          Comparable existingOrderingVal = readerContext.getOrderingValue(
              existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight(), readerSchema,
              payloadProps);
          if (isDeleteRecordWithNaturalOrder(existingRecordMetadataPair.getLeft(), existingOrderingVal)) {
            return Option.empty();
          }
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
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(currentInstantTime,
        hoodieTableMetaClient, false);
    Pair<InternalSchema, Map<String, String>> mergedInternalSchema = new InternalSchemaMerger(fileSchema, internalSchema,
        true, false, false).mergeSchemaGetRenamed();
    Schema mergedAvroSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema.getLeft(), readerSchema.getFullName());
    assert mergedAvroSchema.equals(readerSchema);
    return Option.of(Pair.of(readerContext.projectRecord(dataBlock.getSchema(), mergedAvroSchema, mergedInternalSchema.getRight()), mergedAvroSchema));
  }

  /**
   * Merge two records using the configured record merger.
   *
   * @param older
   * @param olderInfoMap
   * @param newer
   * @param newerInfoMap
   * @return
   * @throws IOException
   */
  protected Option<T> merge(Option<T> older, Map<String, Object> olderInfoMap,
                            Option<T> newer, Map<String, Object> newerInfoMap) throws IOException {
    if (!older.isPresent()) {
      return newer;
    }

    if (enablePartialMerging) {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.partialMerge(
          readerContext.constructHoodieRecord(older, olderInfoMap), (Schema) olderInfoMap.get(INTERNAL_META_SCHEMA),
          readerContext.constructHoodieRecord(newer, newerInfoMap), (Schema) newerInfoMap.get(INTERNAL_META_SCHEMA),
          readerSchema, payloadProps);

      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), payloadProps)) {
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          return Option.ofNullable((T) mergedRecord.get().getLeft().rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData());
        }
        return Option.ofNullable((T) mergedRecord.get().getLeft().getData());
      }
      return Option.empty();
    } else {
      switch (recordMergeMode) {
        case OVERWRITE_WITH_LATEST:
          return newer;
        case EVENT_TIME_ORDERING:
          Comparable oldOrderingValue = readerContext.getOrderingValue(
              older, olderInfoMap, readerSchema, payloadProps);
          if (isDeleteRecordWithNaturalOrder(older, oldOrderingValue)) {
            return newer;
          }
          Comparable newOrderingValue = readerContext.getOrderingValue(
              newer, newerInfoMap, readerSchema, payloadProps);
          if (isDeleteRecordWithNaturalOrder(newer, newOrderingValue)) {
            return Option.empty();
          }
          if (compareTo(readerContext, oldOrderingValue, newOrderingValue) > 0) {
            return older;
          }
          return newer;
        case CUSTOM:
        default:
          Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.merge(
              readerContext.constructHoodieRecord(older, olderInfoMap), (Schema) olderInfoMap.get(INTERNAL_META_SCHEMA),
              readerContext.constructHoodieRecord(newer, newerInfoMap), (Schema) newerInfoMap.get(INTERNAL_META_SCHEMA), payloadProps);

          if (mergedRecord.isPresent()
              && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), payloadProps)) {
            if (!mergedRecord.get().getRight().equals(readerSchema)) {
              return Option.ofNullable((T) mergedRecord.get().getLeft().rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData());
            }
            return Option.ofNullable((T) mergedRecord.get().getLeft().getData());
          }
          return Option.empty();
      }
    }
  }

  protected boolean hasNextBaseRecord(T baseRecord, Pair<Option<T>, Map<String, Object>> logRecordInfo) throws IOException {
    Map<String, Object> metadata = readerContext.generateMetadataForRecord(
        baseRecord, readerSchema);

    Option<T> resultRecord = logRecordInfo != null
        ? merge(Option.of(baseRecord), metadata, logRecordInfo.getLeft(), logRecordInfo.getRight())
        : merge(Option.empty(), Collections.emptyMap(), Option.of(baseRecord), metadata);
    if (resultRecord.isPresent()) {
      nextRecord = readerContext.seal(resultRecord.get());
      return true;
    }
    return false;
  }

  protected boolean hasNextLogRecord() throws IOException {
    if (logRecordIterator == null) {
      logRecordIterator = records.values().iterator();
    }

    while (logRecordIterator.hasNext()) {
      Pair<Option<T>, Map<String, Object>> nextRecordInfo = logRecordIterator.next();
      Option<T> resultRecord;
      resultRecord = merge(Option.empty(), Collections.emptyMap(),
          nextRecordInfo.getLeft(), nextRecordInfo.getRight());
      if (resultRecord.isPresent()) {
        nextRecord = readerContext.seal(resultRecord.get());
        return true;
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

  private boolean isDeleteRecordWithNaturalOrder(Option<T> rowOption,
                                                 Comparable orderingValue) {
    return rowOption.isEmpty() && orderingValue.equals(0);
  }
}
