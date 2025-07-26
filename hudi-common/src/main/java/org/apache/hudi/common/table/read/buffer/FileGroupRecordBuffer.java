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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.MergeResult;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

abstract class FileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final List<String> orderingFieldNames;
  protected final RecordMergeMode recordMergeMode;
  protected final PartialUpdateMode partialUpdateMode;
  protected final Option<HoodieRecordMerger> recordMerger;
  protected final Option<String> payloadClass;
  protected final TypedProperties props;
  protected final ExternalSpillableMap<Serializable, BufferedRecord<T>> records;
  protected final boolean shouldCheckCustomDeleteMarker;
  protected final boolean shouldCheckBuiltInDeleteMarker;
  protected ClosableIterator<T> baseFileIterator;
  protected UpdateProcessor<T> updateProcessor;
  protected Iterator<BufferedRecord<T>> logRecordIterator;
  protected T nextRecord;
  protected boolean enablePartialMerging = false;
  protected InternalSchema internalSchema;
  protected HoodieTableMetaClient hoodieTableMetaClient;
  protected BufferedRecordMerger<T> bufferedRecordMerger;
  protected long totalLogRecords = 0;

  protected FileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                  HoodieTableMetaClient hoodieTableMetaClient,
                                  RecordMergeMode recordMergeMode,
                                  PartialUpdateMode partialUpdateMode,
                                  TypedProperties props,
                                  List<String> orderingFieldNames,
                                  UpdateProcessor<T> updateProcessor) {
    this.readerContext = readerContext;
    this.updateProcessor = updateProcessor;
    this.readerSchema = AvroSchemaCache.intern(readerContext.getSchemaHandler().getRequiredSchema());
    this.recordMergeMode = recordMergeMode;
    this.partialUpdateMode = partialUpdateMode;
    this.recordMerger = readerContext.getRecordMerger();
    if (recordMerger.isPresent() && recordMerger.get().getMergingStrategy().equals(PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      this.payloadClass = Option.of(hoodieTableMetaClient.getTableConfig().getPayloadClass());
    } else {
      this.payloadClass = Option.empty();
    }
    this.orderingFieldNames = orderingFieldNames;
    // Ensure that ordering field is populated for mergers and legacy payloads
    this.props = ConfigUtils.supplementOrderingFields(props, orderingFieldNames);
    this.internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    this.hoodieTableMetaClient = hoodieTableMetaClient;
    String spillableMapBasePath = props.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath());
    try {
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = initializeRecordsMap(spillableMapBasePath);
    } catch (IOException e) {
      throw new HoodieIOException("IOException when creating ExternalSpillableMap at " + spillableMapBasePath, e);
    }
    this.shouldCheckCustomDeleteMarker =
        readerContext.getSchemaHandler().getCustomDeleteMarkerKeyValue().isPresent();
    this.shouldCheckBuiltInDeleteMarker =
        readerContext.getSchemaHandler().hasBuiltInDelete();
    this.bufferedRecordMerger = BufferedRecordMergerFactory.create(
        readerContext, recordMergeMode, enablePartialMerging, recordMerger, orderingFieldNames, payloadClass, readerSchema, props, partialUpdateMode);
  }

  protected ExternalSpillableMap<Serializable, BufferedRecord<T>> initializeRecordsMap(String spillableMapBasePath) throws IOException {
    long maxMemorySizeInBytes = props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(props.getString(SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
    boolean isBitCaskDiskMapCompressionEnabled = props.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    return new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator<>(),
        readerContext.getRecordSizeEstimator(), diskMapType, readerContext.getRecordSerializer(), isBitCaskDiskMapCompressionEnabled, getClass().getSimpleName());
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
    return columnValue != null && readerContext.getTypeConverter().castToBoolean(columnValue);
  }

  /**
   * Returns whether the record is a DELETE marked by the '_hoodie_operation' field.
   */
  protected final boolean isDeleteHoodieOperation(T record) {
    int hoodieOperationPos = readerContext.getSchemaHandler().getHoodieOperationPos();
    if (hoodieOperationPos < 0) {
      return false;
    }
    String hoodieOperation = readerContext.getMetaFieldValue(record, hoodieOperationPos);
    return hoodieOperation != null && HoodieOperation.isDeleteRecord(hoodieOperation);
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
  public ClosableIterator<BufferedRecord<T>> getLogRecordIterator() {
    return new LogRecordIterator<>(this);
  }

  @Override
  public void close() {
    records.close();
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

  protected boolean hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) throws IOException {
    if (logRecordInfo != null) {
      BufferedRecord<T> baseRecordInfo = BufferedRecord.forRecordWithContext(baseRecord, readerSchema, readerContext, orderingFieldNames, false);
      MergeResult<T> mergeResult = bufferedRecordMerger.finalMerge(baseRecordInfo, logRecordInfo);
      nextRecord = updateProcessor.processUpdate(logRecordInfo.getRecordKey(), baseRecord, mergeResult.getMergedRecord(), mergeResult.isDelete());
      return nextRecord != null;
    }

    // Inserts
    nextRecord = readerContext.seal(baseRecord);
    return true;
  }

  protected void initializeLogRecordIterator() {
    logRecordIterator = records.iterator();
  }

  protected boolean hasNextLogRecord() {
    if (logRecordIterator == null) {
      initializeLogRecordIterator();
    }

    while (logRecordIterator.hasNext()) {
      BufferedRecord<T> nextRecordInfo = logRecordIterator.next();
      nextRecord = updateProcessor.processUpdate(nextRecordInfo.getRecordKey(), null, nextRecordInfo.getRecord(), nextRecordInfo.isDelete());
      if (nextRecord != null) {
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

  static boolean isCommitTimeOrderingValue(Comparable orderingValue) {
    return orderingValue == null || OrderingValues.isDefault(orderingValue);
  }

  static Comparable getOrderingValue(HoodieReaderContext readerContext,
                                     DeleteRecord deleteRecord) {
    Comparable orderingValue = deleteRecord.getOrderingValue();
    return isCommitTimeOrderingValue(orderingValue)
        ? OrderingValues.getDefault()
        : readerContext.convertOrderingValueToEngineType(orderingValue);
  }

  private static class LogRecordIterator<T> implements ClosableIterator<BufferedRecord<T>> {
    private final FileGroupRecordBuffer<T> fileGroupRecordBuffer;
    private final Iterator<BufferedRecord<T>> logRecordIterator;

    private LogRecordIterator(FileGroupRecordBuffer<T> fileGroupRecordBuffer) {
      this.fileGroupRecordBuffer = fileGroupRecordBuffer;
      this.logRecordIterator = fileGroupRecordBuffer.records.iterator();
    }

    @Override
    public boolean hasNext() {
      return logRecordIterator.hasNext();
    }

    @Override
    public BufferedRecord<T> next() {
      return logRecordIterator.next();
    }

    @Override
    public void close() {
      fileGroupRecordBuffer.close();
    }
  }
}
