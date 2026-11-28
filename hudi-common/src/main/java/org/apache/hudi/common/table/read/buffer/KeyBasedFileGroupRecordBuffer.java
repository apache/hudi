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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BatchSupport;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.FileIOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into a record key based map.
 * The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class KeyBasedFileGroupRecordBuffer<T> extends FileGroupRecordBuffer<T> {
  // The share of the merge memory that the applied deletes can take before they spill
  private static final long APPLIED_DELETES_MEMORY_DIVISOR = 10;

  /**
   * The deletes already merged with a base record, by record key, without their rows. A base file can hold
   * more than one record with the same key (an insert does not enforce key uniqueness), and a delete of the
   * key applies to each of them. Null until the first one.
   */
  private ExternalSpillableMap<Serializable, BufferedRecord<T>> appliedDeletes;

  public KeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                       RecordMergeMode recordMergeMode,
                                       Option<PartialUpdateMode> partialUpdateModeOpt,
                                       TypedProperties props,
                                       List<String> orderingFieldNames,
                                       UpdateProcessor<T> updateProcessor) {
    super(readerContext, recordMergeMode, partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
  }

  /**
   * @deprecated the meta client is not used; use the constructor without it.
   */
  @Deprecated
  public KeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                       HoodieTableMetaClient hoodieTableMetaClient,
                                       RecordMergeMode recordMergeMode,
                                       Option<PartialUpdateMode> partialUpdateModeOpt,
                                       TypedProperties props,
                                       List<String> orderingFieldNames,
                                       UpdateProcessor<T> updateProcessor) {
    this(readerContext, recordMergeMode, partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    Pair<ClosableIterator<T>, HoodieSchema> recordsIteratorSchemaPair =
        getRecordsIterator(dataBlock, keySpecOpt);
    if (dataBlock.containsPartialUpdates() && !enablePartialMerging) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
      bufferedRecordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          recordMergeMode,
          true,
          recordMerger,
          readerSchema,
          payloadClasses,
          props,
          partialUpdateModeOpt);
    }

    HoodieSchema schema = HoodieSchemaCache.intern(recordsIteratorSchemaPair.getRight());

    RecordContext<T> recordContext = readerContext.getRecordContext();
    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        boolean isDelete = recordContext.isDeleteRecord(nextRecord, deleteContext);
        BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(nextRecord, schema, readerContext.getRecordContext(), orderingFieldNames, isDelete);
        processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      }
    }
  }

  /**
   * Merges the incoming record with whatever is already buffered under {@code recordKey} and stores the
   * result. Subclasses choose the identifier a record is buffered under (a record key here, a record
   * position in {@link PositionBasedFileGroupRecordBuffer}), but cannot replace this merge-then-store
   * step itself, hence the {@code final}.
   *
   * <p>This is not the only way {@code records} is mutated. The map is {@code protected}, and
   * {@link PositionBasedFileGroupRecordBuffer} writes to it directly where merging would be wrong: when
   * re-keying already-merged entries in its key-based fallback, and when overwriting delete markers under
   * commit-time ordering.
   */
  @Override
  public final void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    BufferedRecord<T> existingRecord = records.get(recordKey);
    totalLogRecords++;
    bufferedRecordMerger.deltaMerge(record, existingRecord).ifPresent(bufferedRecord ->
        records.put(recordKey, bufferedRecord.toBinary(readerContext.getRecordContext())));
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    for (BufferedRecord<T> record : deleteBlock.getRecordsToDelete(readerContext)) {
      processNextDataRecord(record, record.getRecordKey());
    }
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
    return mergeBaseRecord(recordKey, baseRecord, records.remove(recordKey));
  }

  /**
   * Merges a base record with the log record taken from the buffer, or, when an earlier base record with the key took
   * it and it is a delete, with that delete. A base record the delete removes is only counted: the earlier base record
   * already reported the change of the key.
   */
  protected final boolean mergeBaseRecord(String recordKey, T baseRecord, BufferedRecord<T> logRecordInfo) throws IOException {
    if (logRecordInfo != null) {
      if (logRecordInfo.isDelete()) {
        putAppliedDelete(recordKey, copy(logRecordInfo));
      }
      return hasNextBaseRecord(baseRecord, logRecordInfo);
    }
    BufferedRecord<T> appliedDelete = getAppliedDelete(recordKey);
    if (appliedDelete != null) {
      BufferedRecord<T> baseRecordInfo = BufferedRecords.fromEngineRecord(baseRecord, readerSchema, readerContext.getRecordContext(), orderingFieldNames, false);
      if (bufferedRecordMerger.finalMerge(baseRecordInfo, copy(appliedDelete)).isDelete()) {
        updateProcessor.processDeletedDuplicate();
        return false;
      }
    }
    return hasNextBaseRecord(baseRecord, null);
  }

  protected void putAppliedDelete(String recordKey, BufferedRecord<T> delete) {
    if (appliedDeletes == null) {
      appliedDeletes = createAppliedDeletesMap();
    }
    appliedDeletes.put(recordKey, delete);
  }

  protected BufferedRecord<T> getAppliedDelete(String recordKey) {
    return appliedDeletes == null ? null : appliedDeletes.get(recordKey);
  }

  private ExternalSpillableMap<Serializable, BufferedRecord<T>> createAppliedDeletesMap() {
    try {
      return createSpillableMap(props.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath()),
          props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue()) / APPLIED_DELETES_MEMORY_DIVISOR);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create the map of applied deletes", e);
    }
  }

  /**
   * A copy of the delete, with its row if it has one, as the merge can modify the record it is given.
   */
  private static <T> BufferedRecord<T> copy(BufferedRecord<T> delete) {
    return new BufferedRecord<>(delete.getRecordKey(), delete.getOrderingValue(), delete.getRecord(), delete.getSchemaId(), delete.getHoodieOperation());
  }

  @Override
  public void close() {
    try {
      if (appliedDeletes != null) {
        appliedDeletes.close();
      }
    } finally {
      super.close();
    }
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      if (hasNextBaseRecord(baseFileIterator.next())) {
        return true;
      }
    }

    // Handle records solely from log files.
    return hasNextLogRecord();
  }

  /**
   * Whether partial merging has been switched on for this buffer. Subclasses cannot replace this accessor,
   * hence the {@code final}; they may still set the underlying {@code enablePartialMerging} flag while
   * processing a data block, as {@link PositionBasedFileGroupRecordBuffer} does.
   */
  public final boolean isPartialMergingEnabled() {
    return enablePartialMerging;
  }

  @Override
  public Option<BatchMatcher<T>> batchMatcher(BatchSupport<T, ?> batchSupport) {
    if (enablePartialMerging || records.getDiskBasedMapNumEntries() > 0) {
      return Option.empty();
    }
    Map<Object, BufferedRecord<T>> byKey = new HashMap<>(Math.max(16, records.size() * 2));
    for (Map.Entry<Serializable, BufferedRecord<T>> entry : records.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        return Option.empty();
      }
      byKey.put(batchSupport.toEngineKey((String) entry.getKey()), entry.getValue());
    }
    return Option.of(new BatchMatcher.KeyBased<>(byKey));
  }
}
