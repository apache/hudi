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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
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
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

public abstract class FileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final Integer readerSchemaId;
  protected final Option<String> orderingFieldName;
  protected final TypedProperties props;
  protected final EngineBasedMerger<T> merger;
  protected final ExternalSpillableMap<Serializable, BufferedRecord<T>> records;
  protected final HoodieReadStats readStats;
  protected final boolean shouldCheckCustomDeleteMarker;
  protected final boolean shouldCheckBuiltInDeleteMarker;
  protected final boolean emitDelete;
  protected ClosableIterator<T> baseFileIterator;
  protected Iterator<BufferedRecord<T>> logRecordIterator;
  protected T nextRecord;
  protected boolean enablePartialMerging = false;
  protected InternalSchema internalSchema;
  protected HoodieTableMetaClient hoodieTableMetaClient;
  protected long totalLogRecords = 0;

  protected FileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                  HoodieTableMetaClient hoodieTableMetaClient,
                                  TypedProperties props,
                                  HoodieReadStats readStats,
                                  Option<String> orderingFieldName,
                                  EngineBasedMerger<T> merger,
                                  boolean emitDelete) {
    this.readerContext = readerContext;
    this.readerSchema = AvroSchemaCache.intern(readerContext.getSchemaHandler().getRequiredSchema());
    this.readerSchemaId = readerContext.encodeAvroSchema(readerSchema);
    this.orderingFieldName = orderingFieldName;
    this.props = props;
    this.merger = merger;
    this.internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    this.hoodieTableMetaClient = hoodieTableMetaClient;
    long maxMemorySizeInBytes = props.getLong(MAX_MEMORY_FOR_MERGE.key(), MAX_MEMORY_FOR_MERGE.defaultValue());
    String spillableMapBasePath = props.getString(SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath());
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.valueOf(props.getString(SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT));
    boolean isBitCaskDiskMapCompressionEnabled = props.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    this.readStats = readStats;
    this.emitDelete = emitDelete;
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
    BufferedRecord<T> merged = merger.merge(Option.ofNullable(existingRecord), Option.of(newRecord), enablePartialMerging);
    // if the merged record is the same as the existing record, we can skip the processing
    if (merged == existingRecord) {
      return Option.empty();
    }
    return Option.of(merged);
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
    if (merger.shouldKeepIncomingDelete(deleteRecord, existingRecord)) {
      return Option.of(deleteRecord);
    }
    return Option.empty();
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
   * Merge record from base file and log file using the configured merger.
   *
   * @param baseRecord  old {@link BufferedRecord} from the base file
   * @param logRecord  newer {@link BufferedRecord} from the log file, may be null
   * @return a value pair, left is boolean value `isDelete`, and right is engine row.
   * @throws IOException
   */
  private BufferedRecord<T> merge(BufferedRecord<T> baseRecord, BufferedRecord<T> logRecord) throws IOException {
    return merger.merge(Option.of(baseRecord), Option.ofNullable(logRecord), enablePartialMerging);
  }

  protected boolean hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) throws IOException {
    if (logRecordInfo != null) {
      BufferedRecord<T> baseRecordInfo = BufferedRecord.forRecordWithContext(baseRecord, readerSchema, readerContext, orderingFieldName, false);
      BufferedRecord<T> merged = merge(baseRecordInfo, logRecordInfo);
      if (!merged.isDelete()) {
        // Updates
        nextRecord = readerContext.seal(merged.getRecord());
        readStats.incrementNumUpdates();
        return true;
      } else if (emitDelete) {
        // emit Deletes
        nextRecord = readerContext.getDeleteRow(merged.getRecord(), baseRecordInfo.getRecordKey());
        readStats.incrementNumDeletes();
        return nextRecord != null;
      } else {
        // not emit Deletes
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
      } else if (emitDelete) {
        nextRecord = readerContext.getDeleteRow(nextRecordInfo.getRecord(), nextRecordInfo.getRecordKey());
        readStats.incrementNumDeletes();
        if (nextRecord != null) {
          return true;
        }
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
}
