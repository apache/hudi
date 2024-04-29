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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
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
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_SCHEMA;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;

public abstract class HoodieBaseFileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final Option<String> partitionNameOverrideOpt;
  protected final Option<String[]> partitionPathFieldOpt;
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
                                         TypedProperties payloadProps,
                                         long maxMemorySizeInBytes,
                                         String spillableMapBasePath,
                                         ExternalSpillableMap.DiskMapType diskMapType,
                                         boolean isBitCaskDiskMapCompressionEnabled) {
    this.readerContext = readerContext;
    this.readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    this.partitionNameOverrideOpt = partitionNameOverrideOpt;
    this.partitionPathFieldOpt = partitionPathFieldOpt;
    this.recordMerger = recordMerger;
    this.payloadProps = payloadProps;
    this.internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    this.hoodieTableMetaClient = hoodieTableMetaClient;
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
      // Merge and store the combined record
      // Note that the incoming `record` is from an older commit, so it should be put as
      // the `older` in the merge API
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = enablePartialMerging
          ? recordMerger.partialMerge(
          readerContext.constructHoodieRecord(Option.of(record), metadata),
          (Schema) metadata.get(INTERNAL_META_SCHEMA),
          readerContext.constructHoodieRecord(
              existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight()),
          (Schema) existingRecordMetadataPair.getRight().get(INTERNAL_META_SCHEMA),
          readerSchema,
          payloadProps)
          : recordMerger.merge(
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
        return Option.of(Pair.of(
            combinedRecord.getData(),
            enablePartialMerging
                ? readerContext.updateSchemaAndResetOrderingValInMetadata(metadata, combinedRecordAndSchema.getRight())
                : metadata));
      }
      return Option.empty();
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
      // Merge and store the merged record. The ordering val is taken to decide whether the same key record
      // should be deleted or be kept. The old record is kept only if the DELETE record has smaller ordering val.
      // For same ordering values, uses the natural order(arrival time semantics).
      Comparable existingOrderingVal = readerContext.getOrderingValue(
          existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight(), readerSchema,
          payloadProps);
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
    Option<Pair<Function<T,T>, Schema>> schemaEvolutionTransformerOpt =
        composeEvolvedSchemaTransformer(dataBlock);

    // In case when schema has been evolved original persisted records will have to be
    // transformed to adhere to the new schema
    Function<T,T> transformer =
        schemaEvolutionTransformerOpt.map(Pair::getLeft)
            .orElse(Function.identity());

    Schema schema = schemaEvolutionTransformerOpt.map(Pair::getRight)
        .orElseGet(dataBlock::getSchema);

    return Pair.of(new CloseableMappingIterator<>(blockRecordsIterator, transformer), schema);
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
  protected Option<Pair<Function<T,T>, Schema>> composeEvolvedSchemaTransformer(
      HoodieDataBlock dataBlock) {
    if (internalSchema.isEmptySchema()) {
      return Option.empty();
    }

    long currentInstantTime = Long.parseLong(dataBlock.getLogBlockHeader().get(INSTANT_TIME));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(currentInstantTime,
        hoodieTableMetaClient, false);
    Pair<InternalSchema, Map<String,String>> mergedInternalSchema = new InternalSchemaMerger(fileSchema, internalSchema,
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

    Option<Pair<HoodieRecord, Schema>> mergedRecord;
    if (enablePartialMerging) {
      mergedRecord = recordMerger.partialMerge(
          readerContext.constructHoodieRecord(older, olderInfoMap), (Schema) olderInfoMap.get(INTERNAL_META_SCHEMA),
          readerContext.constructHoodieRecord(newer, newerInfoMap), (Schema) newerInfoMap.get(INTERNAL_META_SCHEMA),
          readerSchema, payloadProps);
    } else {
      mergedRecord = recordMerger.merge(
          readerContext.constructHoodieRecord(older, olderInfoMap), (Schema) olderInfoMap.get(INTERNAL_META_SCHEMA),
          readerContext.constructHoodieRecord(newer, newerInfoMap), (Schema) newerInfoMap.get(INTERNAL_META_SCHEMA), payloadProps);
    }

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
