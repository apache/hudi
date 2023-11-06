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
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieValidationException;

import org.apache.avro.Schema;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_SCHEMA;

public abstract class HoodieBaseFileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {
  protected final HoodieReaderContext<T> readerContext;
  protected final Schema readerSchema;
  protected final Schema baseFileSchema;
  protected final Option<String> partitionNameOverrideOpt;
  protected final Option<String[]> partitionPathFieldOpt;
  protected final HoodieRecordMerger recordMerger;
  protected final TypedProperties payloadProps;
  protected final Map<Object, Pair<Option<T>, Map<String, Object>>> records;
  protected ClosableIterator<T> baseFileIterator;
  protected Iterator<Pair<Option<T>, Map<String, Object>>> logRecordIterator;
  protected T nextRecord;
  protected boolean enablePartialMerging = false;

  public HoodieBaseFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                         Schema readerSchema,
                                         Schema baseFileSchema,
                                         Option<String> partitionNameOverrideOpt,
                                         Option<String[]> partitionPathFieldOpt,
                                         HoodieRecordMerger recordMerger,
                                         TypedProperties payloadProps) {
    this.readerContext = readerContext;
    this.readerSchema = readerSchema;
    this.baseFileSchema = baseFileSchema;
    this.partitionNameOverrideOpt = partitionNameOverrideOpt;
    this.partitionPathFieldOpt = partitionPathFieldOpt;
    this.recordMerger = recordMerger;
    this.payloadProps = payloadProps;
    this.records = new HashMap<>();
  }

  @Override
  public void setBaseFileIteraotr(ClosableIterator<T> baseFileIterator) {
    this.baseFileIterator = baseFileIterator;
  }

  @Override
  public T next() {
    return nextRecord;
  }

  @Override
  public Map<Object, Pair<Option<T>, Map<String, Object>>> getLogRecords() {
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
  protected Option<T> doProcessNextDataRecord(T record,
                                              Map<String, Object> metadata,
                                              Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair) throws IOException {
    if (existingRecordMetadataPair != null) {
      // Merge and store the combined record
      // Note that the incoming `record` is from an older commit, so it should be put as
      // the `older` in the merge API
      HoodieRecord<T> combinedRecord;
      if (enablePartialMerging) {
        combinedRecord = (HoodieRecord<T>) recordMerger.partialMerge(
            readerContext.constructHoodieRecord(Option.of(record), metadata),
            (Schema) metadata.get(INTERNAL_META_SCHEMA),
            readerContext.constructHoodieRecord(
                existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight()),
            (Schema) existingRecordMetadataPair.getRight().get(INTERNAL_META_SCHEMA),
            readerSchema,
            payloadProps).get().getLeft();
      } else {
        combinedRecord = (HoodieRecord<T>) recordMerger.merge(
            readerContext.constructHoodieRecord(Option.of(record), metadata),
            (Schema) metadata.get(INTERNAL_META_SCHEMA),
            readerContext.constructHoodieRecord(
                existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight()),
            (Schema) existingRecordMetadataPair.getRight().get(INTERNAL_META_SCHEMA),
            payloadProps).get().getLeft();
      }
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecordMetadataPair.getLeft().get()) {
        return Option.of(combinedRecord.getData());
      }
      return Option.empty();
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      return Option.of(record);
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
   * @throws IOException
   */
  protected Pair<ClosableIterator<T>, Schema> getRecordsIterator(
      HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    ClosableIterator<T> blockRecordsIterator;
    if (keySpecOpt.isPresent()) {
      KeySpec keySpec = keySpecOpt.get();
      blockRecordsIterator = dataBlock.getEngineRecordIterator(readerContext, keySpec.getKeys(), keySpec.isFullKey());
    } else {
      blockRecordsIterator = dataBlock.getEngineRecordIterator(readerContext);
    }
    return Pair.of(blockRecordsIterator, dataBlock.getSchema());
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
    if (mergedRecord.isPresent()) {
      return Option.ofNullable((T) mergedRecord.get().getLeft().getData());
    }
    return Option.empty();
  }

  /**
   * Filter a record for downstream processing when:
   *  1. A set of pre-specified keys exists.
   *  2. The key of the record is not contained in the set.
   */
  protected boolean shouldSkip(T record, String keyFieldName, boolean isFullKey, Set<String> keys) {
    String recordKey = readerContext.getValue(record, readerSchema, keyFieldName).toString();
    // Can not extract the record key, throw.
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("Can not extract the key for a record");
    }

    // No keys are specified. Cannot skip at all.
    if (keys.isEmpty()) {
      return false;
    }

    // When the record key matches with one of the keys or key prefixes, can not skip.
    if ((isFullKey && keys.contains(recordKey))
        || (!isFullKey && keys.stream().anyMatch(recordKey::startsWith))) {
      return false;
    }

    // Otherwise, this record is not needed.
    return true;
  }

  /**
   * Extract the record positions from a log block header.
   *
   * @param logBlock
   * @return
   * @throws IOException
   */
  protected static List<Long> extractRecordPositions(HoodieLogBlock logBlock) throws IOException {
    List<Long> blockPositions = new ArrayList<>();

    Roaring64NavigableMap positions = logBlock.getRecordPositions();
    if (positions == null || positions.isEmpty()) {
      throw new HoodieValidationException("No record position info is found when attempt to do position based merge.");
    }

    Iterator<Long> iterator = positions.iterator();
    while (iterator.hasNext()) {
      blockPositions.add(iterator.next());
    }

    if (blockPositions.isEmpty()) {
      throw new HoodieCorruptedDataException("No positions are extracted.");
    }

    return blockPositions;
  }
}
