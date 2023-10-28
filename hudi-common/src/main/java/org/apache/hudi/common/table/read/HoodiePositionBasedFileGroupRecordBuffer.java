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
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into record position based map.
 * Here the position means that record position in the base file. The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class HoodiePositionBasedFileGroupRecordBuffer<T> extends HoodieBaseFileGroupRecordBuffer<T> {
  private long nextRecordPosition = 0L;

  public HoodiePositionBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                                  Schema readerSchema,
                                                  Schema baseFileSchema,
                                                  Option<String> partitionNameOverrideOpt,
                                                  Option<String[]> partitionPathFieldOpt,
                                                  HoodieRecordMerger recordMerger,
                                                  TypedProperties payloadProps) {
    super(readerContext, readerSchema, baseFileSchema, partitionNameOverrideOpt, partitionPathFieldOpt,
        recordMerger, payloadProps);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.POSITION_BASED;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    // Prepare key filters.
    Set<String> keys = new HashSet<>();
    boolean isFullKey = true;
    if (keySpecOpt.isPresent()) {
      if (!keySpecOpt.get().getKeys().isEmpty()) {
        keys = new HashSet<>(keySpecOpt.get().getKeys());
      }
      isFullKey = keySpecOpt.get().isFullKey();
    }

    if (dataBlock.containsPartialUpdates()) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
    }
    
    // Extract positions from data block.
    List<Long> recordPositions = extractRecordPositions(dataBlock);

    // TODO: return an iterator that can generate sequence number with the record.
    //  Then we can hide this logic into data block.
    try (ClosableIterator<T> recordIterator = dataBlock.getEngineRecordIterator(readerContext)) {
      int recordIndex = 0;
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();

        // Skip a record if it is not contained in the specified keys.
        if (shouldSkip(nextRecord, dataBlock.getKeyFieldName(), isFullKey, keys)) {
          recordIndex++;
          continue;
        }

        long recordPosition = recordPositions.get(recordIndex++);
        processNextDataRecord(
            nextRecord,
            readerContext.generateMetadataForRecord(nextRecord, readerSchema),
            recordPosition
        );
      }
    }
  }

  @Override
  public void processNextDataRecord(T record, Map<String, Object> metadata, Object recordPosition) throws IOException {
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordPosition);
    Option<T> mergedRecord = doProcessNextDataRecord(record, metadata, existingRecordMetadataPair);
    if (mergedRecord.isPresent()) {
      records.put(recordPosition, Pair.of(Option.ofNullable(readerContext.seal(mergedRecord.get())), metadata));
    }
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    List<Long> recordPositions = extractRecordPositions(deleteBlock);

    int recordIndex = 0;
    Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
    while (it.hasNext()) {
      DeleteRecord record = it.next();
      long recordPosition = recordPositions.get(recordIndex++);
      processNextDeletedRecord(record, recordPosition);
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Object recordPosition) {
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordPosition);
    Option<DeleteRecord> recordOpt = doProcessNextDeletedRecord(deleteRecord, existingRecordMetadataPair);
    if (recordOpt.isPresent()) {
      String recordKey = recordOpt.get().getRecordKey();
      records.put(recordPosition, Pair.of(Option.empty(), readerContext.generateMetadataForRecord(
          recordKey, recordOpt.get().getPartitionPath(), recordOpt.get().getOrderingValue())));
    }
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.values().stream()
        .filter(r -> r.getLeft().isPresent())
        .map(r -> readerContext.getRecordKey(r.getKey().get(), readerSchema)).anyMatch(recordKey::equals);
  }

  @Override
  public boolean hasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      T baseRecord = baseFileIterator.next();
      Pair<Option<T>, Map<String, Object>> logRecordInfo = records.remove(nextRecordPosition++);

      Map<String, Object> metadata = readerContext.generateMetadataForRecord(
          baseRecord, baseFileSchema);

      Option<T> resultRecord = logRecordInfo != null
          ? merge(Option.of(baseRecord), metadata, logRecordInfo.getLeft(), logRecordInfo.getRight())
          : merge(Option.empty(), Collections.emptyMap(), Option.of(baseRecord), metadata);
      if (resultRecord.isPresent()) {
        nextRecord = readerContext.seal(resultRecord.get());
        return true;
      }
    }

    // Handle records solely from log files.
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
}
