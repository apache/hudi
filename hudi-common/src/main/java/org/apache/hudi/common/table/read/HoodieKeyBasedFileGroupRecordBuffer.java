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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into a record key based map.
 * The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class HoodieKeyBasedFileGroupRecordBuffer<T> extends HoodieBaseFileGroupRecordBuffer<T> {

  public HoodieKeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                             HoodieTableMetaClient hoodieTableMetaClient,
                                             Option<String> partitionNameOverrideOpt,
                                             Option<String[]> partitionPathFieldOpt,
                                             HoodieRecordMerger recordMerger,
                                             TypedProperties payloadProps,
                                             long maxMemorySizeInBytes,
                                             String spillableMapBasePath,
                                             ExternalSpillableMap.DiskMapType diskMapType,
                                             boolean isBitCaskDiskMapCompressionEnabled) {
    super(readerContext, hoodieTableMetaClient, partitionNameOverrideOpt, partitionPathFieldOpt,
        recordMerger, payloadProps, maxMemorySizeInBytes, spillableMapBasePath, diskMapType, isBitCaskDiskMapCompressionEnabled);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    Pair<ClosableIterator<T>, Schema> recordsIteratorSchemaPair =
        getRecordsIterator(dataBlock, keySpecOpt);
    if (dataBlock.containsPartialUpdates()) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
    }

    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        Map<String, Object> metadata = readerContext.generateMetadataForRecord(
            nextRecord, recordsIteratorSchemaPair.getRight());
        String recordKey = (String) metadata.get(HoodieReaderContext.INTERNAL_META_RECORD_KEY);
        processNextDataRecord(nextRecord, metadata, recordKey);
      }
    }
  }

  @Override
  public void processNextDataRecord(T record, Map<String, Object> metadata, Serializable recordKey) throws IOException {
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordKey);
    Option<Pair<T, Map<String, Object>>> mergedRecordAndMetadata =
        doProcessNextDataRecord(record, metadata, existingRecordMetadataPair);
    if (mergedRecordAndMetadata.isPresent()) {
      records.put(recordKey, Pair.of(
          Option.ofNullable(readerContext.seal(mergedRecordAndMetadata.get().getLeft())),
          mergedRecordAndMetadata.get().getRight()));
    }
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
    while (it.hasNext()) {
      DeleteRecord record = it.next();
      String recordKey = record.getRecordKey();
      processNextDeletedRecord(record, recordKey);
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordKey) {
    records.put(recordKey, Pair.of(Option.empty(), readerContext.generateMetadataForRecord(
        (String) recordKey, deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue())));
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      T baseRecord = baseFileIterator.next();
      if (doHasNextMerge(baseRecord)) {
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

  protected boolean doHasNextMerge(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordKey(baseRecord, readerSchema);
    Pair<Option<T>, Map<String, Object>> logRecordInfo = records.remove(recordKey);
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
}