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
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_RECORD_KEY;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into a record key based map.
 * The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class KeyBasedFileGroupRecordBuffer<T> extends FileGroupRecordBuffer<T> {

  public KeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                       HoodieTableMetaClient hoodieTableMetaClient,
                                       RecordMergeMode recordMergeMode,
                                       Option<String> partitionNameOverrideOpt,
                                       Option<String[]> partitionPathFieldOpt,
                                       TypedProperties props,
                                       HoodieReadStats readStats) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partitionNameOverrideOpt, partitionPathFieldOpt, props, readStats);
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

    Schema schema = AvroSchemaCache.intern(recordsIteratorSchemaPair.getRight());

    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        Map<String, Object> metadata = readerContext.generateMetadataForRecord(
            nextRecord, schema);
        String recordKey = (String) metadata.get(HoodieReaderContext.INTERNAL_META_RECORD_KEY);

        if (isBuiltInDeleteRecord(nextRecord) || isCustomDeleteRecord(nextRecord)) {
          processDeleteRecord(nextRecord, metadata);
        } else {
          processNextDataRecord(nextRecord, metadata, recordKey);
        }
      }
    }
  }

  @Override
  public void processNextDataRecord(T record, Map<String, Object> metadata, Serializable recordKey) throws IOException {
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordKey);
    Option<Pair<Option<T>, Map<String, Object>>> mergedRecordAndMetadata =
        doProcessNextDataRecord(record, metadata, existingRecordMetadataPair);

    if (mergedRecordAndMetadata.isPresent()) {
      records.put(recordKey, Pair.of(
          mergedRecordAndMetadata.get().getLeft().isPresent()
              ? Option.ofNullable(readerContext.seal(mergedRecordAndMetadata.get().getLeft().get()))
              : Option.empty(),
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
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordKey);
    Option<DeleteRecord> recordOpt = doProcessNextDeletedRecord(deleteRecord, existingRecordMetadataPair);
    if (recordOpt.isPresent()) {
      records.put(recordKey, Pair.of(Option.empty(), readerContext.generateMetadataForRecord(
          (String) recordKey, recordOpt.get().getPartitionPath(),
          getOrderingValue(readerContext, recordOpt.get()))));
    }
  }

  protected void processDeleteRecord(T record, Map<String, Object> metadata) {
    DeleteRecord deleteRecord = DeleteRecord.create(
        new HoodieKey(
            (String) metadata.get(INTERNAL_META_RECORD_KEY),
            // The partition path of the delete record is set to null because it is not
            // used, and the delete record is never surfaced from the file group reader
            null),
        readerContext.getOrderingValue(
            Option.of(record), metadata, readerSchema, orderingFieldName));
    processNextDeletedRecord(
        deleteRecord,
        (String) metadata.get(INTERNAL_META_RECORD_KEY));
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordKey(baseRecord, readerSchema);
    Pair<Option<T>, Map<String, Object>> logRecordInfo = records.remove(recordKey);
    return hasNextBaseRecord(baseRecord, logRecordInfo);
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
}
