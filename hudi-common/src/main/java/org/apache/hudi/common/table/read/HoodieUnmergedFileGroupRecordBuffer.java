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
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class HoodieUnmergedFileGroupRecordBuffer<T> extends HoodieBaseFileGroupRecordBuffer<T> {
  // Used to order the records in the record map.
  private Long putIndex = 0L;
  private Long getIndex = 0L;

  public HoodieUnmergedFileGroupRecordBuffer(
      HoodieReaderContext<T> readerContext,
      HoodieTableMetaClient hoodieTableMetaClient,
      Option<String> partitionNameOverrideOpt,
      Option<String[]> partitionPathFieldOpt,
      HoodieRecordMerger recordMerger,
      TypedProperties payloadProps,
      long maxMemorySizeInBytes,
      String spillableMapBasePath,
      ExternalSpillableMap.DiskMapType diskMapType,
      boolean isBitCaskDiskMapCompressionEnabled) {
    super(readerContext, hoodieTableMetaClient, partitionNameOverrideOpt, partitionPathFieldOpt, recordMerger,
        payloadProps, maxMemorySizeInBytes, spillableMapBasePath, diskMapType, isBitCaskDiskMapCompressionEnabled);
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Output from base file first.
    if (baseFileIterator.hasNext()) {
      nextRecord = baseFileIterator.next();
      return true;
    }

    // Output records based on the index to preserve the order.
    if (!records.isEmpty()) {
      Pair<Option<T>, Map<String, Object>> nextRecordInfo = records.remove(getIndex++);

      if (nextRecordInfo == null) {
        throw new HoodieException("Row index should be continuous!");
      }

      if (nextRecordInfo.getLeft().isPresent()) {
        nextRecord = nextRecordInfo.getKey().get();
      } else {
        nextRecord = readerContext.constructRawDeleteRecord(nextRecordInfo.getRight());
      }
      return true;
    }

    return false;
  }

  @Override
  public Iterator<Pair<Option<T>, Map<String, Object>>> getLogRecordIterator() {
    return records.values().iterator();
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.UNMERGED;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    Pair<ClosableIterator<T>, Schema> recordsIteratorSchemaPair =
        getRecordsIterator(dataBlock, keySpecOpt);
    if (dataBlock.containsPartialUpdates()) {
      throw new HoodieException("Partial update is not supported for unmerged record read");
    }

    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        Map<String, Object> metadata = readerContext.generateMetadataForRecord(
            nextRecord, recordsIteratorSchemaPair.getRight());
        processNextDataRecord(nextRecord, metadata, putIndex++);
      }
    }
  }

  @Override
  public void processNextDataRecord(T record, Map<String, Object> metadata, Serializable index) {
    records.put(index, Pair.of(Option.ofNullable(readerContext.seal(record)), metadata));
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
    Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
    while (it.hasNext()) {
      DeleteRecord record = it.next();
      processNextDeletedRecord(record, putIndex++);
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable index) {
    records.put(index, Pair.of(Option.empty(), readerContext.generateMetadataForRecord(
        deleteRecord.getRecordKey(), deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue())));
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }
}