/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.SerializableRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCOperation;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HoodieCDCLogger<T extends HoodieRecordPayload> implements Closeable {

  private final String partitionPath;

  private final String fileName;

  private final String commitTime;

  private final List<String> keyFields;

  private final int taskPartitionId;

  private final boolean populateMetaFields;

  // writer for cdc data
  private final HoodieLogFormat.Writer cdcWriter;

  private final boolean cdcEnabled;

  private final String cdcSupplementalLoggingMode;

  // the cdc data
  private final Map<String, SerializableRecord> cdcData;

  private final Function<GenericRecord, GenericRecord> rewriteRecordFunc;

  // the count of records currently being written, used to generate the same seqno for the cdc data
  private final AtomicLong writtenRecordCount = new AtomicLong(-1);

  public HoodieCDCLogger(
      String partitionPath,
      String fileName,
      String commitTime,
      HoodieWriteConfig config,
      List<String> keyFields,
      int taskPartitionId,
      HoodieLogFormat.Writer cdcWriter,
      long maxInMemorySizeInBytes,
      Function<GenericRecord, GenericRecord> rewriteRecordFunc) {
    try {
      this.partitionPath = partitionPath;
      this.fileName = fileName;
      this.commitTime = commitTime;
      this.keyFields = keyFields;
      this.taskPartitionId = taskPartitionId;
      this.populateMetaFields = config.populateMetaFields();
      this.cdcWriter = cdcWriter;
      this.rewriteRecordFunc = rewriteRecordFunc;

      this.cdcEnabled = config.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED);
      this.cdcSupplementalLoggingMode = config.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE);
      this.cdcData = new ExternalSpillableMap<>(
          maxInMemorySizeInBytes,
          config.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled()
      );
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to initialize HoodieCDCLogger", e);
    }
  }

  public void put(HoodieRecord<T> hoodieRecord, GenericRecord oldRecord, Option<IndexedRecord> indexedRecord) {
    if (cdcEnabled) {
      String recordKey;
      if (oldRecord == null) {
        recordKey = hoodieRecord.getRecordKey();
      } else {
        recordKey = StringUtils.join(
            keyFields.stream().map(keyField -> oldRecord.get(keyField).toString()).toArray(String[]::new),
          ":");
      }
      if (indexedRecord.isPresent()) {
        GenericRecord record = (GenericRecord) indexedRecord.get();
        if (oldRecord == null) {
          // inserted cdc record
          cdcData.put(recordKey, createCDCRecord(HoodieCDCOperation.INSERT, recordKey, partitionPath,
              null, record));
        } else {
          // updated cdc record
          cdcData.put(recordKey, createCDCRecord(HoodieCDCOperation.UPDATE, recordKey, partitionPath,
              oldRecord, record));
        }
      } else {
        // deleted cdc record
        cdcData.put(recordKey, createCDCRecord(HoodieCDCOperation.DELETE, recordKey, partitionPath,
            oldRecord, null));
      }
    }
  }

  private SerializableRecord createCDCRecord(HoodieCDCOperation operation,
                                               String recordKey,
                                               String partitionPath,
                                               GenericRecord oldRecord,
                                               GenericRecord newRecord) {
    GenericData.Record record;
    if (cdcSupplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE_AFTER)) {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), commitTime,
          oldRecord, addCommitMetadata(newRecord, recordKey, partitionPath));
    } else if (cdcSupplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE)) {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), recordKey, oldRecord);
    } else {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), recordKey);
    }
    return new SerializableRecord(record);
  }

  private GenericRecord addCommitMetadata(GenericRecord record, String recordKey, String partitionPath) {
    if (record != null && populateMetaFields) {
      GenericRecord rewriteRecord = rewriteRecordFunc.apply(record);
      String seqId = HoodieRecord.generateSequenceId(commitTime, taskPartitionId, writtenRecordCount.get());
      HoodieAvroUtils.addCommitMetadataToRecord(rewriteRecord, commitTime, seqId);
      HoodieAvroUtils.addHoodieKeyToRecord(rewriteRecord, recordKey, partitionPath, fileName);
      return rewriteRecord;
    }
    return record;
  }

  public long getAndIncrement() {
    return writtenRecordCount.getAndIncrement();
  }

  public boolean isEmpty() {
    return !this.cdcEnabled || this.cdcData.isEmpty();
  }

  public Option<AppendResult> writeCDCData() {
    if (isEmpty()) {
      return Option.empty();
    }
    try {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = buildCDCBlockHeader();
      List<IndexedRecord> records = cdcData.values().stream()
          .map(SerializableRecord::getRecord).collect(Collectors.toList());
      HoodieLogBlock block = new HoodieCDCDataBlock(records, header,
          StringUtils.join(keyFields.toArray(new String[0]), ","));
      AppendResult result = cdcWriter.appendBlocks(Collections.singletonList(block));

      // call close to trigger the data flush.
      this.close();

      return Option.of(result);
    } catch (Exception e) {
      throw new HoodieException("Failed to write the cdc data to " + cdcWriter.getLogFile().getPath(), e);
    }
  }

  private Map<HoodieLogBlock.HeaderMetadataType, String> buildCDCBlockHeader() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, commitTime);
    if (cdcSupplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE_AFTER)) {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_STRING);
    } else if (cdcSupplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE)) {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_OP_RECORDKEY_BEFORE_STRING);
    } else {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_OP_AND_RECORDKEY_STRING);
    }
    return header;
  }

  @Override
  public void close() {
    if (cdcWriter != null) {
      try {
        cdcWriter.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close HoodieCDCLogger", e);
      }
    }
    cdcData.clear();
  }
}
