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

package org.apache.hudi.io.cdc;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCOperation;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;

import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;

/**
 * Writes CDC records as native CDC log files from Avro input records.
 */
public class HoodieAvroNativeCDCLogger implements HoodieCDCLogWriter<IndexedRecord> {

  private final String commitTime;
  private final HoodieSchema dataSchema;
  private final HoodieSchema cdcSchema;
  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;
  private final CDCTransformer transformer;
  private final HoodieNativeCDCFileWriter<IndexedRecord> nativeCDCFileWriter;
  private PendingCDCRecord pendingRecord;

  public HoodieAvroNativeCDCLogger(
      String commitTime,
      HoodieWriteConfig config,
      HoodieTableConfig tableConfig,
      String partitionPath,
      HoodieStorage storage,
      HoodieSchema schema,
      StoragePath parentPath,
      String fileId,
      String writeToken,
      LogFileCreationCallback fileCreationCallback,
      TaskContextSupplier taskContextSupplier) {
    this.commitTime = commitTime;
    this.dataSchema = HoodieSchemaCache.intern(HoodieSchemaUtils.removeMetadataFields(schema));
    this.cdcSupplementalLoggingMode = tableConfig.cdcSupplementalLoggingMode();
    this.cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(cdcSupplementalLoggingMode, dataSchema);
    this.transformer = getTransformer();
    this.nativeCDCFileWriter = new HoodieNativeCDCFileWriter<>(
        commitTime,
        partitionPath,
        storage,
        config,
        cdcSchema,
        tableConfig.getBaseFileFormat(),
        parentPath,
        fileId,
        writeToken,
        fileCreationCallback,
        taskContextSupplier,
        HoodieRecord.HoodieRecordType.AVRO);
  }

  @Override
  public void put(String recordKey, IndexedRecord oldRecord, Option<IndexedRecord> newRecord) {
    GenericData.Record cdcRecord;
    if (newRecord.isPresent()) {
      if (oldRecord == null) {
        cdcRecord = transformer.transform(HoodieCDCOperation.INSERT, recordKey, null, (GenericRecord) newRecord.get());
      } else {
        cdcRecord = transformer.transform(HoodieCDCOperation.UPDATE, recordKey, (GenericRecord) oldRecord, (GenericRecord) newRecord.get());
      }
    } else {
      cdcRecord = transformer.transform(HoodieCDCOperation.DELETE, recordKey, (GenericRecord) oldRecord, null);
    }

    flushPendingRecord();
    pendingRecord = new PendingCDCRecord(recordKey, cdcRecord);
  }

  @Override
  public void remove(String recordKey) {
    if (pendingRecord != null && pendingRecord.recordKey.equals(recordKey)) {
      pendingRecord = null;
    }
  }

  @Override
  public Map<String, Long> getCDCWriteStats() {
    return nativeCDCFileWriter.getCDCWriteStats();
  }

  @Override
  public void close() {
    try {
      flushPendingRecord();
      nativeCDCFileWriter.close();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close HoodieAvroNativeCDCLogger", e);
    }
  }

  private void flushPendingRecord() {
    if (pendingRecord == null) {
      return;
    }
    try {
      nativeCDCFileWriter.write(pendingRecord.recordKey, pendingRecord.record);
      pendingRecord = null;
    } catch (IOException e) {
      throw new HoodieException("Failed to write the cdc data to native cdc log file", e);
    }
  }

  private CDCTransformer getTransformer() {
    if (cdcSupplementalLoggingMode == DATA_BEFORE_AFTER) {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), commitTime, removeCommitMetadata(oldRecord), removeCommitMetadata(newRecord));
    } else if (cdcSupplementalLoggingMode == DATA_BEFORE) {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), recordKey, removeCommitMetadata(oldRecord));
    } else {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), recordKey);
    }
  }

  private GenericRecord removeCommitMetadata(GenericRecord record) {
    return record == null ? null : HoodieAvroUtils.projectRecordToNewSchemaShallow(record, dataSchema.getAvroSchema());
  }

  private static class PendingCDCRecord {
    private final String recordKey;
    private final IndexedRecord record;

    private PendingCDCRecord(String recordKey, IndexedRecord record) {
      this.recordKey = recordKey;
      this.record = record;
    }
  }

  /**
   * A transformer that transforms normal Avro records into CDC records.
   */
  private interface CDCTransformer {
    GenericData.Record transform(HoodieCDCOperation operation,
                                 String recordKey,
                                 GenericRecord oldRecord,
                                 GenericRecord newRecord);
  }
}
