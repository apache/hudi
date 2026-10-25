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

import org.apache.hudi.common.engine.RecordContext;
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
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.Map;

import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;

/**
 * Writes CDC records as native CDC log files, for example {@code .cdc.parquet}.
 */
public class HoodieNativeCDCLogger<T> implements HoodieCDCLogWriter<BufferedRecord<T>> {

  /** Instant time used for naming and writing native CDC log files. */
  private final String commitTime;
  /** Table data schema without Hudi metadata fields. */
  private final HoodieSchema dataSchema;
  /** CDC record schema derived from the table CDC supplemental logging mode. */
  private final HoodieSchema cdcSchema;
  /** Cached encoded CDC schema id reused for every buffered CDC record. */
  private final Integer cdcSchemaId;
  /** Supplemental logging mode determining which fields are emitted in CDC records. */
  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;
  /** Engine-specific record context for row construction, projection, and conversion. */
  private final RecordContext<T> recordContext;
  /** Manages native CDC file creation, rolling, writing, and stats. */
  private final HoodieNativeCDCFileWriter<T> nativeCDCFileWriter;
  /** Last CDC record staged for write so it can be retracted if the merge later fails. */
  private PendingCDCRecord<T> pendingRecord;

  public HoodieNativeCDCLogger(
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
      TaskContextSupplier taskContextSupplier,
      RecordContext<T> recordContext,
      HoodieRecord.HoodieRecordType recordType) {
    this.commitTime = commitTime;
    this.dataSchema = HoodieSchemaCache.intern(HoodieSchemaUtils.removeMetadataFields(schema));
    this.cdcSupplementalLoggingMode = tableConfig.cdcSupplementalLoggingMode();
    this.cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(cdcSupplementalLoggingMode, dataSchema);
    this.recordContext = recordContext;
    this.cdcSchemaId = recordContext.encodeSchema(cdcSchema);
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
        recordType);
  }

  @Override
  public void put(String recordKey, BufferedRecord<T> oldRecord, Option<BufferedRecord<T>> newRecord) {
    flushPendingRecord();
    HoodieCDCOperation operation;
    if (newRecord.isPresent()) {
      operation = oldRecord == null ? HoodieCDCOperation.INSERT : HoodieCDCOperation.UPDATE;
    } else {
      operation = HoodieCDCOperation.DELETE;
    }
    this.pendingRecord = new PendingCDCRecord<>(recordKey, createCDCRecord(recordKey, operation, oldRecord, newRecord.orElse(null)));
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
      throw new HoodieIOException("Failed to close HoodieNativeCDCLogger", e);
    }
  }

  private BufferedRecord<T> createCDCRecord(
      String recordKey,
      HoodieCDCOperation operation,
      BufferedRecord<T> oldRecord,
      BufferedRecord<T> newRecord) {
    Object[] fieldValues = new Object[cdcSchema.getFields().size()];
    if (cdcSupplementalLoggingMode == DATA_BEFORE_AFTER) {
      fieldValues[0] = convertString(operation.getValue());
      fieldValues[1] = convertString(commitTime);
      fieldValues[2] = projectDataRecord(oldRecord);
      fieldValues[3] = projectDataRecord(newRecord);
    } else if (cdcSupplementalLoggingMode == DATA_BEFORE) {
      fieldValues[0] = convertString(operation.getValue());
      fieldValues[1] = convertString(recordKey);
      fieldValues[2] = projectDataRecord(oldRecord);
    } else {
      fieldValues[0] = convertString(operation.getValue());
      fieldValues[1] = convertString(recordKey);
    }
    T cdcRecord = recordContext.constructEngineRecord(cdcSchema, fieldValues);
    return new BufferedRecord<>(recordKey, null, cdcRecord, cdcSchemaId, null);
  }

  private Object convertString(String value) {
    return recordContext.convertValueToEngineType(value);
  }

  private T projectDataRecord(BufferedRecord<T> record) {
    if (record == null || record.getRecord() == null) {
      return null;
    }
    HoodieSchema recordSchema = recordContext.getSchemaFromBufferRecord(record);
    T dataRecord = record.getRecord();
    if (needsProjection(recordSchema)) {
      dataRecord = recordContext.projectRecord(recordSchema, dataSchema).apply(dataRecord);
      recordSchema = dataSchema;
    }
    return recordContext.seal(recordSchema, dataRecord);
  }

  private boolean needsProjection(HoodieSchema recordSchema) {
    return recordSchema != null
        && (recordSchema.getFields().size() != dataSchema.getFields().size() || !recordSchema.equals(dataSchema));
  }

  private void flushPendingRecord() {
    if (pendingRecord == null) {
      return;
    }
    try {
      // Write the engine-native CDC record directly; wrapping it into a payload-based HoodieRecord
      // would let the payload (e.g., ExpressionPayload) re-interpret the CDC record with a wrong schema.
      nativeCDCFileWriter.write(pendingRecord.recordKey, pendingRecord.record.getRecord());
      pendingRecord = null;
    } catch (IOException e) {
      throw new HoodieException("Failed to write the cdc data to native cdc log file", e);
    }
  }

  private static class PendingCDCRecord<T> {
    private final String recordKey;
    private final BufferedRecord<T> record;

    private PendingCDCRecord(String recordKey, BufferedRecord<T> record) {
      this.recordKey = recordKey;
      this.record = record;
    }
  }
}
