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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;

/**
 * Writes CDC records as native CDC log files from Avro input records.
 */
public class HoodieAvroNativeCDCLogger implements HoodieCDCLogWriter<IndexedRecord> {

  private final String commitTime;
  private final String partitionPath;
  private final HoodieStorage storage;
  private final HoodieWriteConfig config;
  private final HoodieSchema dataSchema;
  private final HoodieSchema cdcSchema;
  private final HoodieFileFormat nativeFileFormat;
  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;
  private final StoragePath parentPath;
  private final String fileId;
  private final String writeToken;
  private final LogFileCreationCallback fileCreationCallback;
  private final TaskContextSupplier taskContextSupplier;
  private final CDCTransformer transformer;
  private final Properties recordProperties;
  private final List<StoragePath> cdcAbsPaths;
  private int nextLogVersion;
  private HoodieFileWriter cdcWriter;
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
    this.partitionPath = partitionPath;
    this.storage = storage;
    this.config = config;
    this.dataSchema = HoodieSchemaCache.intern(HoodieSchemaUtils.removeMetadataFields(schema));
    this.cdcSupplementalLoggingMode = tableConfig.cdcSupplementalLoggingMode();
    this.cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(cdcSupplementalLoggingMode, dataSchema);
    this.nativeFileFormat = tableConfig.getBaseFileFormat();
    this.parentPath = parentPath;
    this.fileId = fileId;
    this.writeToken = writeToken;
    this.fileCreationCallback = fileCreationCallback;
    this.taskContextSupplier = taskContextSupplier;
    this.transformer = getTransformer();
    this.recordProperties = new Properties();
    this.recordProperties.putAll(config.getProps());
    this.cdcAbsPaths = new ArrayList<>();
    this.nextLogVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
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
    Map<String, Long> stats = new HashMap<>();
    try {
      for (StoragePath cdcAbsPath : cdcAbsPaths) {
        String cdcFileName = cdcAbsPath.getName();
        String cdcPath = StringUtils.isNullOrEmpty(partitionPath) ? cdcFileName : partitionPath + "/" + cdcFileName;
        stats.put(cdcPath, storage.getPathInfo(cdcAbsPath).getLength());
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to get cdc write stat", e);
    }
    return stats;
  }

  @Override
  public void close() {
    try {
      flushPendingRecord();
      closeCDCWriter();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close HoodieAvroNativeCDCLogger", e);
    }
  }

  private void flushPendingRecord() {
    if (pendingRecord == null) {
      return;
    }
    try {
      ensureCDCWriter();
      cdcWriter.write(
          pendingRecord.recordKey,
          new HoodieAvroIndexedRecord(new HoodieKey(pendingRecord.recordKey, partitionPath), pendingRecord.record),
          cdcSchema,
          recordProperties);
      pendingRecord = null;
    } catch (IOException e) {
      throw new HoodieException("Failed to write the cdc data to native cdc log file", e);
    }
  }

  private void ensureCDCWriter() throws IOException {
    if (cdcWriter != null && cdcWriter.canWrite()) {
      return;
    }
    closeCDCWriter();
    HoodieLogFile cdcLogFile = createNativeCDCLogFile();
    cdcWriter = HoodieFileWriterFactory.getFileWriter(
        commitTime, cdcLogFile.getPath(), storage, config, cdcSchema, taskContextSupplier, HoodieRecord.HoodieRecordType.AVRO);
    cdcAbsPaths.add(cdcLogFile.getPath());
  }

  private void closeCDCWriter() throws IOException {
    if (cdcWriter != null) {
      cdcWriter.close();
      cdcWriter = null;
    }
  }

  private HoodieLogFile createNativeCDCLogFile() throws IOException {
    int version = nextAvailableVersion();
    HoodieLogFile nativeCDCLogFile = new HoodieLogFile(makeNativeCDCLogPath(version), 0);
    fileCreationCallback.preFileCreation(nativeCDCLogFile);
    nextLogVersion = version + 1;
    return nativeCDCLogFile;
  }

  private int nextAvailableVersion() throws IOException {
    int candidateVersion = nextLogVersion;
    while (storage.exists(makeNativeCDCLogPath(candidateVersion))) {
      candidateVersion++;
    }
    return candidateVersion;
  }

  private StoragePath makeNativeCDCLogPath(int version) {
    return new StoragePath(parentPath, FSUtils.makeNativeLogFileName(
        fileId, writeToken, commitTime, version, HoodieCDCUtils.CDC_LOGFILE_SUFFIX, nativeFileFormat));
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
