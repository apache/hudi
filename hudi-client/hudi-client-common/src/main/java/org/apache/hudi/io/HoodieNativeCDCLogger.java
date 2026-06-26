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

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
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
import org.apache.hudi.common.table.read.BufferedRecord;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;

/**
 * Writes CDC records as native CDC log files, for example {@code .cdc.parquet}.
 */
public class HoodieNativeCDCLogger<T> implements HoodieCDCLogWriter<BufferedRecord<T>> {

  /** Instant time used for naming and writing native CDC log files. */
  private final String commitTime;
  /** Partition path whose records are being merged. */
  private final String partitionPath;
  /** Storage instance used to create, probe, and size CDC log files. */
  private final HoodieStorage storage;
  /** Write config used to initialize native CDC file writers. */
  private final HoodieWriteConfig config;
  /** Table data schema without Hudi metadata fields. */
  private final HoodieSchema dataSchema;
  /** CDC record schema derived from the table CDC supplemental logging mode. */
  private final HoodieSchema cdcSchema;
  /** Native CDC file format from table config. */
  private final HoodieFileFormat nativeFileFormat;
  /** Cached encoded CDC schema id reused for every buffered CDC record. */
  private final Integer cdcSchemaId;
  /** Supplemental logging mode determining which fields are emitted in CDC records. */
  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;
  /** Parent storage path where native CDC log files are written. */
  private final StoragePath parentPath;
  /** File id of the data file group whose CDC records are being logged. */
  private final String fileId;
  /** Write token used in generated native CDC log file names. */
  private final String writeToken;
  /** Callback invoked before each native CDC log file is created. */
  private final LogFileCreationCallback fileCreationCallback;
  /** Task context supplier passed through to native file writer creation. */
  private final TaskContextSupplier taskContextSupplier;
  /** Engine-specific record context for row construction, projection, and conversion. */
  private final RecordContext<T> recordContext;
  /** Engine record type used by the native CDC file writer. */
  private final HoodieRecord.HoodieRecordType recordType;
  /** Properties passed to each CDC file writer record write. */
  private final Properties recordProperties;
  /** Absolute paths of native CDC log files created by this logger. */
  private final List<StoragePath> cdcAbsPaths;
  /** Next native CDC log file version candidate. */
  private int nextLogVersion;
  /** Currently open native CDC file writer, rolling when it can no longer accept writes. */
  private HoodieFileWriter cdcWriter;
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
    this.recordContext = recordContext;
    this.recordType = recordType;
    this.cdcSchemaId = recordContext.encodeSchema(cdcSchema);
    this.recordProperties = new Properties();
    this.recordProperties.putAll(config.getProps());
    this.cdcAbsPaths = new ArrayList<>();
    this.nextLogVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
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
    }
    return recordContext.seal(dataRecord);
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
      ensureCDCWriter();
      cdcWriter.write(
          pendingRecord.recordKey,
          recordContext.constructHoodieRecord(pendingRecord.record, partitionPath),
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
        commitTime, cdcLogFile.getPath(), storage, config, cdcSchema, taskContextSupplier, recordType);
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

  private static class PendingCDCRecord<T> {
    private final String recordKey;
    private final BufferedRecord<T> record;

    private PendingCDCRecord(String recordKey, BufferedRecord<T> record) {
      this.recordKey = recordKey;
      this.record = record;
    }
  }
}
