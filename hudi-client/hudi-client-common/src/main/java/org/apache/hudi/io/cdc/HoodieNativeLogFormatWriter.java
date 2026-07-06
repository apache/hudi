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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.LogReaderUtils;
import org.apache.hudi.common.table.log.NativeLogFooterMetadata;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.LogExtensions.DATA_LOG_EXTENSION;
import static org.apache.hudi.common.model.LogExtensions.DELETE_LOG_EXTENSION;

/**
 * Writes MOR log blocks as native files, for example {@code .log.parquet} and {@code .deletes.parquet}.
 */
public class HoodieNativeLogFormatWriter extends HoodieLogFormat.Writer {

  private final HoodieWriteConfig writeConfig;
  private final HoodieFileFormat nativeFileFormat;
  private final HoodieSchema tableSchema;
  private final TaskContextSupplier taskContextSupplier;
  private final RecordContext recordContext;
  private final List<String> orderingFieldNames;
  private final Properties recordProperties;
  private final Option<String> baseFileInstantTimeOfPositions;
  private HoodieFileWriter dataFileWriter;
  private HoodieFileWriter deleteFileWriter;
  private HoodieLogFile dataLogFile;
  private HoodieLogFile deleteLogFile;
  private HoodieSchema deleteLogSchema;
  private int currentAppendVersion = -1;
  private final List<Long> dataRecordPositions = new ArrayList<>();
  private final List<Long> deleteRecordPositions = new ArrayList<>();
  private List<AppendResult> lastAppendResults = new ArrayList<>();
  private Option<Object> lastDataFileFormatMetadata = Option.empty();

  public HoodieNativeLogFormatWriter(Integer bufferSize,
                                     HoodieStorage storage,
                                     StoragePath parentPath,
                                     String logFileId,
                                     String instantTime,
                                     Integer logVersion,
                                     String logWriteToken,
                                     Long sizeThreshold,
                                     LogFileCreationCallback fileCreationCallback,
                                     HoodieTableVersion tableVersion,
                                     HoodieWriteConfig writeConfig,
                                     HoodieFileFormat nativeFileFormat,
                                     HoodieSchema tableSchema,
                                     TaskContextSupplier taskContextSupplier,
                                     RecordContext recordContext,
                                     List<String> orderingFieldNames,
                                     Option<String> baseFileInstantTimeOfPositions) throws IOException {
    super(bufferSize, storage, parentPath, logFileId, DATA_LOG_EXTENSION, instantTime, logVersion, logWriteToken,
        null, 0L, sizeThreshold, fileCreationCallback, tableVersion);
    this.writeConfig = writeConfig;
    this.nativeFileFormat = nativeFileFormat;
    this.tableSchema = tableSchema;
    this.taskContextSupplier = taskContextSupplier;
    this.recordContext = recordContext;
    this.orderingFieldNames = orderingFieldNames;
    this.recordProperties = new Properties();
    this.recordProperties.putAll(writeConfig.getProps());
    this.logFile = new HoodieLogFile(makeNativeLogPath(this.logVersion, DATA_LOG_EXTENSION), this.fileSize);
    this.baseFileInstantTimeOfPositions = baseFileInstantTimeOfPositions;
  }

  public List<AppendResult> getLastAppendResults() {
    return lastAppendResults;
  }

  public Option<Object> getLastDataFileFormatMetadata() {
    return lastDataFileFormatMetadata;
  }

  @Override
  public AppendResult appendBlock(HoodieLogBlock block) {
    throw new UnsupportedOperationException("HoodieNativeLogFormatWriter writes native records directly.");
  }

  @Override
  public AppendResult appendBlocks(List<HoodieLogBlock> blocks) {
    throw new UnsupportedOperationException("HoodieNativeLogFormatWriter writes native records directly.");
  }

  @Override
  public long getCurrentSize() {
    return lastAppendResults.stream().mapToLong(AppendResult::size).sum();
  }

  @Override
  public void sync() {
    // Native log files are write-once; closing the file writer publishes the file.
  }

  @Override
  public void close() {
    try {
      closeFileWriters();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close native log file writers", e);
    }
  }

  public boolean hasPendingWrites() {
    return dataFileWriter != null || deleteFileWriter != null;
  }

  public boolean canWriteDataFile() {
    return dataFileWriter == null || dataFileWriter.canWrite();
  }

  public boolean canWriteDeleteFile() {
    return deleteFileWriter == null || deleteFileWriter.canWrite();
  }

  public void appendRecord(HoodieRecord record, HoodieSchema recordSchema, String keyFieldName) throws IOException {
    ensureDataFileWriter(recordSchema);
    dataFileWriter.write(record.getRecordKey(recordSchema, keyFieldName),
        record, recordSchema, recordProperties);
    dataRecordPositions.add(record.getCurrentPosition());
  }

  public void appendDeleteRecord(HoodieRecord record, HoodieSchema recordSchema, String keyFieldName) throws IOException {
    ensureDeleteFileWriter();
    String recordKey = record.getRecordKey(recordSchema, keyFieldName);
    Comparable orderingValue = getDeleteOrderingValue(record, recordSchema);
    Object deleteEngineRecord = recordContext.constructEngineRecord(
        deleteLogSchema, createDeleteLogFieldValues(recordKey, orderingValue));
    deleteFileWriter.writeRow(recordKey, deleteEngineRecord);
    long recordPosition = baseFileInstantTimeOfPositions.isPresent() ? record.getCurrentPosition() : -1L;
    deleteRecordPositions.add(recordPosition);
  }

  private Comparable getDeleteOrderingValue(HoodieRecord record, HoodieSchema recordSchema) {
    if (orderingFieldNames.isEmpty()) {
      return OrderingValues.getDefault();
    }
    return record.getOrderingValue(recordSchema, recordProperties, orderingFieldNames.toArray(new String[0]));
  }

  private Object[] createDeleteLogFieldValues(String recordKey, Comparable orderingValue) {
    Object[] fieldValues = new Object[deleteLogSchema.getFields().size()];
    fieldValues[0] = recordContext.convertValueToEngineType(recordKey);
    if (!OrderingValues.isCommitTimeOrderingValue(orderingValue)) {
      if (orderingFieldNames.size() == 1) {
        fieldValues[1] = orderingValue;
      } else {
        List<Comparable> orderingValues = OrderingValues.getValues((ArrayComparable) orderingValue);
        for (int i = 0; i < orderingValues.size(); i++) {
          fieldValues[i + 1] = orderingValues.get(i);
        }
      }
    }
    return fieldValues;
  }

  public AppendResult flushAppend(Map<HeaderMetadataType, String> header) throws IOException {
    int appendVersion = currentAppendVersion;
    lastAppendResults = new ArrayList<>();
    lastDataFileFormatMetadata = Option.empty();
    addFooterMetadata(header);
    closeFileWriters();
    if (dataLogFile != null) {
      lastAppendResults.add(toAppendResult(dataLogFile));
    }
    if (deleteLogFile != null) {
      lastAppendResults.add(toAppendResult(deleteLogFile));
    }
    dataLogFile = null;
    deleteLogFile = null;
    currentAppendVersion = -1;
    return completeAppend(appendVersion);
  }

  private void addFooterMetadata(Map<HeaderMetadataType, String> header) throws IOException {
    if (dataFileWriter != null) {
      dataFileWriter.addFooterMetadata(NativeLogFooterMetadata.toFooterMetadata(
          addRecordPositionsIfRequired(header, dataRecordPositions)));
    }
    if (deleteFileWriter != null) {
      deleteFileWriter.addFooterMetadata(NativeLogFooterMetadata.toFooterMetadata(
          addRecordPositionsIfRequired(header, deleteRecordPositions)));
    }
  }

  private Map<HeaderMetadataType, String> addRecordPositionsIfRequired(
      Map<HeaderMetadataType, String> header, List<Long> recordPositions) throws IOException {
    if (!header.containsKey(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS)) {
      return header;
    }

    Map<HeaderMetadataType, String> updatedHeader = new HashMap<>(header);
    Option<String> encodedRecordPositions = LogReaderUtils.encodePositionsAsLongList(recordPositions);
    if (encodedRecordPositions.isPresent()) {
      updatedHeader.put(HeaderMetadataType.RECORD_POSITIONS, encodedRecordPositions.get());
    } else {
      updatedHeader.remove(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
    }
    return updatedHeader;
  }

  private void ensureDataFileWriter(HoodieSchema recordSchema) throws IOException {
    ensureAppendVersion();
    if (dataFileWriter == null) {
      dataLogFile = createNativeLogFile(currentAppendVersion, DATA_LOG_EXTENSION);
      dataFileWriter = HoodieFileWriterFactory.getFileWriter(
          instantTime, dataLogFile.getPath(), storage, writeConfig, recordSchema, taskContextSupplier,
          writeConfig.getRecordMerger().getRecordType());
    }
  }

  private void ensureDeleteFileWriter() throws IOException {
    ensureAppendVersion();
    if (deleteFileWriter == null) {
      deleteLogFile = createNativeLogFile(currentAppendVersion, DELETE_LOG_EXTENSION);
      deleteLogSchema = HoodieSchemas.createDeleteLogSchema(tableSchema, orderingFieldNames);
      // The delete records are built through recordContext#constructEngineRecord (see #appendDeleteRecord), so the
      // writer must match the record context's engine type rather than the merger's record type: on Spark executors
      // the reader context for write degrades to Avro (no engine context available), and using the merger record
      // type here (e.g. SPARK) would create an engine-native writer that fails on the Avro delete records.
      deleteFileWriter = HoodieFileWriterFactory.getFileWriter(
          instantTime, deleteLogFile.getPath(), storage, writeConfig, deleteLogSchema, taskContextSupplier,
          recordContext.getEngineRecordType());
    }
  }

  private void ensureAppendVersion() throws IOException {
    if (currentAppendVersion < 0) {
      currentAppendVersion = nextAvailableVersion();
    }
  }

  private void closeFileWriters() throws IOException {
    if (dataFileWriter != null) {
      dataFileWriter.close();
      lastDataFileFormatMetadata = writeConfig.isMetadataColumnStatsIndexEnabled()
          ? Option.ofNullable(dataFileWriter.getFileFormatMetadata()) : Option.empty();
      dataFileWriter = null;
    }
    if (deleteFileWriter != null) {
      deleteFileWriter.close();
      deleteFileWriter = null;
    }
    dataRecordPositions.clear();
    deleteRecordPositions.clear();
  }

  private HoodieLogFile createNativeLogFile(int version, String logExtension) throws IOException {
    HoodieLogFile nativeLogFile = new HoodieLogFile(makeNativeLogPath(version, logExtension), 0);
    getFileCreationCallback().preFileCreation(nativeLogFile);
    return nativeLogFile;
  }

  private AppendResult toAppendResult(HoodieLogFile nativeLogFile) throws IOException {
    long fileSize = storage.getPathInfo(nativeLogFile.getPath()).getLength();
    nativeLogFile.setFileSize(fileSize);
    return new AppendResult(nativeLogFile, 0, fileSize);
  }

  private AppendResult completeAppend(int appendVersion) {
    this.logVersion = appendVersion + 1;
    if (lastAppendResults.isEmpty()) {
      return new AppendResult(logFile, 0, 0);
    }

    // The returned result preserves the legacy single-result writer contract. Native append handles consume
    // lastAppendResults directly so every physical data/delete file gets its own write stat.
    AppendResult firstResult = lastAppendResults.get(0);
    this.logFile = firstResult.logFile();
    return firstResult;
  }

  private int nextAvailableVersion() throws IOException {
    int candidateVersion = logVersion;
    while (nativeLogExists(candidateVersion, DATA_LOG_EXTENSION)
        || nativeLogExists(candidateVersion, DELETE_LOG_EXTENSION)) {
      candidateVersion++;
    }
    return candidateVersion;
  }

  private boolean nativeLogExists(int version, String logExtension) throws IOException {
    return storage.exists(makeNativeLogPath(version, logExtension));
  }

  private StoragePath makeNativeLogPath(int version, String logExtension) {
    return new StoragePath(parentPath, FSUtils.makeNativeLogFileName(
        logFileId, logWriteToken, instantTime, version, logExtension, nativeFileFormat));
  }

}
