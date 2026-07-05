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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
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

/**
 * Manages native CDC log file creation, rolling, writes, and stats.
 */
class HoodieNativeCDCFileWriter {

  private final String commitTime;
  private final String partitionPath;
  private final HoodieStorage storage;
  private final HoodieWriteConfig config;
  private final HoodieSchema cdcSchema;
  private final HoodieFileFormat nativeFileFormat;
  private final StoragePath parentPath;
  private final String fileId;
  private final String writeToken;
  private final LogFileCreationCallback fileCreationCallback;
  private final TaskContextSupplier taskContextSupplier;
  private final HoodieRecord.HoodieRecordType recordType;
  private final Properties recordProperties;
  private final List<StoragePath> cdcAbsPaths;
  private int nextLogVersion;
  private HoodieFileWriter cdcWriter;

  HoodieNativeCDCFileWriter(
      String commitTime,
      String partitionPath,
      HoodieStorage storage,
      HoodieWriteConfig config,
      HoodieSchema cdcSchema,
      HoodieFileFormat nativeFileFormat,
      StoragePath parentPath,
      String fileId,
      String writeToken,
      LogFileCreationCallback fileCreationCallback,
      TaskContextSupplier taskContextSupplier,
      HoodieRecord.HoodieRecordType recordType) {
    this.commitTime = commitTime;
    this.partitionPath = partitionPath;
    this.storage = storage;
    this.config = config;
    this.cdcSchema = cdcSchema;
    this.nativeFileFormat = nativeFileFormat;
    this.parentPath = parentPath;
    this.fileId = fileId;
    this.writeToken = writeToken;
    this.fileCreationCallback = fileCreationCallback;
    this.taskContextSupplier = taskContextSupplier;
    this.recordType = recordType;
    this.recordProperties = new Properties();
    this.recordProperties.putAll(config.getProps());
    this.cdcAbsPaths = new ArrayList<>();
    this.nextLogVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
  }

  void write(String recordKey, HoodieRecord record) throws IOException {
    ensureCDCWriter();
    cdcWriter.write(recordKey, record, cdcSchema, recordProperties);
  }

  Map<String, Long> getCDCWriteStats() {
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

  void close() throws IOException {
    if (cdcWriter != null) {
      cdcWriter.close();
      cdcWriter = null;
    }
  }

  private void ensureCDCWriter() throws IOException {
    if (cdcWriter != null && cdcWriter.canWrite()) {
      return;
    }
    close();
    HoodieLogFile cdcLogFile = createNativeCDCLogFile();
    cdcWriter = HoodieFileWriterFactory.getFileWriter(
        commitTime, cdcLogFile.getPath(), storage, config, cdcSchema, taskContextSupplier, recordType);
    cdcAbsPaths.add(cdcLogFile.getPath());
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
}
