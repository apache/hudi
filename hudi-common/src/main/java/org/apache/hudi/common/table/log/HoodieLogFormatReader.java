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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * Hoodie log format reader.
 */
@Slf4j
public class HoodieLogFormatReader implements HoodieLogFormat.Reader {

  private final List<HoodieLogFile> logFiles;
  private HoodieLogFormat.Reader currentReader;
  private final HoodieStorage storage;
  private final HoodieSchema readerSchema;
  private final InternalSchema internalSchema;
  private final HoodieTableMetaClient metaClient;
  private final String recordKeyField;
  private final boolean enableInlineReading;
  private final int bufferSize;

  HoodieLogFormatReader(HoodieStorage storage, HoodieTableMetaClient metaClient, List<HoodieLogFile> logFiles,
                        HoodieSchema readerSchema, boolean reverseLogReader, int bufferSize, boolean enableRecordLookups,
                        String recordKeyField, InternalSchema internalSchema) throws IOException {
    this.logFiles = logFiles;
    this.storage = storage;
    this.metaClient = metaClient;
    this.readerSchema = readerSchema;
    this.bufferSize = bufferSize;
    this.recordKeyField = recordKeyField;
    this.enableInlineReading = enableRecordLookups;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    if (!logFiles.isEmpty()) {
      HoodieLogFile nextLogFile = logFiles.remove(0);
      this.currentReader = createReader(nextLogFile, reverseLogReader);
    }
  }

  /**
   * Closes any resources held
   */
  @Override
  public void close() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }
  }

  @Override
  public boolean hasNext() {

    if (currentReader == null) {
      return false;
    } else if (currentReader.hasNext()) {
      return true;
    } else if (!logFiles.isEmpty()) {
      try {
        HoodieLogFile nextLogFile = logFiles.remove(0);
        this.currentReader.close();
        this.currentReader = createReader(nextLogFile, false);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      log.debug("Moving to the next reader for logfile {}", currentReader.getLogFile());
      return hasNext();
    }
    return false;
  }

  @Override
  public HoodieLogBlock next() {
    return currentReader.next();
  }

  @Override
  public HoodieLogFile getLogFile() {
    return currentReader.getLogFile();
  }

  @Override
  public void remove() {
  }

  @Override
  public boolean hasPrev() {
    return this.currentReader.hasPrev();
  }

  @Override
  public HoodieLogBlock prev() throws IOException {
    return this.currentReader.prev();
  }

  private HoodieLogFormat.Reader createReader(HoodieLogFile logFile, boolean reverseLogReader) throws IOException {
    if (FSUtils.matchNativeLogFile(logFile.getFileName()).isPresent()) {
      if (metaClient == null) {
        throw new HoodieNotSupportedException("Native log files require HoodieTableMetaClient");
      }
      List<String> orderingFieldNames =
          HoodieRecordUtils.getOrderingFieldNames(metaClient.getTableConfig().getRecordMergeMode(), metaClient);
      return new HoodieNativeLogFileReader(storage, logFile, readerSchema, internalSchema,
          orderingFieldNames,
          getRelativePartitionPath(logFile), metaClient.getTableConfig().getProps());
    }
    return new HoodieLogFileReader(storage, logFile, readerSchema, bufferSize, reverseLogReader,
        enableInlineReading, recordKeyField, internalSchema);
  }

  private String getRelativePartitionPath(HoodieLogFile logFile) {
    StoragePath logFileParent = logFile.getPath().getParent();
    return FSUtils.getRelativePartitionPath(metaClient.getBasePath(), logFileParent);
  }
}
