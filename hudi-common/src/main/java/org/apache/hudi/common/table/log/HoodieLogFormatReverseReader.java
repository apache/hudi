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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A log format reader by reading log files and blocks in reserve order.
 * This reader assumes that each log file only has one log block.
 */
public class HoodieLogFormatReverseReader implements HoodieLogFormat.Reader {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogFormatReader.class);
  private final List<HoodieLogFile> logFiles;
  // Readers for previously scanned log-files that are still open
  private final List<HoodieLogFileReader> prevReadersInOpenState;
  private HoodieLogFileReader currentReader;
  private final HoodieStorage storage;
  private final Schema readerSchema;
  private InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();
  private final boolean reverseLogReader;
  private final String recordKeyField;
  private final boolean enableInlineReading;
  private final int bufferSize;
  private int logFilePos = -1;

  HoodieLogFormatReverseReader(HoodieStorage storage, List<HoodieLogFile> logFiles, Schema readerSchema,
                               boolean reverseLogReader, int bufferSize, boolean enableRecordLookups,
                               String recordKeyField, InternalSchema internalSchema) throws IOException {
    this.logFiles = logFiles;
    this.storage = storage;
    this.readerSchema = readerSchema;
    this.reverseLogReader = reverseLogReader;
    this.bufferSize = bufferSize;
    this.prevReadersInOpenState = new ArrayList<>();
    this.recordKeyField = recordKeyField;
    this.enableInlineReading = enableRecordLookups;
    this.internalSchema =
        internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    logFilePos = logFiles.size() - 1;
    if (logFilePos >= 0) {
      HoodieLogFile nextLogFile = logFiles.get(logFilePos);
      logFilePos--;
      this.currentReader = new HoodieLogFileReader(storage, nextLogFile, readerSchema, bufferSize, false,
          enableRecordLookups, recordKeyField, internalSchema);
    }
  }

  @Override
  public void close() throws IOException {
    for (HoodieLogFileReader reader : prevReadersInOpenState) {
      reader.close();
    }

    prevReadersInOpenState.clear();

    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() {
    if (currentReader == null) {
      return false;
    } else if (currentReader.hasNext()) {
      return true;
    } else if (logFilePos >= 0) {
      try {
        HoodieLogFile nextLogFile = logFiles.get(logFilePos);
        logFilePos--;
        this.prevReadersInOpenState.add(currentReader);
        this.currentReader = new HoodieLogFileReader(storage, nextLogFile, readerSchema, bufferSize, false,
            enableInlineReading, recordKeyField, internalSchema);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      LOG.info("Moving to the next reader for logfile {}", currentReader.getLogFile());
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
}
