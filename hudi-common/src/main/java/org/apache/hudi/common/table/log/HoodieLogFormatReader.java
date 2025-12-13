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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Hoodie log format reader.
 */
public class HoodieLogFormatReader implements HoodieLogFormat.Reader {

  private final List<HoodieLogFile> logFiles;
  private HoodieLogFileReader currentReader;
  private final HoodieStorage storage;
  private final Schema readerSchema;
  private final InternalSchema internalSchema;
  private final String recordKeyField;
  private final boolean enableInlineReading;
  private final int bufferSize;

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogFormatReader.class);

  public HoodieLogFormatReader(HoodieStorage storage, List<HoodieLogFile> logFiles, Schema readerSchema,
                        boolean reverseLogReader, int bufferSize, boolean enableRecordLookups,
                        String recordKeyField, InternalSchema internalSchema) throws IOException {
    this.logFiles = logFiles;
    this.storage = storage;
    this.readerSchema = readerSchema;
    this.bufferSize = bufferSize;
    this.recordKeyField = recordKeyField;
    this.enableInlineReading = enableRecordLookups;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    if (!logFiles.isEmpty()) {
      HoodieLogFile nextLogFile = logFiles.remove(0);
      this.currentReader = new HoodieLogFileReader(storage, nextLogFile, readerSchema, bufferSize, false,
          enableRecordLookups, recordKeyField, internalSchema);
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
        this.currentReader = new HoodieLogFileReader(storage, nextLogFile, readerSchema, bufferSize, false,
            enableInlineReading, recordKeyField, internalSchema);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      LOG.debug("Moving to the next reader for logfile {}", currentReader.getLogFile());
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
