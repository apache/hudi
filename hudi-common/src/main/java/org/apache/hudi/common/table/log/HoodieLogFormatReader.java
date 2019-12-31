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

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hoodie log format reader.
 */
public class HoodieLogFormatReader implements HoodieLogFormat.Reader {

  private final List<HoodieLogFile> logFiles;
  // Readers for previously scanned log-files that are still open
  private final List<HoodieLogFileReader> prevReadersInOpenState;
  private HoodieLogFileReader currentReader;
  private final FileSystem fs;
  private final Schema readerSchema;
  private final boolean readBlocksLazily;
  private final boolean reverseLogReader;
  private int bufferSize;

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogFormatReader.class);

  HoodieLogFormatReader(FileSystem fs, List<HoodieLogFile> logFiles, Schema readerSchema, boolean readBlocksLazily,
      boolean reverseLogReader, int bufferSize) throws IOException {
    this.logFiles = logFiles;
    this.fs = fs;
    this.readerSchema = readerSchema;
    this.readBlocksLazily = readBlocksLazily;
    this.reverseLogReader = reverseLogReader;
    this.bufferSize = bufferSize;
    this.prevReadersInOpenState = new ArrayList<>();
    if (logFiles.size() > 0) {
      HoodieLogFile nextLogFile = logFiles.remove(0);
      this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, bufferSize, readBlocksLazily, false);
    }
  }

  @Override
  /**
   * Note : In lazy mode, clients must ensure close() should be called only after processing all log-blocks as the
   * underlying inputstream will be closed. TODO: We can introduce invalidate() API at HoodieLogBlock and this object
   * can call invalidate on all returned log-blocks so that we check this scenario specifically in HoodieLogBlock
   */
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
    } else if (logFiles.size() > 0) {
      try {
        HoodieLogFile nextLogFile = logFiles.remove(0);
        // First close previous reader only if readBlockLazily is true
        if (!readBlocksLazily) {
          this.currentReader.close();
        } else {
          this.prevReadersInOpenState.add(currentReader);
        }
        this.currentReader =
            new HoodieLogFileReader(fs, nextLogFile, readerSchema, bufferSize, readBlocksLazily, false);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      LOG.info("Moving to the next reader for logfile {}", currentReader.getLogFile());
      return this.currentReader.hasNext();
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
  public void remove() {}

  @Override
  public boolean hasPrev() {
    return this.currentReader.hasPrev();
  }

  @Override
  public HoodieLogBlock prev() throws IOException {
    return this.currentReader.prev();
  }

}
