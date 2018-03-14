/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log;

import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieLogFormatReader implements HoodieLogFormat.Reader {

  private final List<HoodieLogFile> logFiles;
  private HoodieLogFileReader currentReader;
  private final FileSystem fs;
  private final Schema readerSchema;
  private final boolean readBlocksLazily;
  private final boolean reverseLogReader;

  private final static Logger log = LogManager.getLogger(HoodieLogFormatReader.class);

  HoodieLogFormatReader(FileSystem fs, List<HoodieLogFile> logFiles,
                                  Schema readerSchema, boolean readBlocksLazily, boolean reverseLogReader) throws IOException {
    this.logFiles = logFiles;
    this.fs = fs;
    this.readerSchema = readerSchema;
    this.readBlocksLazily = readBlocksLazily;
    this.reverseLogReader = reverseLogReader;
    if(logFiles.size() > 0) {
      HoodieLogFile nextLogFile = logFiles.remove(0);
      this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, readBlocksLazily,
          false);
    }
  }

  HoodieLogFormatReader(FileSystem fs, List<HoodieLogFile> logFiles,
                               Schema readerSchema) throws IOException {
    this(fs, logFiles, readerSchema, false, false);
  }

  @Override
  public void close() throws IOException {
    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() {

    if(currentReader == null) {
      return false;
    }
    else if (currentReader.hasNext()) {
      return true;
    }
    else if (logFiles.size() > 0) {
      try {
        HoodieLogFile nextLogFile = logFiles.remove(0);
        this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, readBlocksLazily,
            false);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      log.info("Moving to the next reader for logfile " + currentReader.getLogFile());
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
  public void remove() {
  }

}