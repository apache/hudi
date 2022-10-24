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
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class HoodieCDCLogRecordIterator implements ClosableIterator<IndexedRecord> {

  private final FileSystem fs;

  private final Schema cdcSchema;

  private final Iterator<HoodieLogFile> cdcLogFileIter;

  private HoodieLogFormat.Reader reader;

  private ClosableIterator<IndexedRecord> itr;

  private IndexedRecord record;

  public HoodieCDCLogRecordIterator(FileSystem fs, HoodieLogFile[] cdcLogFiles, Schema cdcSchema) {
    this.fs = fs;
    this.cdcSchema = cdcSchema;
    this.cdcLogFileIter = Arrays.stream(cdcLogFiles).iterator();
  }

  @Override
  public boolean hasNext() {
    if (record != null) {
      return true;
    }
    if (itr == null || !itr.hasNext()) {
      if (reader == null || !reader.hasNext()) {
        // step1: load new file reader first.
        if (!loadReader()) {
          return false;
        }
      }
      // step2: load block records iterator
      if (!loadItr()) {
        return false;
      }
    }
    record = itr.next();
    return true;
  }

  private boolean loadReader() {
    try {
      closeReader();
      if (cdcLogFileIter.hasNext()) {
        reader = new HoodieLogFileReader(fs, cdcLogFileIter.next(), cdcSchema,
            HoodieLogFileReader.DEFAULT_BUFFER_SIZE, false);
        return reader.hasNext();
      }
      return false;
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage());
    }
  }

  private boolean loadItr() {
    HoodieDataBlock dataBlock = (HoodieDataBlock) reader.next();
    closeItr();
    itr = dataBlock.getRecordIterator();
    return itr.hasNext();
  }

  @Override
  public IndexedRecord next() {
    IndexedRecord ret = record;
    record = null;
    return ret;
  }

  @Override
  public void close() {
    try {
      closeItr();
      closeReader();
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage());
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private void closeItr() {
    if (itr != null) {
      itr.close();
      itr = null;
    }
  }
}
