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
import java.util.concurrent.atomic.AtomicInteger;

public class HoodieCDCLogRecordIterator implements ClosableIterator<IndexedRecord> {

  private final FileSystem fs;

  private final Schema cdcSchema;

  private final Iterator<HoodieLogFile> cdcLogFileIter;

  private HoodieLogFormat.Reader reader;

  /**
   * Due to the hasNext of {@link HoodieLogFormat.Reader} is not idempotent,
   * Here guarantee idempotent by `hasNextCall` and `nextCall`.
   */
  private final AtomicInteger hasNextCall = new AtomicInteger(0);
  private final AtomicInteger nextCall = new AtomicInteger(0);

  private ClosableIterator<IndexedRecord> itr;

  public HoodieCDCLogRecordIterator(FileSystem fs, HoodieLogFile[] cdcLogFiles, Schema cdcSchema) {
    this.fs = fs;
    this.cdcSchema = cdcSchema;
    this.cdcLogFileIter = Arrays.stream(cdcLogFiles).iterator();
  }

  @Override
  public boolean hasNext() {
    if (hasNextCall.get() > nextCall.get()) {
      return true;
    }
    hasNextCall.incrementAndGet();
    if (itr == null || !itr.hasNext()) {
      if (reader == null || !reader.hasNext()) {
        if (!loadNextCDCLogFile()) {
          return false;
        }
      }
      return loadNextCDCBlock();
    }
    return true;
  }

  private boolean loadNextCDCLogFile() {
    try {
      if (reader != null) {
        // call close explicitly to release memory.
        reader.close();
      }
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

  private boolean loadNextCDCBlock() {
    HoodieDataBlock dataBlock = (HoodieDataBlock) reader.next();
    if (itr != null) {
      // call close explicitly to release memory.
      itr.close();
    }
    itr = dataBlock.getRecordIterator();
    return itr.hasNext();
  }

  @Override
  public IndexedRecord next() {
    nextCall.incrementAndGet();
    return itr.next();
  }

  @Override
  public void close() {
    try {
      itr.close();
      itr = null;
      reader.close();
      reader = null;
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage());
    }
  }
}
