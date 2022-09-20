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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;

public class HoodieCDCLogRecordReader implements ClosableIterator<IndexedRecord> {

  private final HoodieLogFile cdcLogFile;

  private final HoodieLogFormat.Reader reader;

  private ClosableIterator<IndexedRecord> itr;

  public HoodieCDCLogRecordReader(
      FileSystem fs,
      Path cdcLogPath,
      Schema cdcSchema) throws IOException {
    this.cdcLogFile = new HoodieLogFile(fs.getFileStatus(cdcLogPath));
    this.reader = new HoodieLogFileReader(fs, cdcLogFile, cdcSchema,
        HoodieLogFileReader.DEFAULT_BUFFER_SIZE, false);
  }

  @Override
  public boolean hasNext() {
    if (itr == null || !itr.hasNext()) {
      if (reader.hasNext()) {
        HoodieDataBlock dataBlock = (HoodieDataBlock) reader.next();
        if (dataBlock.getBlockType() == HoodieLogBlock.HoodieLogBlockType.CDC_DATA_BLOCK) {
          itr = dataBlock.getRecordIterator();
          return itr.hasNext();
        } else {
          return hasNext();
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public IndexedRecord next() {
    return itr.next();
  }

  @Override
  public void close() {
    try {
      itr.close();
      reader.close();
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage());
    }
  }
}
