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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * CDC log record iterator for inline CDC log blocks.
 */
public class HoodieCDCInlineLogRecordIterator implements HoodieCDCLogRecordIterator<IndexedRecord> {

  private final HoodieStorage storage;

  private final HoodieSchema cdcSchema;

  private final Iterator<HoodieLogFile> cdcLogFileIter;

  private HoodieLogFormat.Reader reader;

  private ClosableIterator<HoodieCDCLogRecord<IndexedRecord>> itr;

  private HoodieCDCLogRecord<IndexedRecord> record;

  public HoodieCDCInlineLogRecordIterator(HoodieStorage storage, HoodieLogFile[] cdcLogFiles, HoodieSchema cdcSchema) {
    this.storage = storage;
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
        if (!loadReader()) {
          return false;
        }
      }
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
        reader = new HoodieLogFileReader(
            storage, cdcLogFileIter.next(), cdcSchema, HoodieLogFileReader.DEFAULT_BUFFER_SIZE);
        return reader.hasNext();
      }
      return false;
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private boolean loadItr() {
    HoodieDataBlock dataBlock = (HoodieDataBlock) reader.next();
    closeItr();
    itr = new CloseableMappingIterator<>(
        dataBlock.getRecordIterator(HoodieRecordType.AVRO),
        // Cast via Object to avoid an unchecked-cast warning; AVRO record iterators yield HoodieAvroIndexedRecord.
        record -> new InlineCDCLogRecord(((HoodieAvroIndexedRecord) (Object) record).getData()));
    return itr.hasNext();
  }

  @Override
  public HoodieCDCLogRecord<IndexedRecord> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more CDC log records");
    }
    HoodieCDCLogRecord<IndexedRecord> ret = record;
    record = null;
    return ret;
  }

  @Override
  public void close() {
    try {
      closeItr();
      closeReader();
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

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

  private static class InlineCDCLogRecord implements HoodieCDCLogRecord<IndexedRecord> {
    private final IndexedRecord record;

    private InlineCDCLogRecord(IndexedRecord record) {
      this.record = record;
    }

    @Override
    public String getOperation() {
      return String.valueOf(record.get(0));
    }

    @Override
    public String getRecordKey() {
      return String.valueOf(record.get(1));
    }

    @Override
    public IndexedRecord getAvroImage(int ordinal) {
      return (IndexedRecord) record.get(ordinal);
    }

    @Override
    public IndexedRecord getEngineImage(int ordinal, int imageArity) {
      throw new UnsupportedOperationException("Inline CDC records do not contain engine row images");
    }

    @Override
    public boolean isNative() {
      return false;
    }
  }
}
