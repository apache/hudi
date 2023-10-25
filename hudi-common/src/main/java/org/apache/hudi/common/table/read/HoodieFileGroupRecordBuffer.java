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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface HoodieFileGroupRecordBuffer<T> {
  enum BufferType {
    KEY_BASED,
    POSITION_BASED
  }

  /**
   * @return The merge strategy implemented.
   */
  BufferType getBufferType();

  /**
   * Process a log data block, and store the resulting records into the buffer.
   *
   * @param dataBlock
   * @param keySpecOpt
   * @throws IOException
   */
  void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException;

  /**
   * Process a next record in a log data block.
   *
   * @param record
   * @param metadata
   * @throws Exception
   */
  void processNextDataRecord(T record, Map<String, Object> metadata, Object index) throws IOException;

  /**
   * Process a log delete block, and store the resulting records into the buffer.
   *
   * @param deleteBlock
   * @throws IOException
   */
  void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException;

  /**
   * Process next delete record.
   *
   * @param deleteRecord
   */
  void processNextDeletedRecord(DeleteRecord deleteRecord, Object index);

  /**
   * Check if a record exists in the buffered records.
   */
  boolean containsLogRecord(String recordKey);

  /**
   * @return the number of log records in the buffer.
   */
  int size();

  /**
   * @return An iterator on the log records.
   */
  Iterator<Pair<Option<T>, Map<String, Object>>> getLogRecordIterator();

  /**
   * @return The underlying data stored in the buffer.
   */
  Map<Object, Pair<Option<T>, Map<String, Object>>> getLogRecords();

  /**
   * Link the base file iterator for consequential merge.
   *
   * @param baseFileIterator
   */
  void setBaseFileIteraotr(ClosableIterator<T> baseFileIterator);

  /**
   * Check if next merged record exists.
   *
   * @return true if it has, otherwise false.
   */
  boolean hasNext() throws IOException;

  /**
   *
   * @return output the next merged record.
   */
  T next();

  void close();
}
