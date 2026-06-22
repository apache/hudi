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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Common readable surface for record readers over a Hudi file group.
 *
 * @param <T> The type of engine-specific record representation.
 */
public interface HoodieRecordReader<T> extends Closeable {

  /**
   * Returns buffered records carrying Hudi metadata needed by merge/write paths.
   */
  ClosableIterator<BufferedRecord<T>> getClosableBufferedRecordIterator() throws IOException;

  /**
   * Returns final engine-specific records.
   */
  ClosableIterator<T> getClosableIterator() throws IOException;

  /**
   * Returns final records wrapped as {@link HoodieRecord}s.
   */
  ClosableIterator<HoodieRecord<T>> getClosableHoodieRecordIterator() throws IOException;

  /**
   * Returns only record keys from the file group.
   */
  ClosableIterator<String> getClosableKeyIterator() throws IOException;

  /**
   * Returns records that come from log files only.
   */
  ClosableIterator<BufferedRecord<T>> getLogRecordsOnly() throws IOException;

  /**
   * Returns read statistics collected by this reader.
   */
  HoodieReadStats getReadStats();

  /**
   * Notifies the reader that writing a record failed so callbacks can run cleanup or bookkeeping.
   *
   * @param recordKey key of the failed record
   */
  void onWriteFailure(String recordKey);
}
