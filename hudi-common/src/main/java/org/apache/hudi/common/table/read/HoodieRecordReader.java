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
import java.io.Serializable;
import java.util.Map;

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
   * Returns the same records as {@link #getLogRecordsOnly()}, indexed for random access.
   *
   * <p>Keys are record keys, except where the reader merges on record positions, in which case they
   * are the positions in the base file. A reader that falls back from position based to key based
   * merging part way through a file group re-keys what it has already read, so a caller that needs
   * one key type should not merge on positions.
   *
   * <p>A reader that already holds the log records in memory returns a read-only view of its own
   * index, valid until the next call on this reader or until {@link #close()}; one that streams them
   * materializes a map instead. Prefer {@link #getLogRecordsOnly()} when a single pass is enough,
   * since it streams within the reader's memory budget while bulk-iterating the map does not.
   *
   * @return log records indexed by record key or by record position, empty if the file group has no
   *         log files to merge
   * @throws UnsupportedOperationException if the reader does not index log records, for example when
   *         reading with merging skipped
   */
  Map<Serializable, BufferedRecord<T>> getLogRecordsMap() throws IOException;

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
