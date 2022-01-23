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

package org.apache.hudi.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.KeyValue;
//import org.apache.hudi.hbase.client.Scan;

/**
 * Scanner that returns the next KeyValue.
 */
@InterfaceAudience.Private
// TODO: Change name from KeyValueScanner to CellScanner only we already have a simple CellScanner
// so this should be something else altogether, a decoration on our base CellScanner. TODO.
// This class shows in CPs so do it all in one swell swoop. HBase-2.0.0.
public interface KeyValueScanner extends Shipper, Closeable {
  /**
   * The byte array represents for NO_NEXT_INDEXED_KEY;
   * The actual value is irrelevant because this is always compared by reference.
   */
  public static final Cell NO_NEXT_INDEXED_KEY = new KeyValue();

  /**
   * Look at the next Cell in this scanner, but do not iterate scanner.
   * NOTICE: The returned cell has not been passed into ScanQueryMatcher. So it may not be what the
   * user need.
   * @return the next Cell
   */
  Cell peek();

  /**
   * Return the next Cell in this scanner, iterating the scanner
   * @return the next Cell
   */
  Cell next() throws IOException;

  /**
   * Seek the scanner at or after the specified KeyValue.
   * @param key seek value
   * @return true if scanner has values left, false if end of scanner
   */
  boolean seek(Cell key) throws IOException;

  /**
   * Reseek the scanner at or after the specified KeyValue.
   * This method is guaranteed to seek at or after the required key only if the
   * key comes after the current position of the scanner. Should not be used
   * to seek to a key which may come before the current position.
   * @param key seek value (should be non-null)
   * @return true if scanner has values left, false if end of scanner
   */
  boolean reseek(Cell key) throws IOException;

  /**
   * Get the order of this KeyValueScanner. This is only relevant for StoreFileScanners.
   * This is required for comparing multiple files to find out which one has the latest
   * data. StoreFileScanners are ordered from 0 (oldest) to newest in increasing order.
   */
  default long getScannerOrder(){
    return 0;
  }

  /**
   * Close the KeyValue scanner.
   */
  @Override
  void close();

  /**
   * Allows to filter out scanners (both StoreFile and memstore) that we don't
   * want to use based on criteria such as Bloom filters and timestamp ranges.
   * @param scan the scan that we are selecting scanners for
   * @param store the store we are performing the scan on.
   * @param oldestUnexpiredTS the oldest timestamp we are interested in for
   *          this query, based on TTL
   * @return true if the scanner should be included in the query
   */
  //boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS);

  // "Lazy scanner" optimizations

  /**
   * Similar to {@link #seek} (or {@link #reseek} if forward is true) but only
   * does a seek operation after checking that it is really necessary for the
   * row/column combination specified by the kv parameter. This function was
   * added to avoid unnecessary disk seeks by checking row-column Bloom filters
   * before a seek on multi-column get/scan queries, and to optimize by looking
   * up more recent files first.
   * @param forward do a forward-only "reseek" instead of a random-access seek
   * @param useBloom whether to enable multi-column Bloom filter optimization
   */
  boolean requestSeek(Cell kv, boolean forward, boolean useBloom)
      throws IOException;

  /**
   * We optimize our store scanners by checking the most recent store file
   * first, so we sometimes pretend we have done a seek but delay it until the
   * store scanner bubbles up to the top of the key-value heap. This method is
   * then used to ensure the top store file scanner has done a seek operation.
   */
  boolean realSeekDone();

  /**
   * Does the real seek operation in case it was skipped by
   * seekToRowCol(KeyValue, boolean) (TODO: Whats this?). Note that this function should
   * be never called on scanners that always do real seek operations (i.e. most
   * of the scanners). The easiest way to achieve this is to call
   * {@link #realSeekDone()} first.
   */
  void enforceSeek() throws IOException;

  /**
   * @return true if this is a file scanner. Otherwise a memory scanner is
   *         assumed.
   */
  boolean isFileScanner();

  /**
   * @return the file path if this is a file scanner, otherwise null.
   * @see #isFileScanner()
   */
  Path getFilePath();

  // Support for "Reversed Scanner"
  /**
   * Seek the scanner at or before the row of specified Cell, it firstly
   * tries to seek the scanner at or after the specified Cell, return if
   * peek KeyValue of scanner has the same row with specified Cell,
   * otherwise seek the scanner at the first Cell of the row which is the
   * previous row of specified KeyValue
   *
   * @param key seek KeyValue
   * @return true if the scanner is at the valid KeyValue, false if such
   *         KeyValue does not exist
   *
   */
  public boolean backwardSeek(Cell key) throws IOException;

  /**
   * Seek the scanner at the first Cell of the row which is the previous row
   * of specified key
   * @param key seek value
   * @return true if the scanner at the first valid Cell of previous row,
   *         false if not existing such Cell
   */
  public boolean seekToPreviousRow(Cell key) throws IOException;

  /**
   * Seek the scanner at the first KeyValue of last row
   *
   * @return true if scanner has values left, false if the underlying data is
   *         empty
   * @throws IOException
   */
  public boolean seekToLastRow() throws IOException;

  /**
   * @return the next key in the index, usually the first key of next block OR a key that falls
   * between last key of current block and first key of next block..
   * see HFileWriterImpl#getMidpoint, or null if not known.
   */
  public Cell getNextIndexedKey();
}
