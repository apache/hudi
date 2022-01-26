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

package org.apache.hudi.hbase.io.hfile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.regionserver.Shipper;
import org.apache.hudi.hbase.Cell;

/**
 * A scanner allows you to position yourself within a HFile and
 * scan through it.  It allows you to reposition yourself as well.
 *
 * <p>A scanner doesn't always have a key/value that it is pointing to
 * when it is first created and before
 * {@link #seekTo()}/{@link #seekTo(Cell)} are called.
 * In this case, {@link #getKey()}/{@link #getValue()} returns null.  At most
 * other times, a key and value will be available.  The general pattern is that
 * you position the Scanner using the seekTo variants and then getKey and
 * getValue.
 */
@InterfaceAudience.Private
public interface HFileScanner extends Shipper, Closeable {
  /**
   * SeekTo or just before the passed <code>cell</code>.  Examine the return
   * code to figure whether we found the cell or not.
   * Consider the cell stream of all the cells in the file,
   * <code>c[0] .. c[n]</code>, where there are n cells in the file.
   * @param cell
   * @return -1, if cell &lt; c[0], no position;
   * 0, such that c[i] = cell and scanner is left in position i; and
   * 1, such that c[i] &lt; cell, and scanner is left in position i.
   * The scanner will position itself between c[i] and c[i+1] where
   * c[i] &lt; cell &lt;= c[i+1].
   * If there is no cell c[i+1] greater than or equal to the input cell, then the
   * scanner will position itself at the end of the file and next() will return
   * false when it is called.
   * @throws IOException
   */
  int seekTo(Cell cell) throws IOException;

  /**
   * Reseek to or just before the passed <code>cell</code>. Similar to seekTo
   * except that this can be called even if the scanner is not at the beginning
   * of a file.
   * This can be used to seek only to cells which come after the current position
   * of the scanner.
   * Consider the cell stream of all the cells in the file,
   * <code>c[0] .. c[n]</code>, where there are n cellc in the file after
   * current position of HFileScanner.
   * The scanner will position itself between c[i] and c[i+1] where
   * c[i] &lt; cell &lt;= c[i+1].
   * If there is no cell c[i+1] greater than or equal to the input cell, then the
   * scanner will position itself at the end of the file and next() will return
   * false when it is called.
   * @param cell Cell to find (should be non-null)
   * @return -1, if cell &lt; c[0], no position;
   * 0, such that c[i] = cell and scanner is left in position i; and
   * 1, such that c[i] &lt; cell, and scanner is left in position i.
   * @throws IOException
   */
  int reseekTo(Cell cell) throws IOException;

  /**
   * Consider the cell stream of all the cells in the file,
   * <code>c[0] .. c[n]</code>, where there are n cells in the file.
   * @param cell Cell to find
   * @return false if cell &lt;= c[0] or true with scanner in position 'i' such
   * that: c[i] &lt; cell.  Furthermore: there may be a c[i+1], such that
   * c[i] &lt; cell &lt;= c[i+1] but there may also NOT be a c[i+1], and next() will
   * return false (EOF).
   * @throws IOException
   */
  boolean seekBefore(Cell cell) throws IOException;

  /**
   * Positions this scanner at the start of the file.
   * @return False if empty file; i.e. a call to next would return false and
   * the current key and value are undefined.
   * @throws IOException
   */
  boolean seekTo() throws IOException;

  /**
   * Scans to the next entry in the file.
   * @return Returns false if you are at the end otherwise true if more in file.
   * @throws IOException
   */
  boolean next() throws IOException;

  /**
   * Gets the current key in the form of a cell. You must call
   * {@link #seekTo(Cell)} before this method.
   * @return gets the current key as a Cell.
   */
  Cell getKey();

  /**
   * Gets a buffer view to the current value.  You must call
   * {@link #seekTo(Cell)} before this method.
   *
   * @return byte buffer for the value. The limit is set to the value size, and
   * the position is 0, the start of the buffer view.
   */
  ByteBuffer getValue();

  /**
   * @return Instance of {@link org.apache.hudi.hbase.Cell}.
   */
  Cell getCell();

  /**
   * Convenience method to get a copy of the key as a string - interpreting the
   * bytes as UTF8. You must call {@link #seekTo(Cell)} before this method.
   * @return key as a string
   * @deprecated Since hbase-2.0.0
   */
  @Deprecated
  String getKeyString();

  /**
   * Convenience method to get a copy of the value as a string - interpreting
   * the bytes as UTF8. You must call {@link #seekTo(Cell)} before this method.
   * @return value as a string
   * @deprecated Since hbase-2.0.0
   */
  @Deprecated
  String getValueString();

  /**
   * @return Reader that underlies this Scanner instance.
   */
  HFile.Reader getReader();

  /**
   * @return True is scanner has had one of the seek calls invoked; i.e.
   * {@link #seekBefore(Cell)} or {@link #seekTo()} or {@link #seekTo(Cell)}.
   * Otherwise returns false.
   */
  boolean isSeeked();

  /**
   * @return the next key in the index (the key to seek to the next block)
   */
  Cell getNextIndexedKey();

  /**
   * Close this HFile scanner and do necessary cleanup.
   */
  @Override
  void close();
}
