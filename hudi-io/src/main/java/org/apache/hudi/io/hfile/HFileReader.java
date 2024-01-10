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

package org.apache.hudi.io.hfile;

import org.apache.hudi.common.util.Option;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface HFileReader extends Closeable {
  void initializeMetadata() throws IOException;

  Option<byte[]> getMetaInfo(UTF8StringKey key) throws IOException;

  Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException;

  long getNumKeyValueEntries();

  /**
   * SeekTo or just before the passed {@link Key}.  Examine the return
   * code to figure whether we found the key or not.
   * Consider the cell stream of all the cells in the file,
   * <code>c[0] .. c[n]</code>, where there are n cells in the file.
   * <p>
   * The position only moves forward so the caller has to make sure the
   * keys are sorted before making multiple calls of this method.
   * <p>
   * @param key {@link Key} to seek to.
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
  int seekTo(Key key) throws IOException;

  /**
   * Positions this scanner at the start of the file.
   * @return False if empty file; i.e. a call to next would return false and
   * the current key and value are undefined.
   * @throws IOException
   */
  boolean seekTo() throws IOException;

  /**
   * Scans to the next entry in the file.
   *
   * @return Returns false if you are at the end otherwise true if more in file.
   * @throws IOException
   */
  boolean next() throws IOException;

  /**
   * @return The {@link KeyValue} instance at current position.
   */
  Option<KeyValue> getKeyValue() throws IOException;

  /**
   * @return True is scanner has had one of the seek calls invoked; i.e.
   * {@link #seekTo()} or {@link #seekTo(Key)}.
   * Otherwise returns false.
   */
  boolean isSeeked();
}
