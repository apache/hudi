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

/**
 * HFile reader that supports seeks.
 */
public interface HFileReader extends Closeable {
  // Return code of seekTo(Key)
  // When the lookup key is not found, but it's lexicographically greater than
  // or equal to the fake first key of the data block based on the block index and
  // lexicographically smaller than the actual first key of the data block
  // The cursor points to the actual first key of the data block which is
  // lexicographically greater than the lookup key
  // PLEASE NOTE that there is an optimization of block index (enabled by default)
  // that the first key can be shortened to save memory, i.e., a fake first key is
  // stored in the block index entry, and such a fake first key does not exist in
  // the data block. The fake first key is lexicographically greater than the last
  // key of the previous block and lexicographically equal or smaller than the
  // actual first key of the current block.
  // PLEASE ALSO NOTE that the first key of the first data block stored in the block
  // index is not shortened and always exists in the first data block.
  int SEEK_TO_BEFORE_BLOCK_FIRST_KEY = -2;
  // When the lookup key is lexicographically smaller than the first key of the file
  // The cursor points to the first key of the file
  int SEEK_TO_BEFORE_FILE_FIRST_KEY = -1;
  // When the lookup key is found in the file
  // The cursor points to the matched key in the file
  int SEEK_TO_FOUND = 0;
  // When the lookup key is not found, but it's in the range of the file,
  // and specifically the lookup key is lexicographically greater than the
  // actual first key of the data block and lexicographically smaller than
  // the start key of the next data block based on the block index,
  // or lexicographically smaller than or equal to the last key of the file
  // if the data block is the last one
  // The cursor points to the greatest key that is lexicographically smaller than the lookup key
  int SEEK_TO_IN_RANGE = 1;
  // When the lookup key is greater than the last key of the file, EOF is reached
  // The cursor points to EOF
  int SEEK_TO_EOF = 2;

  /**
   * Initializes metadata based on a HFile before other read operations.
   *
   * @throws IOException upon read errors.
   */
  void initializeMetadata() throws IOException;

  /**
   * Gets info entry from file info block of a HFile.
   *
   * @param key meta key.
   * @return the content in bytes if present.
   * @throws IOException upon read errors.
   */
  Option<byte[]> getMetaInfo(UTF8StringKey key) throws IOException;

  /**
   * Gets the content of a meta block from HFile.
   *
   * @param metaBlockName meta block name.
   * @return the content in bytes if present.
   * @throws IOException upon read errors.
   */
  Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException;

  /**
   * @return total number of key value entries in the HFile.
   */
  long getNumKeyValueEntries();

  /**
   * seekTo or just before the passed {@link Key}. Examine the return code to figure whether we
   * found the key or not. Consider the key-value pairs in the file,
   * <code>kv[0] .. kv[n-1]</code>, where there are n KV pairs in the file.
   * <p>
   * The position only moves forward so the caller has to make sure the keys are sorted before
   * making multiple calls of this method.
   * <p>
   *
   * @param key {@link Key} to seek to.
   * @return -2 ({@link HFileReader#SEEK_TO_BEFORE_BLOCK_FIRST_KEY}), such that
   * block_fake_first_key &lt;= key &lt; block_actual_first_key if there is no
   * exact match, and the reader is left in position at block_actual_first_key;
   * -1 ({@link HFileReader#SEEK_TO_BEFORE_FILE_FIRST_KEY}), if key &lt; kv[0], the reader
   * is left in position 0;
   * 0 ({@link HFileReader#SEEK_TO_FOUND}), such that kv[i].key = key and the reader is
   * left in position i;
   * 1 ({@link HFileReader#SEEK_TO_IN_RANGE}), such that kv[i].key &lt; key if there is
   * no exact match and the key is greater than the actual first of the data block, and
   * the reader is left in position i;
   * 2 ({@link HFileReader#SEEK_TO_EOF}), if there is no KV greater than or equal to the
   * input key, and the reader positions itself at the end of the file and next() will
   * return {@code false} when it is called.
   * @throws IOException upon read errors.
   */
  int seekTo(Key key) throws IOException;

  /**
   * Positions this reader at the start of the file.
   *
   * @return {@code false} if empty file; i.e. a call to next would return false and
   * the current key and value are undefined.
   * @throws IOException upon read errors.
   */
  boolean seekTo() throws IOException;

  /**
   * Scans to the next entry in the file.
   *
   * @return {@code false} if the current position is at the end;
   * otherwise {@code true} if more in file.
   * @throws IOException upon read errors.
   */
  boolean next() throws IOException;

  /**
   * @return The {@link KeyValue} instance at current position.
   */
  Option<KeyValue> getKeyValue() throws IOException;

  /**
   * @return {@code true} if the reader has had one of the seek calls invoked; i.e.
   * {@link #seekTo()} or {@link #seekTo(Key)}.
   * Otherwise, {@code false}.
   */
  boolean isSeeked();
}
