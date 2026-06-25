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

import org.apache.hudi.io.util.IOUtils;

import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

/**
 * Util methods for reading and writing HFile.
 */
public class HFileUtils {
  // Hudi does not set a version timestamp on key-value pairs, so the latest timestamp is used.
  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;
  // Key type is constant Put (4) in Hudi.
  public static final byte KEY_TYPE_PUT = (byte) 4;
  // HBase KeyValue key suffix beyond the row: column-family length (1) + timestamp (8) + type (1).
  public static final int KEY_METADATA_SUFFIX_LENGTH =
      DataSize.SIZEOF_BYTE + DataSize.SIZEOF_INT64 + DataSize.SIZEOF_BYTE;

  /**
   * Returns the serialized length of the HBase KeyValue key for a row: the 2-byte row-length
   * prefix, the row, and the 10-byte metadata suffix (column-family length, timestamp, key type).
   *
   * @param rowLength length of the row (key content) in bytes.
   * @return the HBase KeyValue key length.
   */
  public static int keyValueKeyLength(int rowLength) {
    return DataSize.SIZEOF_INT16 + rowLength + KEY_METADATA_SUFFIX_LENGTH;
  }

  /**
   * Writes the HBase KeyValue key for a row:
   * {@code [2-byte rowLen][row][1-byte cfLen=0][8-byte ts=LATEST][1-byte type=Put]}. Both the data
   * block and the root index block use this so an HBase reader parses their keys identically; a
   * point lookup compares index keys against data keys, so any mismatch in the metadata bytes
   * would break it.
   *
   * @param out output stream to write to.
   * @param row row (key content) bytes.
   */
  public static void writeKeyValueKey(DataOutputStream out, byte[] row) throws IOException {
    out.writeShort((short) row.length);
    out.write(row);
    out.write(0);                     // column-family length
    out.writeLong(LATEST_TIMESTAMP);  // timestamp
    out.write(KEY_TYPE_PUT);          // key type
  }

  /**
   * Reads the HFile major version from the input.
   *
   * @param bytes  input data.
   * @param offset offset to start reading.
   * @return major version of the file.
   */
  public static int readMajorVersion(byte[] bytes, int offset) {
    int ch1 = bytes[offset] & 0xFF;
    int ch2 = bytes[offset + 1] & 0xFF;
    int ch3 = bytes[offset + 2] & 0xFF;
    return ((ch1 << 16) + (ch2 << 8) + ch3);
  }

  /**
   * Compares two HFile {@link Key}.
   *
   * @param key1 left operand key.
   * @param key2 right operand key.
   * @return 0 if equal, < 0 if left is less than right, > 0 otherwise.
   */
  public static int compareKeys(Key key1, Key key2) {
    return IOUtils.compareTo(
        key1.getBytes(), key1.getContentOffset(), key1.getContentLength(),
        key2.getBytes(), key2.getContentOffset(), key2.getContentLength());
  }

  /**
   * @param prefix the prefix to check
   * @param key    key to check
   * @return whether the key starts with the prefix.
   */
  public static boolean isPrefixOfKey(Key prefix, Key key) {
    int prefixLength = prefix.getContentLength();
    int keyLength = key.getLength();
    if (prefixLength > keyLength) {
      return false;
    }

    byte[] prefixBytes = prefix.getBytes();
    byte[] keyBytes = key.getBytes();
    for (int i = 0; i < prefixLength; i++) {
      if (prefixBytes[prefix.getContentOffset() + i] != keyBytes[key.getContentOffset() + i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the value in String.
   *
   * @param kv {@link KeyValue} instance.
   * @return the String with UTF-8 decoding.
   */
  public static String getValue(KeyValue kv) {
    return fromUTF8Bytes(kv.getBytes(), kv.getValueOffset(), kv.getValueLength());
  }
}
