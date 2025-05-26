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

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

/**
 * Util methods for reading and writing HFile.
 */
public class HFileUtils {
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
