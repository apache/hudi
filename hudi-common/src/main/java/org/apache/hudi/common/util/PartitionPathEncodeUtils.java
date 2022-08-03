/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import java.util.BitSet;

/**
 * Utils to encode/decode the partition path.
 * This code is mainly copy from Hive (org.apache.hadoop.hive.common.FileUtils).
 */
public class PartitionPathEncodeUtils {

  public static final String DEFAULT_PARTITION_PATH = "__HIVE_DEFAULT_PARTITION__";

  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }

    /**
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
      '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
      '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
      '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
      '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
      '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
      '[', ']', '^'};

    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }

  public static String escapePathName(String path) {
    return escapePathName(path, null);
  }

  /**
   * Escapes a path name.
   * @param path The path to escape.
   * @param defaultPath
   *          The default name for the path, if the given path is empty or null.
   * @return An escaped path name.
   */
  public static String escapePathName(String path, String defaultPath) {
    if (path == null || path.length() == 0) {
      if (defaultPath == null) {
        // previously, when path is empty or null and no default path is specified,
        // "default" was the return value for escapePathName
        return DEFAULT_PARTITION_PATH;
      } else {
        return defaultPath;
      }
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public static String escapePartitionValue(String value) {
    if (value == null || value.isEmpty()) {
      return DEFAULT_PARTITION_PATH;
    } else {
      return escapePathName(value);
    }
  }
}
