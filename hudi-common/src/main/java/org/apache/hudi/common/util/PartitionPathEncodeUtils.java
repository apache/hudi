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

import org.apache.hudi.exception.HoodieKeyException;

import java.util.BitSet;
import java.util.function.Function;

/**
 * Utils to encode/decode the partition path.
 * This code is mainly copy from Hive (org.apache.hadoop.hive.common.FileUtils).
 */
public class PartitionPathEncodeUtils {

  public static final String DEPRECATED_DEFAULT_PARTITION_PATH = "default";
  public static final String DEFAULT_PARTITION_PATH = "__HIVE_DEFAULT_PARTITION__";

  /**
   * Returns whether {@code partitionValue} is the marker Hudi writes for a null or empty partition
   * value. Both the current marker and the pre-0.12 {@code default} one are recognised, matching
   * {@code PartitionPathParser#parseValue}. The value may be a whole partition directory, in which
   * case a Hive-style column prefix (e.g. {@code datestr=__HIVE_DEFAULT_PARTITION__}) is stripped
   * before comparing.
   */
  public static boolean isDefaultPartitionValue(String partitionValue) {
    int separator = partitionValue.indexOf('=');
    String value = separator < 0 ? partitionValue : partitionValue.substring(separator + 1);
    return DEFAULT_PARTITION_PATH.equals(value) || DEPRECATED_DEFAULT_PARTITION_PATH.equals(value);
  }

  static BitSet charToEscape = new BitSet(128);
  static BitSet charToEscapeFilename = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
      charToEscapeFilename.set(c);
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
      charToEscapeFilename.set(c);
    }

    charToEscapeFilename.set('_');
    charToEscapeFilename.set('-');
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }

  static boolean needsEscapingFilename(char c) {
    return c >= 0 && c < charToEscapeFilename.size() && charToEscapeFilename.get(c);
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

    return doEscape(path, PartitionPathEncodeUtils::needsEscaping);
  }

  private static String doEscape(String path, Function<Character,Boolean> needsEscapeMethod) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscapeMethod.apply(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String escapeFileName(String filename) {
    if (filename == null || filename.length() == 0) {
      return filename;
    }
    return doEscape(filename, PartitionPathEncodeUtils::needsEscapingFilename);
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

  /**
   * Validates that the given (relative) partition path does not contain a directory-traversal
   * segment, throwing {@link HoodieKeyException} if it does. This is enforced regardless of the
   * {@code hoodie.datasource.write.partitionpath.urlencode} setting, since url-encoding is opt-in
   * (disabled by default) and does not escape {@code '.'}, so it never neutralizes {@code ".."}.
   *
   * @param partitionPath the relative partition path (or a single partition field value).
   * @return the same {@code partitionPath} if it is safe.
   * @throws HoodieKeyException if the partition path contains a {@code ".."} traversal segment.
   */
  public static String validateNoPathTraversal(String partitionPath) {
    if (hasPathTraversal(partitionPath)) {
      throw new HoodieKeyException("Invalid partition path \"" + partitionPath + "\": partition paths "
          + "must not contain \"..\" path-traversal segments, which could let a record write Hudi files "
          + "outside the table base path. This is most often caused by an unsanitized data field being "
          + "used as the partition path; sanitize or remap the offending value in the upstream source "
          + "or via a transformer before ingesting it.");
    }
    return partitionPath;
  }

  /**
   * Returns {@code true} if the given (relative) partition path contains a directory-traversal
   * segment (a path segment equal to {@code ".."}). Such a partition path, once resolved against
   * the table base path, can escape the base path and write Hudi-managed files into arbitrary
   * directories reachable by the writer's credentials.
   *
   * <p>The check is intentionally value-content only: it tolerates {@code '.'} inside a segment
   * (e.g. date partitions like {@code 2024.01.01}) and only rejects the standalone {@code ".."}
   * segment. Both forward slash {@code '/'} and the platform-independent literal are treated as
   * separators, since a partition path is always stored using forward slashes.
   *
   * @param partitionPath the relative partition path (or a single partition field value).
   * @return {@code true} if a {@code ".."} traversal segment is present, {@code false} otherwise.
   */
  public static boolean hasPathTraversal(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return false;
    }
    // Normalize backslashes to forward slashes so a "..\.." style value cannot slip through on
    // any platform. Partition paths are always persisted using forward slashes.
    String normalized = partitionPath.replace('\\', '/');
    int start = 0;
    int len = normalized.length();
    while (start <= len) {
      int end = normalized.indexOf('/', start);
      if (end < 0) {
        end = len;
      }
      // A segment of exactly ".." is a traversal segment.
      if (end - start == 2 && normalized.charAt(start) == '.' && normalized.charAt(start + 1) == '.') {
        return true;
      }
      start = end + 1;
    }
    return false;
  }
}
