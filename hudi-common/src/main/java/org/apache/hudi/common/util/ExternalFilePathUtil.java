/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

/**
 * Utility methods for handling externally created files.
 */
public class ExternalFilePathUtil {
  // Suffix acts as a marker when appended to a file path that the path was created by an external system and not a Hudi writer.
  private static final String EXTERNAL_FILE_SUFFIX = "_hudiext";
  // Marker for file group prefix in external file names. URL-encoded "_fg=" to avoid conflicts with file names.
  private static final String FILE_GROUP_PREFIX_MARKER = "_fg%3D";

  /**
   * Appends the commit time and external file marker to the file path. Hudi relies on the commit time in the file name for properly generating views of the files in a table.
   * @param filePath The original file path
   * @param commitTime The time of the commit that added this file to the table
   * @return The file path with this additional information appended
   */
  public static String appendCommitTimeAndExternalFileMarker(String filePath, String commitTime) {
    return filePath + "_" + commitTime + EXTERNAL_FILE_SUFFIX;
  }

  /**
   * Appends the commit time, file group prefix, and external file marker to the file name.
   * Use this when the external file is located in a subdirectory within the partition (e.g., bucket-0/file.parquet).
   *
   * @param fileName The original file name (without any path prefix)
   * @param commitTime The time of the commit that added this file to the table
   * @param externalFileGroupPrefix The prefix path where the file is located (e.g., "bucket-0"). Can be null or empty.
   * @return The file name with commit time and markers appended
   */
  public static String appendCommitTimeAndExternalFileMarker(String fileName, String commitTime, String externalFileGroupPrefix) {
    if (externalFileGroupPrefix == null || externalFileGroupPrefix.isEmpty()) {
      return appendCommitTimeAndExternalFileMarker(fileName, commitTime);
    }
    String encodedPrefix = PartitionPathEncodeUtils.escapePathName(externalFileGroupPrefix);
    return fileName + "_" + commitTime + FILE_GROUP_PREFIX_MARKER + encodedPrefix + EXTERNAL_FILE_SUFFIX;
  }

  /**
   * Checks if the file name was created by an external system by checking for the external file marker at the end of the file name.
   * @param fileName The file name
   * @return True if the file was created by an external system, false otherwise
   */
  public static boolean isExternallyCreatedFile(String fileName) {
    return fileName.endsWith(EXTERNAL_FILE_SUFFIX);
  }

  /**
   * Checks if the external file name contains a file group prefix.
   * @param fileName The file name
   * @return True if the file has a file group prefix encoded in its name
   */
  public static boolean hasExternalFileGroupPrefix(String fileName) {
    return isExternallyCreatedFile(fileName) && fileName.contains(FILE_GROUP_PREFIX_MARKER);
  }

  /**
   * Extracts the file group prefix from an external file name.
   * @param fileName The external file name
   * @return Option containing the decoded file group prefix, or empty if not present
   */
  public static Option<String> getExternalFileGroupPrefix(String fileName) {
    if (!hasExternalFileGroupPrefix(fileName)) {
      return Option.empty();
    }
    int start = fileName.indexOf(FILE_GROUP_PREFIX_MARKER) + FILE_GROUP_PREFIX_MARKER.length();
    int end = fileName.lastIndexOf(EXTERNAL_FILE_SUFFIX);
    return Option.of(PartitionPathEncodeUtils.unescapePathName(fileName.substring(start, end)));
  }

  /**
   * Gets the original file path including the file group prefix if present.
   * For example, "data.parquet_123_fg%3Dbucket-0_hudiext" returns "bucket-0/data.parquet"
   *
   * @param fileName The external file name
   * @return The original file path with prefix
   */
  public static String getOriginalFilePath(String fileName) {
    if (!isExternallyCreatedFile(fileName)) {
      return fileName;
    }
    String originalName = getOriginalFileName(fileName);
    Option<String> prefix = getExternalFileGroupPrefix(fileName);
    return prefix.map(p -> p + "/" + originalName).orElse(originalName);
  }

  /**
   * Extracts the original file name from an external file name (without commit time and markers).
   * For example, "data.parquet_123_hudiext" returns "data.parquet"
   * And "data.parquet_123_fg%3Dbucket-0_hudiext" also returns "data.parquet"
   *
   * @param fileName The external file name
   * @return The original file name
   */
  public static String getOriginalFileName(String fileName) {
    if (!isExternallyCreatedFile(fileName)) {
      return fileName;
    }
    int markerEnd = hasExternalFileGroupPrefix(fileName)
        ? fileName.indexOf(FILE_GROUP_PREFIX_MARKER)
        : fileName.lastIndexOf(EXTERNAL_FILE_SUFFIX);
    int commitTimeStart = fileName.lastIndexOf('_', markerEnd - 1);
    return fileName.substring(0, commitTimeStart);
  }
}
