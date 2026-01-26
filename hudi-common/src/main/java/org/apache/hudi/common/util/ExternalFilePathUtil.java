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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

/**
 * Utility methods for handling externally created files.
 * <p>
 * External files are files created outside Hudi (by external formats) that are later
 * registered in a Hudi table. To distinguish these files from Hudi-generated files and
 * to include commit time information, they are marked with a special suffix.
 * <p>
 * Hudi supports two formats for external files:
 * <p>
 * <b>Format 1: Without file group prefix</b>
 * <pre>{@code <originalFileName>_<commitTime>_hudiext}</pre>
 * Example: {@code data.parquet_20240101000000_hudiext}
 * <ul>
 *   <li>fileId: {@code data.parquet}</li>
 *   <li>commitTime: {@code 20240101000000}</li>
 * </ul>
 * <p>
 * <b>Format 2: With file group prefix</b>
 * <pre>{@code <originalFileName>_<commitTime>_fg%3D<encodedPrefix>_hudiext}</pre>
 * Example: {@code data.parquet_20240101000000_fg%3Dbucket-0_hudiext}
 * <ul>
 *   <li>fileId: {@code bucket-0/data.parquet} (includes prefix path)</li>
 *   <li>commitTime: {@code 20240101000000}</li>
 *   <li>prefix: {@code bucket-0} (URL-decoded from {@code _fg%3D} marker)</li>
 * </ul>
 * <p>
 * The file group prefix format is used when external files are organized in subdirectories
 * within a partition (e.g., bucketed files). The prefix is URL-encoded to avoid conflicts
 * with special characters in directory names.
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
   * Extracts the file group prefix from an external file name.
   * @param fileName The external file name
   * @return Option containing the decoded file group prefix, or empty if not present
   */
  private static Option<String> getExternalFileGroupPrefix(String fileName) {
    if (!isExternallyCreatedFile(fileName)) {
      return Option.empty();
    }
    int prefixMarkerIndex = fileName.indexOf(FILE_GROUP_PREFIX_MARKER);
    if (prefixMarkerIndex == -1) {
      return Option.empty();
    }
    int start = prefixMarkerIndex + FILE_GROUP_PREFIX_MARKER.length();
    int end = fileName.lastIndexOf(EXTERNAL_FILE_SUFFIX);
    return Option.of(PartitionPathEncodeUtils.unescapePathName(fileName.substring(start, end)));
  }

  /**
   * Extracts the original file name from an external file name (without commit time and markers).
   * For example, "data.parquet_123_hudiext" returns "data.parquet"
   * And "data.parquet_123_fg%3Dbucket-0_hudiext" also returns "data.parquet"
   *
   * @param fileName The external file name
   * @return The original file name
   */
  private static String getOriginalFileName(String fileName) {
    if (!isExternallyCreatedFile(fileName)) {
      return fileName;
    }
    int prefixMarkerIndex = fileName.indexOf(FILE_GROUP_PREFIX_MARKER);
    int markerEnd = prefixMarkerIndex != -1
        ? prefixMarkerIndex
        : fileName.lastIndexOf(EXTERNAL_FILE_SUFFIX);
    int commitTimeStart = fileName.lastIndexOf('_', markerEnd - 1);
    return fileName.substring(0, commitTimeStart);
  }

  /**
   * Adjusts the parent path for external files with file group prefix.
   * For files with file group prefix, the prefix represents subdirectories within the partition,
   * so we need to remove the prefix portion to get the actual partition path.
   * Supports arbitrary nesting depths (e.g., "bucket-0/subdir1/subdir2").
   *
   * @param parent the parent path
   * @param fileName the file name to check
   * @return the adjusted parent path
   */
  public static StoragePath getFullPathOfPartition(StoragePath parent, String fileName) {
    return getExternalFileGroupPrefix(fileName)
        .map(prefix -> new StoragePath(parent.toString().substring(0, parent.toString().length() - prefix.length() - 1)))
        .orElse(parent);
  }

  /**
   * Parses external file names to extract fileId and commit time.
   * Handles both formats:
   *   - With prefix: originalName_commitTime_fg%3D<prefix>_hudiext -> fileId = prefix/originalName
   *   - Without prefix: originalName_commitTime_hudiext -> fileId = originalName
   *
   * @param fileName The external file name to parse
   * @return String array of size 2: [fileId, commitTime]
   */
  public static String[] parseFileIdAndCommitTimeFromExternalFile(String fileName) {
    String[] values = new String[2];
    // Extract original file name
    String originalName = getOriginalFileName(fileName);
    // Extract file group prefix (if present)
    Option<String> prefix = getExternalFileGroupPrefix(fileName);
    // Build fileId
    values[0] = prefix.map(p -> p + "/" + originalName).orElse(originalName);
    // Extract commitTime
    int prefixMarkerIndex = fileName.indexOf(FILE_GROUP_PREFIX_MARKER);
    int markerEnd = prefixMarkerIndex != -1
        ? prefixMarkerIndex
        : fileName.lastIndexOf(EXTERNAL_FILE_SUFFIX);
    int commitTimeStart = fileName.lastIndexOf('_', markerEnd - 1);
    values[1] = fileName.substring(commitTimeStart + 1, markerEnd);
    return values;
  }

  /**
   * If the file was created externally, the original file path will have a '_[commitTime]_hudiext' suffix when stored in the metadata table. That suffix needs to be removed from the FileStatus so
   * that the actual file can be found and read.
   *
   * @param pathInfo an input path info that may require updating
   * @param fileId   the fileId for the file
   * @return the original file status if it was not externally created, or a new FileStatus with the original file name if it was externally created
   */
  public static StoragePathInfo maybeHandleExternallyGeneratedFileName(StoragePathInfo pathInfo,
                                                                        String fileId) {
    if (pathInfo == null) {
      return null;
    }
    if (isExternallyCreatedFile(pathInfo.getPath().getName())) {
      String fileName = pathInfo.getPath().getName();
      StoragePath parent = getFullPathOfPartition(pathInfo.getPath().getParent(), fileName);
      return new StoragePathInfo(
          new StoragePath(parent, fileId), pathInfo.getLength(), pathInfo.isDirectory(),
          pathInfo.getBlockReplication(), pathInfo.getBlockSize(), pathInfo.getModificationTime(), pathInfo.getLocations());
    } else {
      return pathInfo;
    }
  }
}
