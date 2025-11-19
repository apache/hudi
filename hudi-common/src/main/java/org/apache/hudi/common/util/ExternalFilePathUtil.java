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

import java.util.Arrays;

/**
 * Utility methods for handling externally created files.
 */
public class ExternalFilePathUtil {
  // Suffix acts as a marker when appended to a file path that the path was created by an external system and not a Hudi writer.
  private static final String EXTERNAL_FILE_SUFFIX = "_hudiext";

  /**
   * Appends the commit time and external file marker to the file path. Hudi relies on the commit time in the file name for properly generating views of the files in a table.
   * @param filePath The original file path
   * @param commitTime The time of the commit that added this file to the table
   * @return The file path with this additional information appended
   */
  public static String appendCommitTimeAndExternalFileMarker(String filePath, String commitTime) {
    return filePath + "_" + commitTime + EXTERNAL_FILE_SUFFIX;
  }

  public static String appendCommitTimeAndExternalFileMarker(String filePath, String commitTime, String partitionPath) {
    return filePath + "_" + commitTime + "_" + getNumPartitionLevels(partitionPath) + "_uv" + EXTERNAL_FILE_SUFFIX;
  }

  private static int getNumPartitionLevels(String partitionPath) {
    if(StringUtils.isNullOrEmpty(partitionPath)) {
      return 0;
    }

    return (int) Arrays.stream(partitionPath.split("/")).filter(p -> !StringUtils.isNullOrEmpty(p)).count();
  }

  /**
   * Checks if the file name was created by an external system by checking for the external file marker at the end of the file name.
   * @param fileName The file name
   * @return True if the file was created by an external system, false otherwise
   */
  public static boolean isExternallyCreatedFile(String fileName) {
    return fileName.endsWith(EXTERNAL_FILE_SUFFIX);
  }
}
