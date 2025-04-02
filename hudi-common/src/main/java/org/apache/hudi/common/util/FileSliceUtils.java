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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;

import java.util.List;
import java.util.stream.Collectors;

public class FileSliceUtils {

  /**
   * Get the total file size of a file slice in parquet format.
   * For the log file, we need to convert its size to the estimated size in the parquet format in a certain proportion
   */
  public static long getTotalFileSizeAsParquetFormat(FileSlice fileSlice, double logFileFraction) {
    long logFileSize = convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()), logFileFraction);
    return fileSlice.getBaseFile().isPresent() ? fileSlice.getBaseFile().get().getFileSize() + logFileSize
        : logFileSize;
  }

  private static long convertLogFilesSizeToExpectedParquetSize(List<HoodieLogFile> hoodieLogFiles, double logFileFraction) {
    long totalSizeOfLogFiles =
        hoodieLogFiles.stream()
            .map(HoodieLogFile::getFileSize)
            .filter(size -> size > 0)
            .reduce(Long::sum)
            .orElse(0L);
    return (long) (totalSizeOfLogFiles * logFileFraction);
  }

}
