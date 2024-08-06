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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;

import java.util.List;
import java.util.Map;

/**
 * A utility class for calculating metrics related to FileSlice.
 */
public class FileSliceMetricUtils {

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_IO_WRITE_MB = "TOTAL_IO_WRITE_MB";
  public static final String TOTAL_IO_MB = "TOTAL_IO_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  // >>>>>>>>> For Sort Merge Join Compaction >>>>>>>>> //
  public static final String TOTAL_BASE_FILES = "TOTAL_BASE_FILES";
  public static final String TOTAL_BASE_FILE_SIZE = "TOTAL_BASE_FILE_SIZE";
  // <<<<<<<<< For Sort Merge Join Compaction <<<<<<<<< //

  public static void addFileSliceCommonMetrics(List<FileSlice> fileSlices, Map<String, Double> metrics, long defaultBaseFileSize) {
    int numLogFiles = 0;
    long totalLogFileSize = 0;
    long totalIORead = 0;
    long totalIOWrite = 0;
    long totalIO = 0;
    // >>>>>>>>> For Sort Merge Join Compaction >>>>>>>>> //
    int numBaseFiles = 0;
    long totalBaseFileSize = 0;
    // <<<<<<<<< For Sort Merge Join Compaction <<<<<<<<< //

    for (FileSlice slice : fileSlices) {
      numLogFiles += slice.getLogFiles().count();
      // Total size of all the log files
      totalLogFileSize += slice.getLogFiles().map(HoodieLogFile::getFileSize).filter(size -> size >= 0)
          .reduce(Long::sum).orElse(0L);
      long baseFileSize = slice.getBaseFile().isPresent() ? slice.getBaseFile().get().getFileSize() : 0L;
      // >>>>>>>>> For Sort Merge Join Compaction >>>>>>>>> //
      numBaseFiles += slice.getBaseFile().isPresent() ? 1 : 0;
      totalBaseFileSize += baseFileSize;
      // <<<<<<<<< For Sort Merge Join Compaction <<<<<<<<< //
      totalIORead += baseFileSize;
      // Total write will be similar to the size of the base file
      totalIOWrite += baseFileSize > 0 ? baseFileSize : defaultBaseFileSize;
    }
    // Total read will be the base file + all the log files
    totalIORead = FSUtils.getSizeInMB(totalIORead + totalLogFileSize);
    totalIOWrite = FSUtils.getSizeInMB(totalIOWrite);

    // Total IO will be the IO for read + write
    totalIO = totalIORead + totalIOWrite;

    metrics.put(TOTAL_IO_READ_MB, (double) totalIORead);
    metrics.put(TOTAL_IO_WRITE_MB, (double) totalIOWrite);
    metrics.put(TOTAL_IO_MB, (double) totalIO);
    metrics.put(TOTAL_LOG_FILE_SIZE, (double) totalLogFileSize);
    metrics.put(TOTAL_LOG_FILES, (double) numLogFiles);

    // >>>>>>>>> For Sort Merge Join Compaction >>>>>>>>> //
    metrics.put(TOTAL_BASE_FILES, (double) numBaseFiles);
    metrics.put(TOTAL_BASE_FILE_SIZE, (double) totalBaseFileSize);
    // <<<<<<<<< For Sort Merge Join Compaction <<<<<<<<< //
  }
}
