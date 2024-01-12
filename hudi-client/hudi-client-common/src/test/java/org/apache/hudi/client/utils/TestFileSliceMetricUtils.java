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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFileSliceMetricUtils {

  @Test
  public void testFileSliceMetricUtilsWithoutFile() {
    Map<String, Double> metrics = new HashMap<>();
    List<FileSlice> fileSlices = new ArrayList<>();
    final long defaultBaseFileSize = 10 * 1024 * 1024;
    final double epsilon = 1e-5;
    FileSliceMetricUtils.addFileSliceCommonMetrics(fileSlices, metrics, defaultBaseFileSize);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_READ_MB), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_WRITE_MB), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_MB), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILE_SIZE), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILES), epsilon);
  }

  @Test
  public void testFileSliceMetricUtilsWithoutLogFile() {
    Map<String, Double> metrics = new HashMap<>();
    List<FileSlice> fileSlices = new ArrayList<>();
    final long defaultBaseFileSize = 10 * 1024 * 1024;
    final double epsilon = 1e-5;
    fileSlices.add(buildFileSlice(15 * 1024 * 1024, new ArrayList<>()));
    fileSlices.add(buildFileSlice(20 * 1024 * 1024, new ArrayList<>()));
    fileSlices.add(buildFileSlice(0, new ArrayList<>()));
    FileSliceMetricUtils.addFileSliceCommonMetrics(fileSlices, metrics, defaultBaseFileSize);
    assertEquals(35.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_READ_MB), epsilon);
    assertEquals(45.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_WRITE_MB), epsilon);
    assertEquals(80.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_MB), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILE_SIZE), epsilon);
    assertEquals(0.0, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILES), epsilon);
  }

  @Test
  public void testFileSliceMetricUtilsWithLogFile() {
    Map<String, Double> metrics = new HashMap<>();
    List<FileSlice> fileSlices = new ArrayList<>();
    final long defaultBaseFileSize = 10 * 1024 * 1024;
    final double epsilon = 1e-5;
    fileSlices.add(buildFileSlice(15 * 1024 * 1024,
        new ArrayList<>(Arrays.asList(5 * 1024 * 1024L, 3 * 1024 * 1024L))));
    fileSlices.add(buildFileSlice(20 * 1024 * 1024,
        new ArrayList<>(Collections.singletonList(2 * 1024 * 1024L))));
    FileSliceMetricUtils.addFileSliceCommonMetrics(fileSlices, metrics, defaultBaseFileSize);
    assertEquals(45.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_READ_MB), epsilon);
    assertEquals(35.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_WRITE_MB), epsilon);
    assertEquals(80.0, metrics.get(FileSliceMetricUtils.TOTAL_IO_MB), epsilon);
    assertEquals(10.0 * 1024 * 1024, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILE_SIZE), epsilon);
    assertEquals(3.0, metrics.get(FileSliceMetricUtils.TOTAL_LOG_FILES), epsilon);
  }

  private FileSlice buildFileSlice(long baseFileLen, List<Long> logFileLens) {
    final String baseFilePath = ".b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1";
    FileSlice slice = new FileSlice("partition_0",
        InProcessTimeGenerator.createNewInstantTime(),
        UUID.randomUUID().toString());
    HoodieBaseFile baseFile = new HoodieBaseFile(baseFilePath);
    baseFile.setFileLen(baseFileLen);
    slice.setBaseFile(baseFile);
    int logVersion = 1;
    for (long logFileLen : logFileLens) {
      String logFilePath = "." + UUID.randomUUID().toString() + "_20170101134598.log." + logVersion;
      HoodieLogFile logFile = new HoodieLogFile(logFilePath);
      logFile.setFileLen(logFileLen);
      slice.addLogFile(logFile);
      logVersion++;
    }
    return slice;
  }

}
