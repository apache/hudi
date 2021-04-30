/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.transform;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RowDataToHoodieFunction}.
 */
public class TestRowDataToHoodieFunction {
  @TempDir
  File tempFile;

  private Configuration conf;

  @BeforeEach
  public void before() {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
  }

  @Test
  void testRateLimit() throws Exception {
    // at most 100 record per second
    RowDataToHoodieFunction<RowData, ?> func1 = getFunc(100);
    long instant1 = System.currentTimeMillis();
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      func1.map(rowData);
    }
    long instant2 = System.currentTimeMillis();
    long processTime1 = instant2 - instant1;

    // at most 1 record per second
    RowDataToHoodieFunction<RowData, ?> func2 = getFunc(1);
    long instant3 = System.currentTimeMillis();
    for (RowData rowData : TestData.DATA_SET_INSERT_DUPLICATES) {
      func2.map(rowData);
    }
    long instant4 = System.currentTimeMillis();
    long processTime2 = instant4 - instant3;

    assertTrue(processTime2 > processTime1, "lower rate should have longer process time");
    assertTrue(processTime2 > 5000, "should process at least 5 seconds");
  }

  private RowDataToHoodieFunction<RowData, ?> getFunc(long rate) throws Exception {
    conf.setLong(FlinkOptions.WRITE_RATE_LIMIT, rate);
    RowDataToHoodieFunction<RowData, ?> func =
        new RowDataToHoodieFunction<>(TestConfigurations.ROW_TYPE, conf);
    func.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 1));
    func.open(conf);
    return func;
  }
}
