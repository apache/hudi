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

package org.apache.hudi.sink.append;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.StreamerUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.hudi.metrics.FlinkStreamWriteMetrics;

import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link AppendWriteFunction}.
 */
public class TestAppendWriteFunction {

  private FlinkStreamWriteMetrics flinkStreamWriteMetrics;
  private UnregisteredMetricsGroup metricGroup;

  @BeforeEach
  void setUp() {
    metricGroup = new UnregisteredMetricsGroup();
    flinkStreamWriteMetrics = new FlinkStreamWriteMetrics(metricGroup);
  }

  @Test
  void testRecordWriteNoFailure() {
    WriteStatus writeStatus = new WriteStatus();
    List<WriteStatus> writeStatusList = Collections.singletonList(writeStatus);

    Configuration configuration = new Configuration();
    StreamerUtil.validateWriteStatus(configuration, HoodieInstantTimeGenerator.getCurrentInstantTimeStr(), writeStatusList);
  }

  @Test
  void testRecordWriteFailureValidationWithoutFailFast() {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.markFailure(
            "key1", "/partition1", new RuntimeException("test exception"));
    List<WriteStatus> writeStatusList = Collections.singletonList(writeStatus);

    Configuration configuration = new Configuration();
    StreamerUtil.validateWriteStatus(configuration, HoodieInstantTimeGenerator.getCurrentInstantTimeStr(), writeStatusList);
  }

  @Test
  void testRecordWriteFailureValidationWithFailFast() {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.markFailure(
        "key1", "/partition1", new RuntimeException("test exception"));
    List<WriteStatus> writeStatusList = Collections.singletonList(writeStatus);

    Configuration configuration = new Configuration();
    configuration.set(FlinkOptions.WRITE_FAIL_FAST, true);

    // Verify that the failure was recorded in metrics
    assertThrows(HoodieException.class,
        () -> StreamerUtil.validateWriteStatus(configuration, HoodieInstantTimeGenerator.getCurrentInstantTimeStr(), writeStatusList));
  }

  @Test
  void testRecordWriteFailure() {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.markFailure(
        "key1", "/partition1", new RuntimeException("test exception"));
    List<WriteStatus> writeStatusList = Arrays.asList(writeStatus);

    AppendWriteFunction.recordWriteFailure(flinkStreamWriteMetrics, writeStatusList);

    // Verify that the failure was recorded in metrics
    assertEquals(1, flinkStreamWriteMetrics.getNumOfRecordWriteFailures());
  }

  @Test
  void testRecordWriteFailureMultipleErrors() {
    WriteStatus writeStatus1 = new WriteStatus();
    writeStatus1.markFailure("key1", "/partition1", new RuntimeException("error 1"));
    writeStatus1.markFailure("key2", "/partition1", new RuntimeException("error 2"));
    writeStatus1.markFailure("key3", "/partition1", new RuntimeException("error 3"));

    WriteStatus writeStatus2 = new WriteStatus();
    writeStatus2.markFailure("key4", "/partition2", new IllegalArgumentException("illegal argument"));
    writeStatus2.markFailure("key5", "/partition2", new NullPointerException("null pointer"));

    List<WriteStatus> writeStatusList = Arrays.asList(writeStatus1, writeStatus2);

    AppendWriteFunction.recordWriteFailure(flinkStreamWriteMetrics, writeStatusList);

    // Should record total 5 failures across both write statuses
    assertEquals(5, flinkStreamWriteMetrics.getNumOfRecordWriteFailures());
  }
}
