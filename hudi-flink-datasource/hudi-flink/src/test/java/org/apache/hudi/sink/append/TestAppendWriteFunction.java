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

/**
 * Test cases for {@link AppendWriteFunction}.
 */
public class TestAppendWriteFunction {

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
}
