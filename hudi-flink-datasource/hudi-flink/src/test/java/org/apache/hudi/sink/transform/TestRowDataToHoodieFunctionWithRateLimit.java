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

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.util.RateLimiter;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRowDataToHoodieFunctionWithRateLimit {

  @Test
  void testLowRateLimitDoesNotBlockAndCloseStopsLimiter() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.WRITE_RATE_LIMIT, 1L);
    config.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    config.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("partition", DataTypes.STRING())).getLogicalType();
    RowDataToHoodieFunctionWithRateLimit<RowData, HoodieFlinkInternalRow> function =
        new RowDataToHoodieFunctionWithRateLimit<>(rowType, config);
    function.setRuntimeContext(new MockStreamingRuntimeContext(false, 4, 0));
    function.open(new Configuration());
    RateLimiter limiter = function.getRateLimiter();

    try {
      assertFalse(limiter.isStopped());
      GenericRowData row = GenericRowData.of(
          StringData.fromString("id-1"), StringData.fromString("p1"));
      assertTimeoutPreemptively(Duration.ofSeconds(1), () -> function.map(row));
    } finally {
      function.close();
    }

    assertTrue(limiter.isStopped());
  }
}
