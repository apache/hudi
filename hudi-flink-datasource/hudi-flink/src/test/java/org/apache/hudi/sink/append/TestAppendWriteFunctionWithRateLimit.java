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

import org.apache.hudi.common.util.RateLimiter;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAppendWriteFunctionWithRateLimit {

  @Test
  void testCloseStopsRateLimiter() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.WRITE_RATE_LIMIT, 1L);
    AppendWriteFunctionWithRateLimit<RowData> function =
        new AppendWriteFunctionWithRateLimit<>(TestConfigurations.ROW_TYPE, config);
    function.setRuntimeContext(new MockStreamingRuntimeContext(false, 4, 0));
    function.open(new Configuration());
    RateLimiter limiter = function.getRateLimiter();

    assertFalse(limiter.isStopped());
    function.close();
    function.close();
    assertTrue(limiter.isStopped());
  }
}
