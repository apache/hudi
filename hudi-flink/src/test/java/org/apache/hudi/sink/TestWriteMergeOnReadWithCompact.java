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

package org.apache.hudi.sink;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for delta stream write with compaction.
 */
public class TestWriteMergeOnReadWithCompact extends TestWriteCopyOnWrite {

  @Override
  protected void setUp(Configuration conf) {
    // trigger the compaction for every finished checkpoint
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
  }

  @Override
  protected Map<String, String> getExpectedBeforeCheckpointComplete() {
    return EXPECTED1;
  }

  @Test
  public void testAppendOnly() throws Exception {
    conf.setBoolean(FlinkOptions.APPEND_ONLY_ENABLE, true);
    conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    funcWrapper = new StreamWriteFunctionWrapper<>(tempFile.getAbsolutePath(), conf);
    assertThrows(IllegalArgumentException.class, () -> {
      funcWrapper.openFunction();
    }, "APPEND_ONLY mode only support in COPY_ON_WRITE table");
  }

  protected Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    // MOR mode merges the messages with the same key.
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
    return expected;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
