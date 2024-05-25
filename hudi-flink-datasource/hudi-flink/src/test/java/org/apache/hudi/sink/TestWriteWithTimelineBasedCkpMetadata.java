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

package org.apache.hudi.sink;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Test cases for timeline based checkpoint metadata.
 */
public class TestWriteWithTimelineBasedCkpMetadata extends TestWriteBase {

  @Override
  protected void setUp(Configuration conf) {
    conf.setBoolean(HoodieWriteConfig.INSTANT_STATE_TIMELINE_SERVER_BASED.key(), true);
  }

  @Test
  public void testTimelineBasedCkpMetadataFailover() throws Exception {
    // reset the config option
    conf.setString(FlinkOptions.OPERATION, "insert");
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1);

    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(1)
        // stop the timeline server by close write client
        .stopTimelineServer()
        .handleEvents(1)
        .checkpointComplete(1)
        // will still be able to write and checkpoint
        .checkWrittenData(EXPECTED4, 1)
        .consume(TestData.DATA_SET_INSERT_SAME_KEY)
        .checkpoint(2)
        .handleEvents(1)
        .checkpointComplete(2)
        .checkWrittenDataCOW(EXPECTED5)
        .end();
  }
}
