/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.sink;

import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.ttl.strategy.KeepByTimeStrategy;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.fixInstantTimeCompatibility;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.instantTimePlusMillis;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for partition TTL.
 */
public class TestWriterWithPartitionTTl extends TestWriteBase {
  // The origin PartitionTTLStrategy calculate the expire time by DAYs, it's too long for test.
  // Override the method isPartitionExpired to calculate expire time by minutes.
  public static class FlinkPartitionTTLTestStrategy extends KeepByTimeStrategy {
    public FlinkPartitionTTLTestStrategy(HoodieTable hoodieTable, String instantTime) {
      super(hoodieTable, instantTime);
    }

    @Override
    protected boolean isPartitionExpired(String referenceTime) {
      String expiredTime = instantTimePlusMillis(referenceTime, ttlInMilis / 24 / 3600);
      return fixInstantTimeCompatibility(instantTime).compareTo(expiredTime) > 0;
    }
  }

  @Override
  protected void setUp(Configuration conf) {
    conf.setString(HoodieTTLConfig.INLINE_PARTITION_TTL.key(), "true");
    conf.setString(HoodieTTLConfig.DAYS_RETAIN.key(), "1");
    conf.setString(HoodieTTLConfig.PARTITION_TTL_STRATEGY_CLASS_NAME.key(), FlinkPartitionTTLTestStrategy.class.getName());
  }

  @Test
  public void testFlinkWriterWithPartitionTTL() throws Exception {
    // open the function and ingest data
    preparePipeline(conf)
        .consume(TestData.DATA_SET_PART1)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .end();

    preparePipeline(conf)
        .consume(TestData.DATA_SET_PART2)
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .end();

    HoodieActiveTimeline timeline = StreamerUtil.createMetaClient(conf).getActiveTimeline();
    assertTrue(timeline.getCompletedReplaceTimeline().getInstants().size() > 0);
    HoodieInstant replaceCommit = timeline.getCompletedReplaceTimeline().getInstants().get(0);
    HoodieReplaceCommitMetadata commitMetadata = timeline.readReplaceCommitMetadataToAvro(replaceCommit);
    assertTrue(commitMetadata.getPartitionToReplaceFileIds().containsKey("par1"));
  }
}
