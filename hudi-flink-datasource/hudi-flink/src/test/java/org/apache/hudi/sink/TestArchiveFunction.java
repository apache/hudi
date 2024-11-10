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

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestArchiveFunction {
  private HoodieActiveTimeline timeline;
  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testSyncedPartitions() throws Exception {
    // Step1: build 4 completed HoodieInstant in active timeline
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "001");
    HoodieInstant instant1Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001");
    HoodieInstant instant1Completed = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");

    HoodieInstant instant2 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    HoodieInstant instant2Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "002");
    HoodieInstant instant2Completed = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "002");

    HoodieInstant instant3 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "003");
    HoodieInstant instant3Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "003");
    HoodieInstant instant3Completed = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003");

    HoodieInstant instant4 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "004");
    HoodieInstant instant4Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "004");
    HoodieInstant instant4Completed = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "004");

    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    FlinkWriteClients.createWriteClient(conf);
    timeline = table.getActiveTimeline();
    timeline.createNewInstant(instant1);
    timeline.createNewInstant(instant1Inflight);
    timeline.createCompleteInstant(instant1Completed);

    timeline.createNewInstant(instant2);
    timeline.createNewInstant(instant2Inflight);
    timeline.createCompleteInstant(instant2Completed);

    timeline.createNewInstant(instant3);
    timeline.createNewInstant(instant3Inflight);
    timeline.createCompleteInstant(instant3Completed);

    timeline.createNewInstant(instant4);
    timeline.createNewInstant(instant4Inflight);
    timeline.createCompleteInstant(instant4Completed);

    // Step2: execute async archive
    conf.setString(HoodieArchivalConfig.ASYNC_ARCHIVE.key(), "true");
    conf.setString(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "1");
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, 3);
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, 2);
    ArchiveFunction archiveFunction = new ArchiveFunction(conf);
    OneInputStreamOperatorTestHarness operatorTestHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap(archiveFunction));
    operatorTestHarness.open();
    archiveFunction.snapshotState(new MockFunctionSnapshotContext(-1));
    archiveFunction.notifyCheckpointComplete(-1);
    // wait for archive finished
    while (archiveFunction.isArchiving()) {
      continue;
    }

    // Step3: check whether the result of archive is correct
    timeline = timeline.reload();
    assertTrue(timeline.getCommitsTimeline().countInstants() == 2);
  }
}