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

package org.apache.hudi.utils;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link org.apache.hudi.util.CompactionUtil}.
 */
public class TestCompactionUtil {

  private HoodieFlinkTable<?> table;
  private HoodieTableMetaClient metaClient;
  private Configuration conf;

  @TempDir
  File tempFile;

  void beforeEach() throws IOException {
    beforeEach(Collections.emptyMap());
  }

  void beforeEach(Map<String, String> options) throws IOException {
    this.conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.forEach((k, v) -> conf.setString(k, v));

    StreamerUtil.initTableIfNotExists(conf);

    this.table = FlinkTables.createTable(conf);
    this.metaClient = table.getMetaClient();
    // initialize the metadata table path
    if (conf.getBoolean(FlinkOptions.METADATA_ENABLED)) {
      FlinkHoodieBackedTableMetadataWriter.create(table.getHadoopConf(), table.getConfig(),
          table.getContext(), Option.empty(), Option.empty());
    }
  }

  @Test
  void rollbackCompaction() throws Exception {
    beforeEach();
    List<String> oriInstants = IntStream.range(0, 3)
        .mapToObj(i -> generateCompactionPlan()).collect(Collectors.toList());
    List<HoodieInstant> instants = metaClient.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.INFLIGHT)
        .getInstants()
        .collect(Collectors.toList());
    assertThat("all the instants should be in pending state", instants.size(), is(3));
    CompactionUtil.rollbackCompaction(table);
    boolean allRolledBack = metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants()
        .allMatch(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    assertTrue(allRolledBack, "all the instants should be rolled back");
    List<String> actualInstants = metaClient.getActiveTimeline()
        .filterPendingCompactionTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(actualInstants, is(oriInstants));
  }

  @Test
  void rollbackEarliestCompaction() throws Exception {
    beforeEach();
    conf.setInteger(FlinkOptions.COMPACTION_TIMEOUT_SECONDS, 0);
    List<String> oriInstants = IntStream.range(0, 3)
        .mapToObj(i -> generateCompactionPlan()).collect(Collectors.toList());
    List<HoodieInstant> instants = metaClient.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.INFLIGHT)
        .getInstants()
        .collect(Collectors.toList());
    assertThat("all the instants should be in pending state", instants.size(), is(3));
    CompactionUtil.rollbackEarliestCompaction(table, conf);
    long requestedCnt = metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED).count();
    assertThat("Only the first instant expects to be rolled back", requestedCnt, is(1L));

    String instantTime = metaClient.getActiveTimeline()
        .filterPendingCompactionTimeline().filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED)
        .firstInstant().get().getTimestamp();
    assertThat(instantTime, is(oriInstants.get(0)));
  }

  @Test
  void testScheduleCompaction() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "false");
    options.put(FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key(), FlinkOptions.TIME_ELAPSED);
    options.put(FlinkOptions.COMPACTION_DELTA_SECONDS.key(), "0");
    beforeEach(options);

    // write a commit with data first
    TestData.writeDataAsBatch(TestData.DATA_SET_SINGLE_INSERT, conf);

    HoodieFlinkWriteClient<?> writeClient = StreamerUtil.createWriteClient(conf);
    CompactionUtil.scheduleCompaction(metaClient, writeClient, true, true);

    Option<HoodieInstant> pendingCompactionInstant = metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().lastInstant();
    assertTrue(pendingCompactionInstant.isPresent(), "A compaction plan expects to be scheduled");

    // write another commit with data and start a new instant
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    TimeUnit.SECONDS.sleep(3); // in case the instant time interval is too close
    writeClient.startCommit();

    CompactionUtil.scheduleCompaction(metaClient, writeClient, true, false);
    int numCompactionCommits = metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants();
    assertThat("Two compaction plan expects to be scheduled", numCompactionCommits, is(2));
  }

  /**
   * Generates a compaction plan on the timeline and returns its instant time.
   */
  private String generateCompactionPlan() {
    HoodieCompactionOperation operation = new HoodieCompactionOperation();
    HoodieCompactionPlan plan = new HoodieCompactionPlan(Collections.singletonList(operation), Collections.emptyMap(), 1);
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    HoodieInstant compactionInstant =
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    try {
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          TimelineMetadataUtils.serializeCompactionPlan(plan));
      table.getActiveTimeline().transitionCompactionRequestedToInflight(compactionInstant);
    } catch (IOException ioe) {
      throw new HoodieIOException("Exception scheduling compaction", ioe);
    }
    metaClient.reloadActiveTimeline();
    return instantTime;
  }
}

