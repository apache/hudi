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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link org.apache.hudi.util.CompactionUtil}.
 */
public class TestCompactionUtil {

  @TempDir
  File tempFile;

  @Test
  void rollbackCompaction() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, 0);

    StreamerUtil.initTableIfNotExists(conf);

    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    HoodieFlinkTable table = writeClient.getHoodieTable();
    HoodieTableMetaClient metaClient = table.getMetaClient();

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
    HoodieInstant instant = metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().orElse(null);
    assertThat(instant.getTimestamp(), is(instantTime));

    CompactionUtil.rollbackCompaction(table, conf);
    HoodieInstant rollbackInstant = table.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().get();
    assertThat(rollbackInstant.getState(), is(HoodieInstant.State.REQUESTED));
    assertThat(rollbackInstant.getTimestamp(), is(instantTime));
  }
}

