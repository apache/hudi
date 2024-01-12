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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ClusteringUtil}.
 */
public class TestClusteringUtil {

  private HoodieFlinkTable<?> table;
  private HoodieTableMetaClient metaClient;
  private HoodieFlinkWriteClient<?> writeClient;
  private Configuration conf;

  @TempDir
  File tempFile;

  void beforeEach() throws IOException {
    beforeEach(Collections.emptyMap());
  }

  @AfterEach
  void afterEach() {
    if (this.writeClient != null) {
      this.writeClient.close();
    }
  }

  void beforeEach(Map<String, String> options) throws IOException {
    this.conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    options.forEach((k, v) -> conf.setString(k, v));

    StreamerUtil.initTableIfNotExists(conf);

    this.table = FlinkTables.createTable(conf);
    this.metaClient = table.getMetaClient();
    this.writeClient = FlinkWriteClients.createWriteClient(conf);
    // init the metadata table if it is enabled
    if (this.writeClient.getConfig().isMetadataTableEnabled()) {
      this.writeClient.initMetadataTable();
    }
  }

  @Test
  void rollbackClustering() throws Exception {
    beforeEach();
    List<String> oriInstants = IntStream.range(0, 3)
        .mapToObj(i -> generateClusteringPlan()).collect(Collectors.toList());
    List<HoodieInstant> instants = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient())
        .stream().filter(instant -> instant.getState() == HoodieInstant.State.INFLIGHT)
        .collect(Collectors.toList());
    assertThat("all the instants should be in pending state", instants.size(), is(3));
    ClusteringUtil.rollbackClustering(table, writeClient);
    boolean allRolledBack = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient())
        .stream().allMatch(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    assertTrue(allRolledBack, "all the instants should be rolled back");
    List<String> actualInstants = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient())
        .stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    assertThat(actualInstants, is(oriInstants));
  }
  
  @Test
  void validateClusteringScheduling() throws Exception {
    beforeEach();
    ClusteringUtil.validateClusteringScheduling(this.conf);
    
    // validate bucket index
    this.conf.setString(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    ClusteringUtil.validateClusteringScheduling(this.conf);
  }

  /**
   * Generates a clustering plan on the timeline and returns its instant time.
   */
  private String generateClusteringPlan() {
    HoodieClusteringGroup group = new HoodieClusteringGroup();
    HoodieClusteringPlan plan = new HoodieClusteringPlan(Collections.singletonList(group),
        HoodieClusteringStrategy.newBuilder().build(), Collections.emptyMap(), 1, false);
    HoodieRequestedReplaceMetadata metadata = new HoodieRequestedReplaceMetadata(WriteOperationType.CLUSTER.name(),
        plan, Collections.emptyMap(), 1);
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    HoodieInstant clusteringInstant =
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
    try {
      metaClient.getActiveTimeline().saveToPendingReplaceCommit(clusteringInstant,
          TimelineMetadataUtils.serializeRequestedReplaceMetadata(metadata));
      table.getActiveTimeline().transitionReplaceRequestedToInflight(clusteringInstant, Option.empty());
    } catch (IOException ioe) {
      throw new HoodieIOException("Exception scheduling clustering", ioe);
    }
    metaClient.reloadActiveTimeline();
    return instantTime;
  }
}
