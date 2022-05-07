/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestSparkClusteringPlanPartitionFilter {
  @Mock
  HoodieSparkCopyOnWriteTable table;
  @Mock
  HoodieSparkEngineContext context;
  HoodieWriteConfig.Builder hoodieWriteConfigBuilder;

  @BeforeEach
  public void setUp() {
    this.hoodieWriteConfigBuilder = HoodieWriteConfig
            .newBuilder()
            .withPath("Fake_Table_Path");
  }

  @Test
  public void testFilterPartitionNoFilter() {
    HoodieWriteConfig config = hoodieWriteConfigBuilder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.NONE)
            .build())
        .build();

    PartitionAwareClusteringPlanStrategy sg = new SparkSizeBasedClusteringPlanStrategy(table, context, config);
    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20210718");
    fakeTimeBasedPartitionsPath.add("20210716");
    fakeTimeBasedPartitionsPath.add("20210719");
    List list = sg.filterPartitionPaths(fakeTimeBasedPartitionsPath);
    assertEquals(3, list.size());
  }

  @Test
  public void testFilterPartitionRecentDays() {
    HoodieWriteConfig config = hoodieWriteConfigBuilder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringSkipPartitionsFromLatest(1)
            .withClusteringTargetPartitions(1)
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .build();

    PartitionAwareClusteringPlanStrategy sg = new SparkSizeBasedClusteringPlanStrategy(table, context, config);
    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20210718");
    fakeTimeBasedPartitionsPath.add("20210716");
    fakeTimeBasedPartitionsPath.add("20210719");
    List list = sg.filterPartitionPaths(fakeTimeBasedPartitionsPath);
    assertEquals(1, list.size());
    assertSame("20210718", list.get(0));
  }

  @Test
  public void testFilterPartitionSelectedPartitions() {
    HoodieWriteConfig config = hoodieWriteConfigBuilder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPartitionFilterBeginPartition("20211222")
            .withClusteringPartitionFilterEndPartition("20211223")
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS)
            .build())
        .build();

    PartitionAwareClusteringPlanStrategy sg = new SparkSizeBasedClusteringPlanStrategy(table, context, config);
    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20211220");
    fakeTimeBasedPartitionsPath.add("20211221");
    fakeTimeBasedPartitionsPath.add("20211222");
    fakeTimeBasedPartitionsPath.add("20211224");
    List list = sg.filterPartitionPaths(fakeTimeBasedPartitionsPath);
    assertEquals(1, list.size());
    assertSame("20211222", list.get(0));
  }
}
