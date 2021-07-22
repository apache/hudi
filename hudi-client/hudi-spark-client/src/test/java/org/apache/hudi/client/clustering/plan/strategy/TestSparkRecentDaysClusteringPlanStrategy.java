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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestSparkRecentDaysClusteringPlanStrategy {
  @Mock
  HoodieSparkCopyOnWriteTable table;
  @Mock
  HoodieSparkEngineContext context;
  HoodieWriteConfig hoodieWriteConfig;

  @BeforeEach
  public void setUp() {
    this.hoodieWriteConfig = HoodieWriteConfig
            .newBuilder()
            .withPath("Fake_Table_Path")
            .withClusteringConfig(HoodieClusteringConfig
                    .newBuilder()
                    .withClusteringSkipPartitionsFromLatest(1)
                    .withClusteringTargetPartitions(1)
                    .build())
            .build();
  }

  @Test
  public void testFilterPartitionPaths() {
    SparkRecentDaysClusteringPlanStrategy sg = new SparkRecentDaysClusteringPlanStrategy(table, context, hoodieWriteConfig);
    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20210718");
    fakeTimeBasedPartitionsPath.add("20210716");
    fakeTimeBasedPartitionsPath.add("20210719");
    List list = sg.filterPartitionPaths(fakeTimeBasedPartitionsPath);
    assertEquals(1, list.size());
    assertSame("20210718", list.get(0));
  }
}
