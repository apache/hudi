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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPartitionAwareClusteringPlanStrategy {

  @Mock
  HoodieTable table;
  @Mock
  HoodieEngineContext context;
  HoodieWriteConfig hoodieWriteConfig;

  @BeforeEach
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("hoodie.clustering.plan.strategy.partition.regex.pattern", "2021072.*");
    this.hoodieWriteConfig = HoodieWriteConfig
        .newBuilder()
        .withPath("dummy_Table_Path")
        .withClusteringConfig(HoodieClusteringConfig
            .newBuilder()
            .fromProperties(props)
            .build())
        .build();
  }

  @Test
  public void testFilterPartitionPaths() {
    PartitionAwareClusteringPlanStrategy strategyTestRegexPattern = new DummyPartitionAwareClusteringPlanStrategy(table, context, hoodieWriteConfig);

    ArrayList<String> fakeTimeBasedPartitionsPath = new ArrayList<>();
    fakeTimeBasedPartitionsPath.add("20210718");
    fakeTimeBasedPartitionsPath.add("20210715");
    fakeTimeBasedPartitionsPath.add("20210723");
    fakeTimeBasedPartitionsPath.add("20210716");
    fakeTimeBasedPartitionsPath.add("20210719");
    fakeTimeBasedPartitionsPath.add("20210721");

    List list = strategyTestRegexPattern.getMatchedPartitions(hoodieWriteConfig, fakeTimeBasedPartitionsPath);
    assertEquals(2, list.size());
    assertTrue(list.contains("20210721"));
    assertTrue(list.contains("20210723"));
  }

  class DummyPartitionAwareClusteringPlanStrategy extends PartitionAwareClusteringPlanStrategy {

    public DummyPartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List list) {
      return null;
    }

    @Override
    protected Map<String, String> getStrategyParams() {
      return null;
    }
  }
}
