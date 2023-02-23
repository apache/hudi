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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.clustering.plan.strategy.FlinkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link FlinkSizeBasedClusteringPlanStrategy}.
 */
public class TestFlinkSizeBasedClusteringPlanStrategy {

  @Mock
  HoodieFlinkCopyOnWriteTable table;
  @Mock
  HoodieFlinkEngineContext context;
  HoodieWriteConfig.Builder hoodieWriteConfigBuilder;

  @BeforeEach
  public void setUp() {
    this.hoodieWriteConfigBuilder = HoodieWriteConfig
            .newBuilder()
            .withPath("path1");
  }

  @Test
  public void testBuildClusteringGroupsForPartitionOnlyOneFile() {
    String partition = "20221117";
    String fileId = "fg-1";
    List<FileSlice> fileSliceGroups = new ArrayList<>();
    fileSliceGroups.add(generateFileSlice(partition, fileId, "0"));
    // test buildClusteringGroupsForPartition with ClusteringSortColumns config
    HoodieWriteConfig configWithSortEnabled = hoodieWriteConfigBuilder.withClusteringConfig(
        HoodieClusteringConfig.newBuilder()
          .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.NONE)
          .withSingleGroupClusteringEnabled(false)
          .withClusteringSortColumns("f0")
          .build())
        .build();
    PartitionAwareClusteringPlanStrategy strategyWithSortEnabled = new FlinkSizeBasedClusteringPlanStrategy(table, context, configWithSortEnabled);
    Stream<HoodieClusteringGroup> groupStreamSort = strategyWithSortEnabled.buildClusteringGroupsForPartition(partition,fileSliceGroups);
    assertEquals(1, groupStreamSort.count());

    // test buildClusteringGroupsForPartition without ClusteringSortColumns config
    HoodieWriteConfig configWithSortDisabled = hoodieWriteConfigBuilder.withClusteringConfig(
        HoodieClusteringConfig.newBuilder()
          .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.NONE)
          .withSingleGroupClusteringEnabled(false)
          .withClusteringSortColumns("")
          .build())
        .build();
    PartitionAwareClusteringPlanStrategy strategyWithSortDisabled = new FlinkSizeBasedClusteringPlanStrategy(table, context, configWithSortDisabled);
    Stream<HoodieClusteringGroup> groupStreamWithOutSort = strategyWithSortDisabled.buildClusteringGroupsForPartition(partition,fileSliceGroups);
    assertEquals(0, groupStreamWithOutSort.count());
  }

  private FileSlice generateFileSlice(String partitionPath, String fileId, String baseInstant) {
    FileSlice fs = new FileSlice(new HoodieFileGroupId(partitionPath, fileId), baseInstant);
    fs.setBaseFile(new HoodieBaseFile(FSUtils.makeBaseFileName(baseInstant, "1-0-1", fileId)));
    return fs;
  }
}
