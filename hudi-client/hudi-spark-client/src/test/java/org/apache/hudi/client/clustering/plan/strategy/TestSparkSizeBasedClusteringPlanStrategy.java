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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkSizeBasedClusteringPlanStrategy {

  @Mock
  HoodieSparkCopyOnWriteTable table;
  @Mock
  HoodieSparkEngineContext context;

  @Test
  public void testBuildClusteringGroup() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(2000)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(500)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategy = new SparkSizeBasedClusteringPlanStrategy(table, context, config);

    ArrayList<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(200));
    fileSlices.add(createFileSlice(200));
    fileSlices.add(createFileSlice(300));
    fileSlices.add(createFileSlice(300));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));

    Stream<HoodieClusteringGroup> clusteringGroupStream = planStrategy.buildClusteringGroupsForPartition("p0", fileSlices);
    List<HoodieClusteringGroup> clusteringGroups = clusteringGroupStream.collect(Collectors.toList());

    // FileSlices will be divided into two clusteringGroups
    assertEquals(2, clusteringGroups.size());

    // First group: 400, 400, 400, 400, 300, and they will be merged into 2 files
    assertEquals(5, clusteringGroups.get(0).getSlices().size());
    assertEquals(2, clusteringGroups.get(0).getNumOutputFileGroups());

    // Second group: 300, 200, 200, and they will be merged into 1 file
    assertEquals(3, clusteringGroups.get(1).getSlices().size());
    assertEquals(1, clusteringGroups.get(1).getNumOutputFileGroups());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBuildClusteringGroupsForPartitionOnlyOneFile(boolean enableSingleGroupClustering) {
    String partition = "20221117";
    String fileId = "fg-1";
    List<FileSlice> fileSliceGroups = new ArrayList<>();
    fileSliceGroups.add(generateFileSlice(partition, fileId, "0"));
    // test buildClusteringGroupsForPartition with ClusteringSortColumns config
    HoodieWriteConfig configWithSortEnabled = HoodieWriteConfig.newBuilder().withPath("").withClusteringConfig(
            HoodieClusteringConfig.newBuilder()
                .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.NONE)
                .withSingleGroupClusteringEnabled(enableSingleGroupClustering)
                .withClusteringSortColumns("f0")
                .build())
        .build();
    SparkSizeBasedClusteringPlanStrategy strategyWithSortEnabled = new SparkSizeBasedClusteringPlanStrategy(table, context, configWithSortEnabled);
    Stream<HoodieClusteringGroup> groupStreamSort = strategyWithSortEnabled.buildClusteringGroupsForPartition(partition, fileSliceGroups);
    assertEquals(enableSingleGroupClustering ? 1 : 0, groupStreamSort.count());
  }

  private FileSlice generateFileSlice(String partitionPath, String fileId, String baseInstant) {
    FileSlice fs = new FileSlice(new HoodieFileGroupId(partitionPath, fileId), baseInstant);
    fs.setBaseFile(new HoodieBaseFile(FSUtils.makeBaseFileName(baseInstant, "1-0-1", fileId)));
    return fs;
  }

  private FileSlice createFileSlice(long baseFileSize) {
    String fileId = FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0);
    FileSlice fs = new FileSlice("p0", "001", fileId);
    HoodieBaseFile f = new HoodieBaseFile(fileId);
    f.setFileLen(baseFileSize);
    fs.setBaseFile(f);
    return fs;
  }
}
