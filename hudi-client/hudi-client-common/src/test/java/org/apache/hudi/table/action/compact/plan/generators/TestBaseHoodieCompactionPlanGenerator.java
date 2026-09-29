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

package org.apache.hudi.table.action.compact.plan.generators;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestBaseHoodieCompactionPlanGenerator extends HoodieCommonTestHarness {

  @ParameterizedTest
  @CsvSource({"5,2,5,2", "5,10,5,5", "250,200,250,200", "5,10,2,2", "5,2,0,0"})
  void testPlanningParallelism(int partitionCount, int configuredParallelism, int selectedCount, int expectedParallelism) throws Exception {
    initMetaClient();
    HoodieTestTable.of(metaClient).addCommit("001");
    metaClient.reloadActiveTimeline();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionPlanParallelism(configuredParallelism).build())
        .build();
    HoodieEngineContext context = spy(new HoodieLocalEngineContext(getStorageConf()));
    HoodieTable table = mock(HoodieTable.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getConfig()).thenReturn(config);
    SyncableFileSystemView view = mock(SyncableFileSystemView.class);
    List<String> partitions = IntStream.range(0, partitionCount).mapToObj(i -> "partition-" + i).collect(Collectors.toList());
    List<String> selectedPartitions = partitions.subList(0, selectedCount);
    HoodieCompactionPlanGenerator generator = spy(new HoodieCompactionPlanGenerator(table, context, config, null));
    doReturn(partitions).when(generator).getPartitions();
    doReturn(Pair.of(selectedPartitions, partitions.subList(selectedCount, partitionCount)))
        .when(generator).filterPartitionPathsByStrategy(partitions);
    if (selectedCount > 0) {
      when(table.getSliceView()).thenReturn(view);
      when(view.getPendingCompactionOperations()).thenReturn(Stream.empty());
      when(view.getFileGroupsInPendingClustering()).thenReturn(Stream.empty());
      for (String partition : selectedPartitions) {
        when(view.getLatestFileSlicesStateless(partition)).thenReturn(Stream.empty());
      }
    }

    assertNull(generator.generateCompactionPlan("002"));

    if (selectedCount == 0) {
      verify(context, never()).flatMap(anyList(), any(), anyInt());
      verifyNoInteractions(view);
    } else {
      verify(context).flatMap(eq(selectedPartitions), any(), eq(expectedParallelism));
      for (String partition : selectedPartitions) {
        verify(view).getLatestFileSlicesStateless(partition);
      }
    }
  }
}
