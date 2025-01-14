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

package org.apache.hudi.table;

import org.apache.hudi.HoodieTestCommitGenerator;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBaseTableServicePlanActionExecutor {

  @TempDir
  File tempFile;
  private static final StorageConfiguration<?> CONF = getDefaultStorageConf();
  private final HoodieEngineContext context = new HoodieLocalEngineContext(CONF);

  /**
   * Clustering commit : cl
   * Commit : c
   * |requestTime<---commit type--->completionTime|(written partitions in current commit)
   *
   * ---------------------------------------------------------------------------------------------------> timeline
   * |0<------cl1------>2|
   *              |1<-----------c2--------->5|(part=0001)
   *                               |4<-----------c3--------->6|(part=0004)
   *                                                                      |7<-------------------c4----------------->9|(part=0007)
   *                                                                                                                   | start incr clustering at 10
   */
  @Test
  void testExecutorWithMultiWriter() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();

    // <requestTime, CompletionTime>
    HashMap<String, String> instants = new HashMap<>();
    instants.put("0000", "0002"); // clustering 1
    instants.put("0001", "0005"); // commit 2
    instants.put("0004", "0006"); // commit 3
    instants.put("0007", "0009"); // commit 4

    prepareTimeline(metaClient, instants);

    DummyTableServicePlanActionExecutor executor = new DummyTableServicePlanActionExecutor(context, writeConfig, getMockHoodieTable(metaClient), "0008");
    Set<String> incrementalPartitions = (Set<String>)executor.getIncrementalPartitions(TableServiceType.CLUSTER).getRight();

    assertEquals(incrementalPartitions.size(), 2);
    assertTrue(incrementalPartitions.contains("0001"));
    assertTrue(incrementalPartitions.contains("0004"));
    assertFalse(incrementalPartitions.contains("0007"));
  }

  private void prepareTimeline(HoodieTableMetaClient metaClient, HashMap<String, String> instants) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    instants.forEach((requestTime, completionTime) -> {
      try {
        if (requestTime.equalsIgnoreCase("0000")) {
          Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
              generateReplaceCommitMetadata("0000", requestTime, UUID.randomUUID().toString(), UUID.randomUUID().toString());
          testTable.addCluster("0000", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue(), completionTime);

        } else if (!requestTime.equalsIgnoreCase("0003")) {
          HoodieCommitMetadata metadata = testTable.createCommitMetadata(requestTime, WriteOperationType.INSERT, Collections.singletonList(requestTime), 10, false);
          testTable.addCommit(requestTime, Option.of(completionTime), Option.of(metadata));
        }
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
  }

  private HoodieTable getMockHoodieTable(HoodieTableMetaClient metaClient) {
    HoodieTable hoodieTable = mock(HoodieTable.class);
    TaskContextSupplier taskContextSupplier = mock(TaskContextSupplier.class);
    when(taskContextSupplier.getPartitionIdSupplier()).thenReturn(() -> 1);
    when(hoodieTable.getTaskContextSupplier()).thenReturn(taskContextSupplier);
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(hoodieTable.getActiveTimeline()).thenReturn(metaClient.getActiveTimeline());
    return hoodieTable;
  }

  private Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> generateReplaceCommitMetadata(
      String instantTime, String partition, String replacedFileId, String newFileId) {
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.toString());
    requestedReplaceMetadata.setVersion(1);
    HoodieSliceInfo sliceInfo = HoodieSliceInfo.newBuilder().setFileId(replacedFileId).build();
    List<HoodieClusteringGroup> clusteringGroups = new ArrayList<>();
    clusteringGroups.add(HoodieClusteringGroup.newBuilder()
        .setVersion(1).setNumOutputFileGroups(1).setMetrics(Collections.emptyMap())
        .setSlices(Collections.singletonList(sliceInfo)).build());
    requestedReplaceMetadata.setExtraMetadata(Collections.emptyMap());
    requestedReplaceMetadata.setClusteringPlan(HoodieClusteringPlan.newBuilder()
        .setVersion(1).setExtraMetadata(Collections.emptyMap())
        .setStrategy(HoodieClusteringStrategy.newBuilder().setStrategyClassName("").setVersion(1).build())
        .setInputGroups(clusteringGroups).build());

    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.addReplaceFileId(partition, replacedFileId);
    replaceMetadata.setOperationType(WriteOperationType.CLUSTER);
    if (!StringUtils.isNullOrEmpty(newFileId)) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partition);
      writeStat.setPath(partition + "/" + HoodieTestCommitGenerator.getBaseFilename(instantTime, newFileId));
      writeStat.setFileId(newFileId);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      replaceMetadata.addWriteStat(partition, writeStat);
    }
    return Pair.of(requestedReplaceMetadata, replaceMetadata);
  }

  class DummyTableServicePlanActionExecutor<T, I, K, O, R> extends BaseTableServicePlanActionExecutor<T, I, K, O, R> {

    public DummyTableServicePlanActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               HoodieTable<T, I, K, O> table,
                                               String instantTime) {
      super(context, config, table, instantTime);
    }

    @Override
    public R execute() {
      return null;
    }
  }
}
