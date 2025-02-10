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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
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
import org.apache.hudi.table.action.IncrementalPartitionAwareStrategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
   * ---------------------------------------------------------------------------------------------------------------------------------------------> timeline
   * |0<-c1->1|(part=0000)
   *           |2<------cl2------>4|(part=0000)
   *                        |3<-----------c3--------->6|(part=0003)
   *                                         |5<-----------c4--------->7|(part=0005)
   *                                                                                |8<-------------------c5----------------->N|(part=0008)
   *                                                                                                             | start incr clustering at 9
   */
  @Test
  public void testExecutorWithMultiWriter() throws Exception {
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
    instants.put("0000", "0001"); // commit 1
    instants.put("0002", "0004"); // clustering 1
    instants.put("0003", "0006"); // commit 2
    instants.put("0005", "0007"); // commit 3

    prepareTimeline(metaClient, instants, Collections.singletonList("0008"));

    DummyTableServicePlanActionExecutor executor = new DummyTableServicePlanActionExecutor(context, writeConfig, getMockHoodieTable(metaClient), "0009");
    Set<String> incrementalPartitions = (Set<String>)executor.getIncrementalPartitions(TableServiceType.CLUSTER).getRight();

    assertEquals(2, incrementalPartitions.size());
    assertTrue(incrementalPartitions.contains("0003"));
    assertTrue(incrementalPartitions.contains("0005"));
    assertFalse(incrementalPartitions.contains("0008"));
  }

  @Test
  public void testGetPartitionsFallbackToFullScan() throws Exception {
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
    instants.put("0000", "0001");
    instants.put("0002", "0004");
    instants.put("0003", "0006");
    instants.put("0005", "0007");
    instants.put("0008", "0010");

    prepareTimeline(metaClient, instants, Collections.emptyList());
    instants.keySet().forEach(instant -> {
      try {
        FileCreateUtilsLegacy.createPartitionMetaFile(tablePath, instant);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    DummyStrategy incrementalStrategy = new DummyStrategy();
    DummyTableServicePlanActionExecutor executor = new DummyTableServicePlanActionExecutor(context, writeConfig, getMockHoodieTable(metaClient), "0009");
    List<String> partitions = (List<String>) executor.getPartitions(incrementalStrategy, TableServiceType.CLUSTER);
    assertEquals(2, partitions.size());

    // Simulation archive commit instant 0000 and clustering instant 0002
    Path path = new Path(tablePath);
    FileSystem fs = path.getFileSystem(new Configuration());
    StoragePath timelinePath = metaClient.getTimelinePath();
    Arrays.stream(fs.listStatus(new Path(timelinePath.toString()))).forEach(instant -> {
      if (instant.getPath().toString().contains("0000") || instant.getPath().toString().contains("0002")) {
        try {
          fs.delete(instant.getPath(), true);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // fall back to get all partitions
    List<String> allPartitions = (List<String>) executor.getPartitions(incrementalStrategy, TableServiceType.CLUSTER);
    assertEquals(allPartitions.stream().sorted().collect(Collectors.toList()), instants.keySet().stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void testContinuousEmptyCommits() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + StoragePath.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);

    // create clustering commit 0001_0002
    String clusteringRequestTime = "0001";
    String clusteringCompletionTIme = "0002";
    Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
        generateReplaceCommitMetadata(clusteringRequestTime, clusteringRequestTime, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    testTable.addCluster(clusteringRequestTime, replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue(), clusteringCompletionTIme);

    HoodieInstant clusteringInstant = metaClient.getActiveTimeline().filterCompletedInstants().getLastClusteringInstant().get();
    FileCreateUtilsLegacy.createPartitionMetaFile(tablePath, clusteringInstant.requestedTime());

    // create empty commit 0003_004
    String emptyCommitRequestTime = "0003";
    String emptyCommitCompletionTIme = "0004";
    HoodieCommitMetadata metadata = testTable.createCommitMetadata(emptyCommitRequestTime, WriteOperationType.INSERT, Collections.emptyList(), 0, false);
    testTable.addCommit(emptyCommitRequestTime, Option.of(emptyCommitCompletionTIme), Option.of(metadata));

    // create empty commit 0005_0006
    String emptyCommitRequestTime2 = "0005";
    String emptyCommitCompletionTIme2 = "0006";
    HoodieCommitMetadata metadata2 = testTable.createCommitMetadata(emptyCommitRequestTime2, WriteOperationType.INSERT, Collections.emptyList(), 0, false);
    testTable.addCommit(emptyCommitRequestTime2, Option.of(emptyCommitCompletionTIme2), Option.of(metadata2));

    // executor.getPartitions should return empty list
    DummyStrategy incrementalStrategy = new DummyStrategy();
    DummyTableServicePlanActionExecutor executor = new DummyTableServicePlanActionExecutor(context, writeConfig, getMockHoodieTable(metaClient), "0009");
    List partitions = executor.getPartitions(incrementalStrategy, TableServiceType.CLUSTER);
    assertTrue(partitions.isEmpty());
  }

  private void prepareTimeline(HoodieTableMetaClient metaClient, HashMap<String, String> commitInstants,
                               List<String> requestInstants) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    commitInstants.forEach((requestTime, completionTime) -> {
      try {
        if (requestTime.equalsIgnoreCase("0002")) {
          Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
              generateReplaceCommitMetadata(requestTime, requestTime, UUID.randomUUID().toString(), UUID.randomUUID().toString());
          testTable.addCluster(requestTime, replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue(), completionTime);
        } else if (!requestTime.equalsIgnoreCase("0004")) {
          HoodieCommitMetadata metadata = testTable.createCommitMetadata(requestTime, WriteOperationType.INSERT, Collections.singletonList(requestTime), 10, false);
          testTable.addCommit(requestTime, Option.of(completionTime), Option.of(metadata));
        }
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });

    requestInstants.forEach(instant -> {
      try {
        testTable.addRequestedCommit(instant);
        FileCreateUtilsLegacy.createPartitionMetaFile(metaClient.getBasePath().toString(), instant);
      } catch (Exception e) {
        throw new RuntimeException(e);
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

  class DummyStrategy implements IncrementalPartitionAwareStrategy {

    @Override
    public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitions) {
      return null;
    }
  }
}
