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

package org.apache.hudi.client.transaction;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConflictResolutionStrategyUtil {

  public static void createCommit(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static HoodieCommitMetadata createCommitMetadata(String instantTime, String writeFileName) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(writeFileName);
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }

  public static HoodieCommitMetadata createCommitMetadata(String instantTime) {
    return createCommitMetadata(instantTime, "file-1");
  }

  public static void createInflightCommit(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
        .addInflightCommit(instantTime)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createCompactionRequested(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    HoodieCompactionPlan compactionPlan = new HoodieCompactionPlan();
    compactionPlan.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieCompactionOperation operation = new HoodieCompactionOperation();
    operation.setFileId(fileId1);
    operation.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    operation.setDataFilePath("/file-1");
    operation.setDeltaFilePaths(Arrays.asList("/file-1"));
    compactionPlan.setOperations(Arrays.asList(operation));
    HoodieTestTable.of(metaClient)
        .addRequestedCompaction(instantTime, compactionPlan);
  }

  public static void createCompaction(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createReplaceRequested(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId1 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.name());
    HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
    HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
    HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
    sliceInfo.setFileId(fileId1);
    sliceInfo.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    clusteringGroup.setSlices(Arrays.asList(sliceInfo));
    clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
    requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
    requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieTestTable.of(metaClient)
        .addRequestedReplace(instantTime, Option.of(requestedReplaceMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createReplaceInflight(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata inflightReplaceMetadata = new HoodieCommitMetadata();
    inflightReplaceMetadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    inflightReplaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    HoodieTestTable.of(metaClient)
        .addInflightReplace(instantTime, Option.of(inflightReplaceMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createReplace(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId1 as deleted
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> partitionFileIds = new HashMap<>();
    partitionFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, Arrays.asList(fileId2));
    replaceMetadata.setPartitionToReplaceFileIds(partitionFileIds);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    replaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    replaceMetadata.setOperationType(writeOperationType);
    // create replace instant to mark fileId1 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.name());
    HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
    HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
    HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
    sliceInfo.setFileId(fileId1);
    sliceInfo.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    clusteringGroup.setSlices(Arrays.asList(sliceInfo));
    clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
    requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
    requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, Option.of(requestedReplaceMetadata), Option.empty(), replaceMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createPendingReplace(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";
    // create replace instant to mark fileId2 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.name());
    HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
    HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
    HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
    sliceInfo.setFileId(fileId2);
    sliceInfo.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    clusteringGroup.setSlices(Arrays.asList(sliceInfo));
    clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
    requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
    requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieTestTable.of(metaClient)
        .addPendingReplace(instantTime, Option.of(requestedReplaceMetadata), Option.empty())
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createCompleteReplace(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId2 as deleted
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> partitionFileIds = new HashMap<>();
    partitionFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, Arrays.asList(fileId2));
    replaceMetadata.setPartitionToReplaceFileIds(partitionFileIds);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-2");
    replaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    replaceMetadata.setOperationType(writeOperationType);
    FileCreateUtils.createReplaceCommit(metaClient.getBasePath().toString(), instantTime, replaceMetadata);
  }

  public static void createPendingCompaction(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    HoodieCompactionPlan compactionPlan = new HoodieCompactionPlan();
    compactionPlan.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieCompactionOperation operation = new HoodieCompactionOperation();
    operation.setFileId(fileId1);
    operation.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    operation.setDataFilePath("/file-1");
    operation.setDeltaFilePaths(Arrays.asList("/file-1-log1"));
    compactionPlan.setOperations(Arrays.asList(operation));
    HoodieTestTable.of(metaClient)
        .addRequestedCompaction(instantTime, compactionPlan);
    FileCreateUtils.createPendingInflightCompaction(metaClient.getBasePath().toString(), instantTime);
  }

  public static void createCompleteCompaction(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-2");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createRequestedCommit(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    HoodieTestTable.of(metaClient)
        .addInflightCommit(instantTime);
  }

  public static void createCompleteCommit(String instantTime, HoodieTableMetaClient metaClient) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-2");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  public static void createReplaceInflight(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) throws Exception {
    Option<HoodieCommitMetadata> inflightReplaceMetadata = Option.empty();
    if (WriteOperationType.INSERT_OVERWRITE.equals(writeOperationType)) {
      inflightReplaceMetadata = Option.of(createReplaceCommitMetadata(WriteOperationType.INSERT_OVERWRITE));
    }
    HoodieTestTable.of(metaClient)
        .addInflightReplace(instantTime, inflightReplaceMetadata);
  }

  private static HoodieReplaceCommitMetadata createReplaceCommitMetadata(WriteOperationType writeOperationType) {
    String fileId1 = "file-1";
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> partitionFileIds = new HashMap<>();
    partitionFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, Arrays.asList(fileId1));
    replaceMetadata.setPartitionToReplaceFileIds(partitionFileIds);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-2");
    replaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    replaceMetadata.setOperationType(writeOperationType);
    return replaceMetadata;
  }
}
