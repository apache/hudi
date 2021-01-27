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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ClusteringUtils}.
 */
public class TestClusteringUtils extends HoodieCommonTestHarness {

  private static final String CLUSTERING_STRATEGY_CLASS = "org.apache.hudi.DefaultClusteringStrategy";
  private static final Map<String, String> STRATEGY_PARAMS = new HashMap<String, String>() {
    {
      put("sortColumn", "record_key");
    }
  };

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testClusteringPlanMultipleInstants() throws Exception {
    String partitionPath1 = "partition1";
    List<String> fileIds1 = new ArrayList<>();
    fileIds1.add(UUID.randomUUID().toString());
    fileIds1.add(UUID.randomUUID().toString());
    String clusterTime1 = "1";
    createRequestedReplaceInstant(partitionPath1, clusterTime1, fileIds1);

    List<String> fileIds2 = new ArrayList<>();
    fileIds2.add(UUID.randomUUID().toString());
    fileIds2.add(UUID.randomUUID().toString());
    fileIds2.add(UUID.randomUUID().toString());

    List<String> fileIds3 = new ArrayList<>();
    fileIds3.add(UUID.randomUUID().toString());

    String clusterTime = "2";
    createRequestedReplaceInstant(partitionPath1, clusterTime, fileIds2, fileIds3);

    // create replace.requested without clustering plan. this instant should be ignored by ClusteringUtils
    createRequestedReplaceInstantNotClustering("3");

    // create replace.requested without any metadata content. This instant should be ignored by ClusteringUtils
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "4"));

    metaClient.reloadActiveTimeline();
    assertEquals(4, metaClient.getActiveTimeline().filterPendingReplaceTimeline().countInstants());

    Map<HoodieFileGroupId, HoodieInstant> fileGroupToInstantMap =
        ClusteringUtils.getAllFileGroupsInPendingClusteringPlans(metaClient);
    assertEquals(fileIds1.size() + fileIds2.size() + fileIds3.size(), fileGroupToInstantMap.size());
    validateClusteringInstant(fileIds1, partitionPath1, clusterTime1, fileGroupToInstantMap);
    validateClusteringInstant(fileIds2, partitionPath1, clusterTime, fileGroupToInstantMap);
    validateClusteringInstant(fileIds3, partitionPath1, clusterTime, fileGroupToInstantMap);
  }

  // replacecommit.inflight doesnt have clustering plan. 
  // Verify that getClusteringPlan fetches content from corresponding requested file.
  @Test
  public void testClusteringPlanInflight() throws Exception {
    String partitionPath1 = "partition1";
    List<String> fileIds1 = new ArrayList<>();
    fileIds1.add(UUID.randomUUID().toString());
    fileIds1.add(UUID.randomUUID().toString());
    String clusterTime1 = "1";
    HoodieInstant requestedInstant = createRequestedReplaceInstant(partitionPath1, clusterTime1, fileIds1);
    HoodieInstant inflightInstant = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant, Option.empty());
    HoodieClusteringPlan requestedClusteringPlan = ClusteringUtils.getClusteringPlan(metaClient, requestedInstant).get().getRight();
    HoodieClusteringPlan inflightClusteringPlan = ClusteringUtils.getClusteringPlan(metaClient, inflightInstant).get().getRight();
    assertEquals(requestedClusteringPlan, inflightClusteringPlan);
  }

  private void validateClusteringInstant(List<String> fileIds, String partitionPath,
                                         String expectedInstantTime, Map<HoodieFileGroupId, HoodieInstant> fileGroupToInstantMap) {
    for (String fileId : fileIds) {
      assertEquals(expectedInstantTime, fileGroupToInstantMap.get(new HoodieFileGroupId(partitionPath, fileId)).getTimestamp());
    }
  }

  private HoodieInstant createRequestedReplaceInstantNotClustering(String instantTime) throws IOException {
    HoodieInstant newRequestedInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.UNKNOWN.name()).build();
    metaClient.getActiveTimeline().saveToPendingReplaceCommit(newRequestedInstant, TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
    return newRequestedInstant;
  }

  private HoodieInstant createRequestedReplaceInstant(String partitionPath1, String clusterTime, List<String>... fileIds) throws IOException {
    List<FileSlice>[] fileSliceGroups = new List[fileIds.length];
    for (int i = 0; i < fileIds.length; i++) {
      fileSliceGroups[i] = fileIds[i].stream().map(fileId -> generateFileSlice(partitionPath1, fileId, "0")).collect(Collectors.toList());
    }

    HoodieClusteringPlan clusteringPlan =
        ClusteringUtils.createClusteringPlan(CLUSTERING_STRATEGY_CLASS, STRATEGY_PARAMS, fileSliceGroups, Collections.emptyMap());

    HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, clusterTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(clusteringPlan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    metaClient.getActiveTimeline().saveToPendingReplaceCommit(clusteringInstant, TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
    return clusteringInstant;
  }

  private FileSlice generateFileSlice(String partitionPath, String fileId, String baseInstant) {
    FileSlice fs = new FileSlice(new HoodieFileGroupId(partitionPath, fileId), baseInstant);
    fs.setBaseFile(new HoodieBaseFile(FSUtils.makeBaseFileName(baseInstant, "1-0-1", fileId)));
    return fs;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
