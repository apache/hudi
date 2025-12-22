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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    Option<HoodieInstant> lastPendingClustering = metaClient.getActiveTimeline().getLastPendingClusterInstant();
    assertTrue(lastPendingClustering.isPresent());
    assertEquals("2", lastPendingClustering.get().getTimestamp());

    //check that it still gets picked if it is inflight
    HoodieInstant inflight = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(lastPendingClustering.get(), Option.empty());
    assertEquals(HoodieInstant.State.INFLIGHT, inflight.getState());
    lastPendingClustering = metaClient.reloadActiveTimeline().getLastPendingClusterInstant();
    assertEquals("2", lastPendingClustering.get().getTimestamp());

    //now that it is complete, the first instant should be picked
    HoodieInstant complete = metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflight, Option.empty());
    assertEquals(HoodieInstant.State.COMPLETED, complete.getState());
    lastPendingClustering = metaClient.reloadActiveTimeline().getLastPendingClusterInstant();
    assertEquals("1", lastPendingClustering.get().getTimestamp());
  }

  // replacecommit.inflight doesn't have clustering plan.
  // Verify that getClusteringPlan fetches content from corresponding requested file.
  @Disabled("Will fail due to avro issue AVRO-3789. This is fixed in avro 1.11.3")
  @Test
  public void testClusteringPlanInflight() throws Exception {
    String partitionPath1 = "partition1";
    List<String> fileIds1 = new ArrayList<>();
    fileIds1.add(UUID.randomUUID().toString());
    fileIds1.add(UUID.randomUUID().toString());
    String clusterTime1 = "1";
    HoodieInstant requestedInstant = createRequestedReplaceInstant(partitionPath1, clusterTime1, fileIds1);
    HoodieInstant inflightInstant = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant, Option.empty());
    assertTrue(ClusteringUtils.isClusteringInstant(metaClient.getActiveTimeline(), requestedInstant));
    HoodieClusteringPlan requestedClusteringPlan = ClusteringUtils.getClusteringPlan(metaClient, requestedInstant).get().getRight();
    assertTrue(ClusteringUtils.isClusteringInstant(metaClient.getActiveTimeline(), inflightInstant));
    HoodieClusteringPlan inflightClusteringPlan = ClusteringUtils.getClusteringPlan(metaClient, inflightInstant).get().getRight();
    assertEquals(requestedClusteringPlan, inflightClusteringPlan);
  }

  @Test
  public void testGetOldestInstantToRetainForClustering() throws IOException {
    String partitionPath1 = "partition1";
    List<String> fileIds1 = new ArrayList<>();
    fileIds1.add(UUID.randomUUID().toString());
    String clusterTime1 = "1";
    HoodieInstant requestedInstant1 = createRequestedReplaceInstant(partitionPath1, clusterTime1, fileIds1);
    HoodieInstant inflightInstant1 = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant1, Option.empty());
    metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflightInstant1, Option.empty());
    List<String> fileIds2 = new ArrayList<>();
    fileIds2.add(UUID.randomUUID().toString());
    fileIds2.add(UUID.randomUUID().toString());
    String clusterTime2 = "2";
    HoodieInstant requestedInstant2 = createRequestedReplaceInstant(partitionPath1, clusterTime2, fileIds2);
    HoodieInstant inflightInstant2 = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant2, Option.empty());
    metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflightInstant2, Option.empty());
    List<String> fileIds3 = new ArrayList<>();
    fileIds3.add(UUID.randomUUID().toString());
    fileIds3.add(UUID.randomUUID().toString());
    fileIds3.add(UUID.randomUUID().toString());
    String clusterTime3 = "3";
    HoodieInstant requestedInstant3 = createRequestedReplaceInstant(partitionPath1, clusterTime3, fileIds3);
    HoodieInstant inflightInstant3 = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant3, Option.empty());
    HoodieInstant completedInstant3 = metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflightInstant3, Option.empty());
    metaClient.reloadActiveTimeline();
    Option<HoodieInstant> actual = ClusteringUtils.getOldestInstantToRetainForClustering(metaClient.getActiveTimeline(), metaClient);
    assertTrue(actual.isPresent());
    assertEquals(clusterTime1, actual.get().getTimestamp(), "no clean in timeline, retain first replace commit");

    String cleanTime1 = "4";
    HoodieInstant requestedInstant4 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime1);
    HoodieCleanerPlan cleanerPlan1 = HoodieCleanerPlan.newBuilder()
        .setEarliestInstantToRetainBuilder(HoodieActionInstant.newBuilder()
            .setAction(completedInstant3.getAction())
            .setTimestamp(completedInstant3.getTimestamp())
            .setState(completedInstant3.getState().name()))
        .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
        .setFilesToBeDeletedPerPartition(new HashMap<>())
        .setVersion(CleanPlanV2MigrationHandler.VERSION)
        .build();
    metaClient.getActiveTimeline().saveToCleanRequested(requestedInstant4, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan1));
    HoodieInstant inflightInstant4 = metaClient.getActiveTimeline().transitionCleanRequestedToInflight(requestedInstant4, Option.empty());
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(cleanTime1, 1L, 1,
        completedInstant3.getTimestamp(), "", Collections.emptyMap(), 0, Collections.emptyMap(), Collections.emptyMap());
    metaClient.getActiveTimeline().transitionCleanInflightToComplete(inflightInstant4,
        TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata));
    metaClient.reloadActiveTimeline();
    actual = ClusteringUtils.getOldestInstantToRetainForClustering(metaClient.getActiveTimeline(), metaClient);
    assertEquals(clusterTime3, actual.get().getTimestamp(),
        "retain the first replace commit after the earliestInstantToRetain ");
  }

  /** test getOldestInstantToRetainForClustering with KEEP_LATEST_FILE_VERSIONS as clean policy */
  @Test
  public void testGetOldestInstantToRetainForClusteringKeepFileVersion() throws IOException {
    String partitionPath1 = "partition1";
    List<String> fileIds1 = new ArrayList<>();
    fileIds1.add(UUID.randomUUID().toString());
    String clusterTime1 = "1";
    HoodieInstant requestedInstant1 = createRequestedReplaceInstant(partitionPath1, clusterTime1, fileIds1);
    HoodieInstant inflightInstant1 = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant1, Option.empty());
    metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflightInstant1, Option.empty());

    String cleanTime1 = "2";
    HoodieInstant requestedInstant2 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime1);
    HoodieCleanerPlan cleanerPlan1 = new HoodieCleanerPlan(null, clusterTime1,
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name(), Collections.emptyMap(),
        CleanPlanV2MigrationHandler.VERSION, Collections.emptyMap(), Collections.emptyList(), Collections.EMPTY_MAP);
    metaClient.getActiveTimeline().saveToCleanRequested(requestedInstant2, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan1));
    HoodieInstant inflightInstant2 = metaClient.getActiveTimeline().transitionCleanRequestedToInflight(requestedInstant2, Option.empty());
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(cleanTime1, 1L, 1,
        "", "", Collections.emptyMap(), 0, Collections.emptyMap(), Collections.emptyMap());
    metaClient.getActiveTimeline().transitionCleanInflightToComplete(inflightInstant2,
        TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata));
    metaClient.reloadActiveTimeline();

    List<String> fileIds2 = new ArrayList<>();
    fileIds2.add(UUID.randomUUID().toString());
    fileIds2.add(UUID.randomUUID().toString());
    String clusterTime2 = "3";
    HoodieInstant requestedInstant3 = createRequestedReplaceInstant(partitionPath1, clusterTime2, fileIds2);
    HoodieInstant inflightInstant3 = metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(requestedInstant3, Option.empty());
    metaClient.getActiveTimeline().transitionReplaceInflightToComplete(inflightInstant3, Option.empty());
    metaClient.reloadActiveTimeline();

    Option<HoodieInstant> actual = ClusteringUtils.getOldestInstantToRetainForClustering(metaClient.getActiveTimeline(), metaClient);
    assertEquals(clusterTime2, actual.get().getTimestamp(),
        "retain the first replace commit after the last complete clean ");
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
    fs.setBaseFile(new HoodieBaseFile(FSUtils.makeBaseFileName(baseInstant, "1-0-1", fileId, BASE_FILE_EXTENSION)));
    return fs;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
