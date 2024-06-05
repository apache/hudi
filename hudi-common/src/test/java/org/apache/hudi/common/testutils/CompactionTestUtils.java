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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.createBaseFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createLogFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.logFileName;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The utility class to support testing compaction.
 */
public class CompactionTestUtils {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  public static Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> setupAndValidateCompactionOperations(
      HoodieTableMetaClient metaClient, boolean inflight, int numEntriesInPlan1, int numEntriesInPlan2,
      int numEntriesInPlan3, int numEntriesInPlan4) throws IOException {
    HoodieCompactionPlan plan1 = createCompactionPlan(metaClient, "000", "001", numEntriesInPlan1, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan(metaClient, "002", "003", numEntriesInPlan2, false, true);
    HoodieCompactionPlan plan3 = createCompactionPlan(metaClient, "004", "005", numEntriesInPlan3, true, false);
    HoodieCompactionPlan plan4 = createCompactionPlan(metaClient, "006", "007", numEntriesInPlan4, false, false);

    if (inflight) {
      scheduleInflightCompaction(metaClient, "001", plan1);
      scheduleInflightCompaction(metaClient, "003", plan2);
      scheduleInflightCompaction(metaClient, "005", plan3);
      scheduleInflightCompaction(metaClient, "007", plan4);
    } else {
      scheduleCompaction(metaClient, "001", plan1);
      scheduleCompaction(metaClient, "003", plan2);
      scheduleCompaction(metaClient, "005", plan3);
      scheduleCompaction(metaClient, "007", plan4);
    }

    createDeltaCommit(metaClient, "000");
    createDeltaCommit(metaClient, "002");
    createDeltaCommit(metaClient, "004");
    createDeltaCommit(metaClient, "006");

    Map<String, String> baseInstantsToCompaction = new HashMap<String, String>() {
      {
        put("000", "001");
        put("002", "003");
        put("004", "005");
        put("006", "007");
      }
    };
    List<Integer> expectedNumEntries =
        Arrays.asList(numEntriesInPlan1, numEntriesInPlan2, numEntriesInPlan3, numEntriesInPlan4);
    List<HoodieCompactionPlan> plans = CollectionUtils.createImmutableList(plan1, plan2, plan3, plan4);
    IntStream.range(0, 4).boxed().forEach(idx -> {
      if (expectedNumEntries.get(idx) > 0) {
        assertEquals(expectedNumEntries.get(idx).longValue(),
            plans.get(idx).getOperations().size(),
            "check if plan " + idx + " has exp entries");
      } else {
        assertNull(plans.get(idx).getOperations(), "Plan " + idx + " has null ops");
      }
    });

    metaClient = createMetaClient(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> pendingCompactionMap =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);

    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> expPendingCompactionMap =
        generateExpectedCompactionOperations(Arrays.asList(plan1, plan2, plan3, plan4), baseInstantsToCompaction);

    // Ensure Compaction operations are fine.
    assertEquals(expPendingCompactionMap, pendingCompactionMap);
    return expPendingCompactionMap;
  }

  public static Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> generateExpectedCompactionOperations(
      List<HoodieCompactionPlan> plans, Map<String, String> baseInstantsToCompaction) {
    return plans.stream().flatMap(plan -> {
      if (plan.getOperations() != null) {
        return plan.getOperations().stream()
            .map(op -> Pair.of(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()),
                Pair.of(baseInstantsToCompaction.get(op.getBaseInstantTime()), op)));
      }
      return Stream.empty();
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public static void scheduleCompaction(HoodieTableMetaClient metaClient, String instantTime,
      HoodieCompactionPlan compactionPlan) throws IOException {
    metaClient.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, instantTime),
        TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
  }

  public static void createDeltaCommit(HoodieTableMetaClient metaClient, String instantTime) {
    HoodieInstant requested = new HoodieInstant(State.REQUESTED, DELTA_COMMIT_ACTION, instantTime);
    metaClient.getActiveTimeline().createNewInstant(requested);
    metaClient.getActiveTimeline().transitionRequestedToInflight(requested, Option.empty());
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, DELTA_COMMIT_ACTION, instantTime), Option.empty());
  }

  public static void scheduleInflightCompaction(HoodieTableMetaClient metaClient, String instantTime,
      HoodieCompactionPlan compactionPlan) throws IOException {
    scheduleCompaction(metaClient, instantTime, compactionPlan);
    metaClient.getActiveTimeline()
        .transitionCompactionRequestedToInflight(new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, instantTime));
  }

  public static HoodieCompactionPlan createCompactionPlan(HoodieTableMetaClient metaClient, String instantTime,
      String compactionInstantTime, int numFileIds, boolean createDataFile, boolean deltaCommitsAfterCompactionRequests) {
    List<HoodieCompactionOperation> ops = IntStream.range(0, numFileIds).boxed().map(idx -> {
      try {
        final String basePath = metaClient.getBasePath().toString();
        final String partition = DEFAULT_PARTITION_PATHS[0];
        final String fileId = UUID.randomUUID().toString();
        if (createDataFile) {
          createBaseFile(basePath, partition, instantTime, fileId);
        }
        createLogFile(basePath, partition, instantTime, fileId, 1);
        createLogFile(basePath, partition, instantTime, fileId, 2);
        FileSlice slice = new FileSlice(partition, instantTime, fileId);
        if (createDataFile) {
          slice.setBaseFile(new DummyHoodieBaseFile(Paths.get(basePath, partition,
              baseFileName(instantTime, fileId)).toString()));
        }
        String logFilePath1 =
            Paths.get(basePath, partition, logFileName(instantTime, fileId, 1)).toString();
        String logFilePath2 =
            Paths.get(basePath, partition, logFileName(instantTime, fileId, 2)).toString();
        slice.addLogFile(new HoodieLogFile(new StoragePath(logFilePath1)));
        slice.addLogFile(new HoodieLogFile(new StoragePath(logFilePath2)));
        HoodieCompactionOperation op =
            CompactionUtils.buildFromFileSlice(partition, slice, Option.empty());
        if (deltaCommitsAfterCompactionRequests) {
          createLogFile(basePath, partition, compactionInstantTime, fileId, 1);
          createLogFile(basePath, partition, compactionInstantTime, fileId, 2);
        }
        return op;
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    }).collect(Collectors.toList());
    return new HoodieCompactionPlan(ops.isEmpty() ? null : ops, new HashMap<>(),
        CompactionUtils.LATEST_COMPACTION_METADATA_VERSION, null, null);
  }

  /**
   * The hoodie data file for testing.
   */
  public static class DummyHoodieBaseFile extends HoodieBaseFile {

    private final String path;

    public DummyHoodieBaseFile(String path) {
      super(path);
      this.path = path;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public String getFileId() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String getCommitTime() {
      return "100";
    }

    @Override
    public long getFileSize() {
      return 0;
    }
  }
}
