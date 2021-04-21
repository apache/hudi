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

package org.apache.hudi.io;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTimelineArchiveLog extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieWrapperFileSystem wrapperFs;

  @BeforeEach
  public void init() throws Exception {
    initPath();
    initSparkContexts();
    initMetaClient();
    hadoopConf = context.getHadoopConf().get();
    metaClient.getFs().mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath);
    wrapperFs = metaClient.getFs();
    hadoopConf.addResource(wrapperFs.getConf());
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  @Test
  public void testArchiveEmptyTable() throws IOException {
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table").build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);
  }

  @Test
  public void testArchiveTableWithArchival() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 4).build())
        .forTable("test-trip-table").build();
    HoodieTestUtils.init(hadoopConf, basePath);
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "104"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "105"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "105"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", wrapperFs.getConf());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");

    createCleanMetadata("100", false);
    createCleanMetadata("101", false);
    createCleanMetadata("102", false);
    createCleanMetadata("103", false);
    createCleanMetadata("104", false);
    createCleanMetadata("105", false);
    createCleanMetadata("106", true);
    createCleanMetadata("107", true);

    // reload the timeline and get all the commmits before archive
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());

    assertEquals(12, timeline.countInstants(), "Loaded 6 commits and the count should match");

    // verify in-flight instants before archive
    verifyInflightInstants(metaClient, 2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    assertTrue(archiveLog.archiveIfRequired(context));

    // reload the timeline and remove the remaining commits
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    originalCommits.removeAll(timeline.getInstants().collect(Collectors.toList()));

    // Check compaction instants
    List<HoodieInstant> instants = metaClient.scanHoodieInstantsFromFileSystem(
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    assertEquals(4, instants.size(), "Should delete all compaction instants < 104");
    assertFalse(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100")),
        "Requested Compaction must be absent for 100");
    assertFalse(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100")),
        "Inflight Compaction must be absent for 100");
    assertFalse(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")),
        "Requested Compaction must be absent for 101");
    assertFalse(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101")),
        "Inflight Compaction must be absent for 101");
    assertFalse(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102")),
        "Requested Compaction must be absent for 102");
    assertFalse(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102")),
        "Inflight Compaction must be absent for 102");
    assertFalse(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103")),
        "Requested Compaction must be absent for 103");
    assertFalse(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103")),
        "Inflight Compaction must be absent for 103");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104")),
        "Requested Compaction must be present for 104");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "104")),
        "Inflight Compaction must be present for 104");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "105")),
        "Requested Compaction must be present for 105");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "105")),
        "Inflight Compaction must be present for 105");

    // read the file
    HoodieArchivedTimeline archivedTimeline = new HoodieArchivedTimeline(metaClient);
    assertEquals(24, archivedTimeline.countInstants(),
        "Total archived records and total read records are the same count");

    //make sure the archived commits are the same as the (originalcommits - commitsleft)
    Set<String> readCommits =
        archivedTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    assertEquals(originalCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()), readCommits,
        "Read commits map should match the originalCommits - commitsLoadedFromArchival");

    // verify in-flight instants after archive
    verifyInflightInstants(metaClient, 2);
  }

  @Test
  public void testArchiveTableWithReplacedFiles() throws Exception {
    HoodieTestUtils.init(hadoopConf, basePath);
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
        .build();

    int numCommits = 4;
    int commitInstant = 100;
    for (int i = 0; i < numCommits; i++) {
      createReplaceMetadata(String.valueOf(commitInstant));
      commitInstant += 100;
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    assertEquals(4, timeline.countInstants(), "Loaded 4 commits and the count should match");
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);

    FileStatus[] allFiles = metaClient.getFs().listStatus(new Path(basePath + "/" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH));
    Set<String> allFileIds = Arrays.stream(allFiles).map(fs -> FSUtils.getFileIdFromFilePath(fs.getPath())).collect(Collectors.toSet());

    // verify 100-1,200-1 are deleted by archival
    assertFalse(allFileIds.contains("file-100-1"));
    assertFalse(allFileIds.contains("file-200-1"));
    assertTrue(allFileIds.contains("file-100-2"));
    assertTrue(allFileIds.contains("file-200-2"));
    assertTrue(allFileIds.contains("file-300-1"));
    assertTrue(allFileIds.contains("file-400-1"));
  }

  @Test
  public void testArchiveTableWithNoArchival() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103"), wrapperFs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(4, timeline.countInstants(), "Loaded 4 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(4, timeline.countInstants(), "Should not archive commits when maxCommitsToKeep is 5");

    List<HoodieInstant> instants = metaClient.scanHoodieInstantsFromFileSystem(
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    assertEquals(8, instants.size(), "Should not delete any aux compaction files when maxCommitsToKeep is 5");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100")),
        "Requested Compaction must be present for 100");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100")),
        "Inflight Compaction must be present for 100");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")),
        "Requested Compaction must be present for 101");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101")),
        "Inflight Compaction must be present for 101");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102")),
        "Requested Compaction must be present for 102");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102")),
        "Inflight Compaction must be present for 102");
    assertTrue(instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103")),
        "Requested Compaction must be present for 103");
    assertTrue(instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103")),
        "Inflight Compaction must be present for 103");
  }

  @Test
  public void testArchiveCommitSafety() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", wrapperFs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertTrue(timeline.containsOrBeforeTimelineStarts("100"), "Archived commits should always be safe");
    assertTrue(timeline.containsOrBeforeTimelineStarts("101"), "Archived commits should always be safe");
    assertTrue(timeline.containsOrBeforeTimelineStarts("102"), "Archived commits should always be safe");
    assertTrue(timeline.containsOrBeforeTimelineStarts("103"), "Archived commits should always be safe");
  }

  @Test
  public void testArchiveCommitSavepointNoHole() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();

    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createSavepointFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", wrapperFs.getConf());
    HoodieTable table = HoodieSparkTable.create(cfg, context);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    assertTrue(archiveLog.archiveIfRequired(context));
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(5, timeline.countInstants(),
        "Since we have a savepoint at 101, we should never archive any commit after 101 (we only archive 100)");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101")),
        "Archived commits should always be safe");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")),
        "Archived commits should always be safe");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")),
        "Archived commits should always be safe");
  }

  @Test
  public void testArchiveRollbacks() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
            .build();

    createCommitAndRollbackFile("100", "101", false);
    createCommitAndRollbackFile("102", "103", false);
    createCommitAndRollbackFile("104", "105", false);
    createCommitAndRollbackFile("106", "107", false);

    HoodieTable table = HoodieSparkTable.create(cfg, context);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    assertTrue(archiveLog.archiveIfRequired(context));
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(2, timeline.countInstants(),
            "first two commits must have been archived");
    assertFalse(metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.ROLLBACK_ACTION, "101")),
            "first rollback must have been archived");
    assertFalse(metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.ROLLBACK_ACTION, "103")),
            "second rollback must have been archived");
    assertTrue(metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.ROLLBACK_ACTION, "105")),
            "first rollback must have been archived");
    assertTrue(metaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.ROLLBACK_ACTION, "107")),
            "second rollback must have been archived");
  }

  @Test
  public void testArchiveCommitCompactionNoHole() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    HoodieTestDataGenerator.createCommitFile(basePath, "100", wrapperFs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "101", wrapperFs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", wrapperFs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "104", wrapperFs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104"), wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "106", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "107", wrapperFs.getConf());
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    HoodieTimeline timeline = metaClient.getActiveTimeline().getWriteTimeline();
    assertEquals(8, timeline.countInstants(), "Loaded 6 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getWriteTimeline();
    assertFalse(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "100")),
        "Instants before oldest pending compaction can be removed");
    assertEquals(7, timeline.countInstants(),
        "Since we have a pending compaction at 101, we should never archive any commit "
            + "after 101 (we only archive 100)");
    assertTrue(timeline.containsInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")),
        "Requested Compaction must still be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")),
        "Instants greater than oldest pending compaction must be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")),
        "Instants greater than oldest pending compaction must be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104")),
        "Instants greater than oldest pending compaction must be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "105")),
        "Instants greater than oldest pending compaction must be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "106")),
        "Instants greater than oldest pending compaction must be present");
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "107")),
        "Instants greater than oldest pending compaction must be present");
  }

  @Test
  public void testArchiveCommitTimeline() throws IOException {
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTestDataGenerator.createCommitFile(basePath, "1", wrapperFs.getConf());
    HoodieInstant instant1 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieTestDataGenerator.createCommitFile(basePath, "2", wrapperFs.getConf());
    Path markerPath = new Path(metaClient.getMarkerFolderPath("2"));
    wrapperFs.mkdirs(markerPath);
    HoodieInstant instant2 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieTestDataGenerator.createCommitFile(basePath, "3", wrapperFs.getConf());
    HoodieInstant instant3 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");

    //add 2 more instants to pass filter criteria set in compaction config above
    HoodieTestDataGenerator.createCommitFile(basePath, "4", wrapperFs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "5", wrapperFs.getConf());

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    boolean result = archiveLog.archiveIfRequired(context);
    assertTrue(result);
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<HoodieInstant> archivedInstants = Arrays.asList(instant1, instant2, instant3);
    assertEquals(new HashSet<>(archivedInstants),
        archivedTimeline.filterCompletedInstants().getInstants().collect(Collectors.toSet()));
    assertFalse(wrapperFs.exists(markerPath));
  }

  private void verifyInflightInstants(HoodieTableMetaClient metaClient, int expectedTotalInstants) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload()
        .getTimelineOfActions(Collections.singleton(HoodieTimeline.CLEAN_ACTION)).filterInflights();
    assertEquals(expectedTotalInstants, timeline.countInstants(),
        "Loaded inflight clean actions and the count should match");
  }

  @Test
  public void testConvertCommitMetadata() {
    HoodieCommitMetadata hoodieCommitMetadata = new HoodieCommitMetadata();
    hoodieCommitMetadata.setOperationType(WriteOperationType.INSERT);

    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-commitMetadata-converter")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = MetadataConversionUtils
        .convertCommitMetadata(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }

  @Test
  public void testArchiveCompletedClean() throws IOException {
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    createCleanMetadata("10", false);
    createCleanMetadata("11", false);
    HoodieInstant notArchivedInstant1 = createCleanMetadata("12", false);
    HoodieInstant notArchivedInstant2 = createCleanMetadata("13", false);

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);

    List<HoodieInstant> notArchivedInstants = metaClient.getActiveTimeline().reload().getInstants().collect(Collectors.toList());
    //There will be 3 * 2 files but due to TimelineLayoutV1 this will show as 2.
    assertEquals(2, notArchivedInstants.size(), "Not archived instants should be 2");
    assertEquals(notArchivedInstants, Arrays.asList(notArchivedInstant1, notArchivedInstant2), "");
  }

  @Test
  public void testArchiveCompletedRollback() throws IOException {
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    createCommitAndRollbackFile("6", "10", false);
    createCommitAndRollbackFile("8", "11", false);
    createCommitAndRollbackFile("7", "12", false);
    HoodieInstant notArchivedInstant1 = new HoodieInstant(State.COMPLETED, "rollback", "12");

    createCommitAndRollbackFile("5", "13", false);
    HoodieInstant notArchivedInstant2 = new HoodieInstant(State.COMPLETED, "rollback", "13");

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);

    List<HoodieInstant> notArchivedInstants = metaClient.getActiveTimeline().reload().getRollbackTimeline().getInstants().collect(Collectors.toList());
    //There will be 2 * 2 files but due to TimelineLayoutV1 this will show as 2.
    assertEquals(2, notArchivedInstants.size(), "Not archived instants should be 2");
    assertEquals(notArchivedInstants, Arrays.asList(notArchivedInstant1, notArchivedInstant2), "");
  }

  @Test
  public void testArchiveCompletedShouldRetainMinInstantsIfInstantsGreaterThanMaxtoKeep() throws IOException {
    int minInstants = 2;
    int maxInstants = 10;
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minInstants, maxInstants).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    for (int i = 0; i < maxInstants + 2; i++) {
      createCleanMetadata(i + "", false);
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);
    assertEquals(minInstants, metaClient.getActiveTimeline().reload().getInstants().count());
  }

  @Test
  public void testArchiveCompletedShouldNotArchiveIfInstantsLessThanMaxtoKeep() throws IOException {
    int minInstants = 2;
    int maxInstants = 10;
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minInstants, maxInstants).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    for (int i = 0; i < maxInstants; i++) {
      createCleanMetadata(i + "", false);
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);
    assertEquals(maxInstants, metaClient.getActiveTimeline().reload().getInstants().count());
  }

  @Test
  public void testArchiveCompletedRollbackAndClean() throws IOException {
    int minInstantsToKeep = 2;
    int maxInstantsToKeep = 10;
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(minInstantsToKeep, maxInstantsToKeep).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    int startInstant = 1;
    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant++) {
      createCleanMetadata(startInstant + "", false);
    }

    for (int i = 0; i < maxInstantsToKeep + 1; i++, startInstant += 2) {
      createCommitAndRollbackFile(startInstant + 1 + "", startInstant + "", false);
    }

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);

    Stream<HoodieInstant> currentInstants = metaClient.getActiveTimeline().reload().getInstants();
    Map<Object, List<HoodieInstant>> actionInstantMap = currentInstants.collect(Collectors.groupingBy(HoodieInstant::getAction));

    assertTrue(actionInstantMap.containsKey("clean"), "Clean Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("clean").size(), "Should have min instant");

    assertTrue(actionInstantMap.containsKey("rollback"), "Rollback Action key must be preset");
    assertEquals(minInstantsToKeep, actionInstantMap.get("rollback").size(), "Should have min instant");
  }

  @Test
  public void testArchiveInflightClean() throws IOException {
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    createCleanMetadata("10", false);
    createCleanMetadata("11", false);
    HoodieInstant notArchivedInstant1 = createCleanMetadata("12", false);
    HoodieInstant notArchivedInstant2 = createCleanMetadata("13", false);
    HoodieInstant notArchivedInstant3 = createCleanMetadata("14", true);

    HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);

    archiveLog.archiveIfRequired(context);

    List<HoodieInstant> notArchivedInstants = metaClient.getActiveTimeline().reload().getInstants().collect(Collectors.toList());
    assertEquals(3, notArchivedInstants.size(), "Not archived instants should be 3");
    assertEquals(notArchivedInstants, Arrays.asList(notArchivedInstant1, notArchivedInstant2, notArchivedInstant3), "");
  }

  private void createReplaceMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    // create replace instant to mark fileId1 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.INSERT_OVERWRITE.toString())
        .setVersion(1)
        .setExtraMetadata(Collections.emptyMap())
        .build();
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.addReplaceFileId(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1);
    replaceMetadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, requestedReplaceMetadata, replaceMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private HoodieInstant createCleanMetadata(String instantTime, boolean inflightOnly) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "", new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>());
    if (inflightOnly) {
      HoodieTestTable.of(metaClient).addInflightClean(instantTime, cleanerPlan);
    } else {
      HoodieCleanStat cleanStats = new HoodieCleanStat(
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          HoodieTestUtils.DEFAULT_PARTITION_PATHS[new Random().nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          instantTime);
      HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats));
      HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
    }
    return new HoodieInstant(inflightOnly, "clean", instantTime);
  }

  private void createCommitAndRollbackFile(String commitToRollback, String rollbackTIme, boolean isRollbackInflight) throws IOException {
    HoodieTestDataGenerator.createCommitFile(basePath, commitToRollback, wrapperFs.getConf());
    createRollbackMetadata(rollbackTIme, commitToRollback, isRollbackInflight);
  }

  private HoodieInstant createRollbackMetadata(String rollbackTime, String commitToRollback, boolean inflight) throws IOException {
    if (inflight) {
      HoodieTestTable.of(metaClient).addInflightRollback(rollbackTime);
    } else {
      HoodieRollbackMetadata hoodieRollbackMetadata = HoodieRollbackMetadata.newBuilder()
              .setVersion(1)
              .setStartRollbackTime(rollbackTime)
              .setTotalFilesDeleted(1)
              .setTimeTakenInMillis(1000)
              .setCommitsRollback(Collections.singletonList(commitToRollback))
              .setPartitionMetadata(Collections.emptyMap())
              .setInstantsRollback(Collections.emptyList())
              .build();
      HoodieTestTable.of(metaClient).addRollback(rollbackTime, hoodieRollbackMetadata);
    }
    return new HoodieInstant(inflight, "rollback", rollbackTime);
  }
}
