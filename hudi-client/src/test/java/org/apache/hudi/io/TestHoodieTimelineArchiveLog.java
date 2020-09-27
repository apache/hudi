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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTimelineArchiveLog extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void init() throws Exception {
    initDFS();
    initPath();
    initSparkContexts();
    hadoopConf = dfs.getConf();
    hadoopConf.addResource(dfs.getConf());
    dfs.mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath);
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
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);
    boolean result = archiveLog.archiveIfRequired(jsc);
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
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "104"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "105"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "105"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");

    HoodieTestUtils.createCleanFiles(metaClient, basePath, "100", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "101", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "102", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "103", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "104", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "105", dfs.getConf());
    HoodieTestUtils.createPendingCleanFiles(metaClient, "106", "107");

    // reload the timeline and get all the commmits before archive
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());

    assertEquals(12, timeline.countInstants(), "Loaded 6 commits and the count should match");

    // verify in-flight instants before archive
    verifyInflightInstants(metaClient, 2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);
    assertTrue(archiveLog.archiveIfRequired(jsc));

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
  public void testArchiveTableWithNoArchival() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(4, timeline.countInstants(), "Loaded 4 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(jsc);
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
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(jsc);
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
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createSavepointFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(6, timeline.countInstants(), "Loaded 6 commits and the count should match");
    assertTrue(archiveLog.archiveIfRequired(jsc));
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
  public void testArchiveCommitCompactionNoHole() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "106", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "107", dfs.getConf());
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline();
    assertEquals(8, timeline.countInstants(), "Loaded 6 commits and the count should match");
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsAndCompactionTimeline();
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

    HoodieTestDataGenerator.createCommitFile(basePath, "1", dfs.getConf());
    HoodieInstant instant1 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieTestDataGenerator.createCommitFile(basePath, "2", dfs.getConf());
    Path markerPath = new Path(metaClient.getMarkerFolderPath("2"));
    dfs.mkdirs(markerPath);
    HoodieInstant instant2 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieTestDataGenerator.createCommitFile(basePath, "3", dfs.getConf());
    HoodieInstant instant3 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");

    //add 2 more instants to pass filter criteria set in compaction config above
    HoodieTestDataGenerator.createCommitFile(basePath, "4", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "5", dfs.getConf());


    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, dfs.getConf());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<HoodieInstant> archivedInstants = Arrays.asList(instant1, instant2, instant3);
    assertEquals(new HashSet<>(archivedInstants), archivedTimeline.getInstants().collect(Collectors.toSet()));
    assertFalse(dfs.exists(markerPath));
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
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, hadoopConf);

    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = archiveLog.convertCommitMetadata(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }
}
