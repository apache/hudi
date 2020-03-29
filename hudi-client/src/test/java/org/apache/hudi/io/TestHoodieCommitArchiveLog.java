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

import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.table.HoodieCommitArchiveLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHoodieCommitArchiveLog extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @Before
  public void init() throws Exception {
    initDFS();
    initPath();
    initSparkContexts("TestHoodieCommitArchiveLog");
    hadoopConf = dfs.getConf();
    jsc.hadoopConfiguration().addResource(dfs.getConf());
    dfs.mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath);
  }

  @After
  public void clean() throws IOException {
    cleanupDFS();
    cleanupSparkContexts();
  }

  @Test
  public void testArchiveEmptyTable() throws IOException {
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table").build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
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

    assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());

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

    assertEquals("Loaded 6 commits and the count should match", 12, timeline.countInstants());

    // verify in-flight instants before archive
    verifyInflightInstants(metaClient, 2);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);

    assertTrue(archiveLog.archiveIfRequired(jsc));

    // reload the timeline and remove the remaining commits
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    originalCommits.removeAll(timeline.getInstants().collect(Collectors.toList()));

    // Check compaction instants
    List<HoodieInstant> instants = metaClient.scanHoodieInstantsFromFileSystem(
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    assertEquals("Should delete all compaction instants < 104", 4, instants.size());
    assertFalse("Requested Compaction must be absent for 100",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100")));
    assertFalse("Inflight Compaction must be absent for 100",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100")));
    assertFalse("Requested Compaction must be absent for 101",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")));
    assertFalse("Inflight Compaction must be absent for 101",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101")));
    assertFalse("Requested Compaction must be absent for 102",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102")));
    assertFalse("Inflight Compaction must be absent for 102",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102")));
    assertFalse("Requested Compaction must be absent for 103",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103")));
    assertFalse("Inflight Compaction must be absent for 103",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103")));
    assertTrue("Requested Compaction must be present for 104",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104")));
    assertTrue("Inflight Compaction must be present for 104",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "104")));
    assertTrue("Requested Compaction must be present for 105",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "105")));
    assertTrue("Inflight Compaction must be present for 105",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "105")));

    // read the file
    HoodieArchivedTimeline archivedTimeline = new HoodieArchivedTimeline(metaClient);
    assertEquals("Total archived records and total read records are the same count",
            24, archivedTimeline.countInstants());

    //make sure the archived commits are the same as the (originalcommits - commitsleft)
    Set<String> readCommits =
            archivedTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    assertEquals("Read commits map should match the originalCommits - commitsLoadedFromArchival",
            originalCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()), readCommits);

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
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
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
    assertEquals("Loaded 4 commits and the count should match", 4, timeline.countInstants());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals("Should not archive commits when maxCommitsToKeep is 5", 4, timeline.countInstants());

    List<HoodieInstant> instants = metaClient.scanHoodieInstantsFromFileSystem(
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    assertEquals("Should not delete any aux compaction files when maxCommitsToKeep is 5", 8, instants.size());
    assertTrue("Requested Compaction must be present for 100",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "100")));
    assertTrue("Inflight Compaction must be present for 100",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "100")));
    assertTrue("Requested Compaction must be present for 101",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")));
    assertTrue("Inflight Compaction must be present for 101",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "101")));
    assertTrue("Requested Compaction must be present for 102",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "102")));
    assertTrue("Inflight Compaction must be present for 102",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "102")));
    assertTrue("Requested Compaction must be present for 103",
        instants.contains(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "103")));
    assertTrue("Inflight Compaction must be present for 103",
        instants.contains(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "103")));
  }

  @Test
  public void testArchiveCommitSafety() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("100"));
    assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("101"));
    assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("102"));
    assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("103"));
  }

  @Test
  public void testArchiveCommitSavepointNoHole() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createSavepointFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals(
        "Since we have a savepoint at 101, we should never archive any commit after 101 (we only archive 100)", 5,
        timeline.countInstants());
    assertTrue("Archived commits should always be safe",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101")));
    assertTrue("Archived commits should always be safe",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")));
    assertTrue("Archived commits should always be safe",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")));
  }

  @Test
  public void testArchiveCommitCompactionNoHole() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable("test-trip-table")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build())
        .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
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

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline();
    assertEquals("Loaded 6 commits and the count should match", 8, timeline.countInstants());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsAndCompactionTimeline();
    assertFalse("Instants before oldest pending compaction can be removed",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "100")));
    assertEquals("Since we have a pending compaction at 101, we should never archive any commit "
        + "after 101 (we only archive 100)", 7, timeline.countInstants());
    assertTrue("Requested Compaction must still be present",
        timeline.containsInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "101")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "104")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "105")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "106")));
    assertTrue("Instants greater than oldest pending compaction must be present",
        timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "107")));
  }

  @Test
  public void checkArchiveCommitTimeline() throws IOException {
    HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
                    .withParallelism(2, 2).forTable("test-trip-table")
                    .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);

    HoodieTestDataGenerator.createCommitFile(basePath, "1", dfs.getConf());
    HoodieInstant instant1 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieTestDataGenerator.createCommitFile(basePath, "2", dfs.getConf());
    HoodieInstant instant2 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieTestDataGenerator.createCommitFile(basePath, "3", dfs.getConf());
    HoodieInstant instant3 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");

    //add 2 more instants to pass filter criteria set in compaction config above
    HoodieTestDataGenerator.createCommitFile(basePath, "4", dfs.getConf());
    HoodieInstant instant4 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "4");
    HoodieTestDataGenerator.createCommitFile(basePath, "5", dfs.getConf());
    HoodieInstant instant5 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "5");

    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);

    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    List<HoodieInstant> archivedInstants = Arrays.asList(instant1, instant2, instant3);
    assertEquals(new HashSet<>(archivedInstants), archivedTimeline.getInstants().collect(Collectors.toSet()));
  }

  private void verifyInflightInstants(HoodieTableMetaClient metaClient, int expectedTotalInstants) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload()
        .getTimelineOfActions(Collections.singleton(HoodieTimeline.CLEAN_ACTION)).filterInflights();
    assertEquals("Loaded inflight clean actions and the count should match", expectedTotalInstants,
        timeline.countInstants());
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
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);

    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = archiveLog.convertCommitMetadata(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }
}
