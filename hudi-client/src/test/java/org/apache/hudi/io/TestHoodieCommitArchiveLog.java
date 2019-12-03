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

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import com.google.common.collect.Sets;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  public void testArchiveEmptyDataset() throws IOException {
    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .withParallelism(2, 2).forTable("test-trip-table").build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
  }

  @Test
  public void testArchiveDatasetWithArchival() throws IOException {
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
    HoodieTestUtils.createInflightCleanFiles(basePath, dfs.getConf(), "101");
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "101", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "102", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "103", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "104", dfs.getConf());
    HoodieTestUtils.createCleanFiles(metaClient, basePath, "105", dfs.getConf());
    HoodieTestUtils.createInflightCleanFiles(basePath, dfs.getConf(), "106", "107");

    // reload the timeline and get all the commmits before archive
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());

    assertEquals("Loaded 6 commits and the count should match", 12, timeline.countInstants());

    // verify in-flight instants before archive
    verifyInflightInstants(metaClient, 3);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);

    assertTrue(archiveLog.archiveIfRequired(jsc));

    // reload the timeline and remove the remaining commits
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    originalCommits.removeAll(timeline.getInstants().collect(Collectors.toList()));

    // Check compaction instants
    List<HoodieInstant> instants = HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);
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
    Reader reader =
        HoodieLogFormat.newReader(dfs, new HoodieLogFile(new Path(basePath + "/.hoodie/.commits_.archive.1_1-0-1")),
            HoodieArchivedMetaEntry.getClassSchema());

    int archivedRecordsCount = 0;
    List<IndexedRecord> readRecords = new ArrayList<>();
    // read the avro blocks and validate the number of records written in each avro block
    while (reader.hasNext()) {
      HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
      List<IndexedRecord> records = blk.getRecords();
      readRecords.addAll(records);
      assertEquals("Archived and read records for each block are same", 8, records.size());
      archivedRecordsCount += records.size();
    }
    assertEquals("Total archived records and total read records are the same count", 8, archivedRecordsCount);

    // make sure the archived commits are the same as the (originalcommits - commitsleft)
    List<String> readCommits = readRecords.stream().map(r -> (GenericRecord) r).map(r -> {
      return r.get("commitTime").toString();
    }).collect(Collectors.toList());
    Collections.sort(readCommits);

    assertEquals("Read commits map should match the originalCommits - commitsLoadedFromArchival",
        originalCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()), readCommits);

    // verify in-flight instants after archive
    verifyInflightInstants(metaClient, 3);
    reader.close();
  }

  @Test
  public void testArchiveDatasetWithNoArchival() throws IOException {
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

    List<HoodieInstant> instants = HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);
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
        "Since we have a savepoint at 101, we should never archive any commit after 101 (we only " + "archive 100)", 5,
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
        + "after 101 (we only " + "archive 100)", 7, timeline.countInstants());
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

  private void verifyInflightInstants(HoodieTableMetaClient metaClient, int expectedTotalInstants) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().reload()
        .getTimelineOfActions(Sets.newHashSet(HoodieTimeline.CLEAN_ACTION)).filterInflights();
    assertEquals("Loaded inflight clean actions and the count should match", expectedTotalInstants,
        timeline.countInstants());
  }
}
