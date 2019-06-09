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

package com.uber.hoodie.io;

import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.uber.hoodie.avro.model.HoodieArchivedMetaEntry;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieCommitArchiveLog {

  //NOTE : Be careful in using DFS (FileSystem.class) vs LocalFs(RawLocalFileSystem.class)
  //The implementation and gurantees of many API's differ, for example check rename(src,dst)
  // We need to use DFS here instead of LocalFs since the FsDataInputStream.getWrappedStream() returns a
  // FsDataInputStream instead of a InputStream and thus throws java.lang.ClassCastException:
  // org.apache.hadoop.fs.FSDataInputStream cannot be cast to org.apache.hadoop.fs.FSInputStream
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static HdfsTestService hdfsTestService;
  private String basePath;
  private Configuration hadoopConf;
  private JavaSparkContext jsc = null;

  @AfterClass
  public static void cleanUp() throws Exception {
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();

    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
    }
  }

  @BeforeClass
  public static void setUpDFS() throws IOException {
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
    if (hdfsTestService == null) {
      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      // Create a temp folder as the base path
      dfs = dfsCluster.getFileSystem();
    }
  }

  @Before
  public void init() throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieCommitArchiveLog"));
    basePath = folder.getRoot().getAbsolutePath();
    hadoopConf = dfs.getConf();
    jsc.hadoopConfiguration().addResource(dfs.getConf());
    dfs.mkdirs(new Path(basePath));
    HoodieTestUtils.init(hadoopConf, basePath);
  }

  @After
  public void clean() {
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testArchiveEmptyDataset() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table").build();
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg,
        new HoodieTableMetaClient(dfs.getConf(), cfg.getBasePath(), true));
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
  }

  @Test
  public void testArchiveDatasetWithArchival() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 4).build())
        .forTable("test-trip-table").build();
    HoodieTestUtils.init(hadoopConf, basePath);
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "100"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "100"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "101"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "102"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "102"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "103"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "103"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "104"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "104"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "104", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "105"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "105"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "105", dfs.getConf());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(dfs.getConf(), basePath);
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());

    HoodieTestUtils.createCleanFiles(basePath, "100", dfs.getConf());
    HoodieTestUtils.createInflightCleanFiles(basePath, dfs.getConf(), "101");
    HoodieTestUtils.createCleanFiles(basePath, "101", dfs.getConf());
    HoodieTestUtils.createCleanFiles(basePath, "102", dfs.getConf());
    HoodieTestUtils.createCleanFiles(basePath, "103", dfs.getConf());
    HoodieTestUtils.createCleanFiles(basePath, "104", dfs.getConf());
    HoodieTestUtils.createCleanFiles(basePath, "105", dfs.getConf());
    HoodieTestUtils.createInflightCleanFiles(basePath, dfs.getConf(), "106", "107");

    //reload the timeline and get all the commmits before archive
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());

    assertEquals("Loaded 6 commits and the count should match", 12, timeline.countInstants());

    // verify in-flight instants before archive
    verifyInflightInstants(metaClient, 3);

    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg,
        new HoodieTableMetaClient(dfs.getConf(), basePath, true));

    assertTrue(archiveLog.archiveIfRequired(jsc));

    //reload the timeline and remove the remaining commits
    timeline = metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
    originalCommits.removeAll(timeline.getInstants().collect(Collectors.toList()));

    // Check compaction instants
    List<HoodieInstant> instants =
        HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
            new Path(metaClient.getMetaAuxiliaryPath()),
            HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);
    assertEquals("Should delete all compaction instants < 104", 4, instants.size());
    assertFalse("Requested Compaction must be absent for 100", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "100")));
    assertFalse("Inflight Compaction must be absent for 100", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "100")));
    assertFalse("Requested Compaction must be absent for 101", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "101")));
    assertFalse("Inflight Compaction must be absent for 101", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "101")));
    assertFalse("Requested Compaction must be absent for 102", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "102")));
    assertFalse("Inflight Compaction must be absent for 102", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "102")));
    assertFalse("Requested Compaction must be absent for 103", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "103")));
    assertFalse("Inflight Compaction must be absent for 103", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "103")));
    assertTrue("Requested Compaction must be present for 104", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "104")));
    assertTrue("Inflight Compaction must be present for 104", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "104")));
    assertTrue("Requested Compaction must be present for 105", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "105")));
    assertTrue("Inflight Compaction must be present for 105", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "105")));

    //read the file
    HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(dfs,
        new HoodieLogFile(new Path(basePath + "/.hoodie/.commits_.archive.1_1-0-1")),
        HoodieArchivedMetaEntry.getClassSchema());

    int archivedRecordsCount = 0;
    List<IndexedRecord> readRecords = new ArrayList<>();
    //read the avro blocks and validate the number of records written in each avro block
    while (reader.hasNext()) {
      HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
      List<IndexedRecord> records = blk.getRecords();
      readRecords.addAll(records);
      assertEquals("Archived and read records for each block are same", 8, records.size());
      archivedRecordsCount += records.size();
    }
    assertEquals("Total archived records and total read records are the same count", 8, archivedRecordsCount);

    //make sure the archived commits are the same as the (originalcommits - commitsleft)
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
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build()).build();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(dfs.getConf(), basePath);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "100"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "100"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "101"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "101", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "102"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "102"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    // Requested Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "103"), dfs.getConf());
    // Inflight Compaction
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "103"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());

    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals("Loaded 4 commits and the count should match", 4, timeline.countInstants());
    boolean result = archiveLog.archiveIfRequired(jsc);
    assertTrue(result);
    timeline = metaClient.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    assertEquals("Should not archive commits when maxCommitsToKeep is 5", 4, timeline.countInstants());

    List<HoodieInstant> instants =
        HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
            new Path(metaClient.getMetaAuxiliaryPath()),
            HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);
    assertEquals("Should not delete any aux compaction files when maxCommitsToKeep is 5", 8, instants.size());
    assertTrue("Requested Compaction must be present for 100", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "100")));
    assertTrue("Inflight Compaction must be present for 100", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "100")));
    assertTrue("Requested Compaction must be present for 101", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "101")));
    assertTrue("Inflight Compaction must be present for 101", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "101")));
    assertTrue("Requested Compaction must be present for 102", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "102")));
    assertTrue("Inflight Compaction must be present for 102", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "102")));
    assertTrue("Requested Compaction must be present for 103", instants.contains(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "103")));
    assertTrue("Inflight Compaction must be present for 103", instants.contains(
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, "103")));
  }

  @Test
  public void testArchiveCommitSafety() throws IOException {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build()).build();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(dfs.getConf(), basePath);
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
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build()).build();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(dfs.getConf(), basePath);
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
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable("test-trip-table").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 5).build()).build();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(dfs.getConf(), basePath);
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, metaClient);
    HoodieTestDataGenerator.createCommitFile(basePath, "100", dfs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "101", dfs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "101"), dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "102", dfs.getConf());
    HoodieTestDataGenerator.createCommitFile(basePath, "103", dfs.getConf());
    HoodieTestDataGenerator.createCompactionRequestedFile(basePath, "104", dfs.getConf());
    HoodieTestDataGenerator.createCompactionAuxiliaryMetadata(basePath,
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, "104"), dfs.getConf());
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
    assertEquals(
        "Since we have a pending compaction at 101, we should never archive any commit "
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
