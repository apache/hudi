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

package org.apache.hudi.hadoop;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieParquetInputFormat {

  protected HoodieParquetInputFormat inputFormat;
  protected JobConf jobConf;
  private final HoodieFileFormat baseFileFormat = HoodieFileFormat.PARQUET;
  private final String baseFileExtension = baseFileFormat.getFileExtension();

  public static void ensureFilesInCommit(String msg, FileStatus[] files, String commit, int expected) {
    int count = 0;
    for (FileStatus file : files) {
      String commitTs = FSUtils.getCommitTime(file.getPath().getName());
      if (commit.equals(commitTs)) {
        count++;
      }
    }
    assertEquals(expected, count, msg);
  }

  @BeforeEach
  public void setUp() {
    inputFormat = new HoodieParquetInputFormat();
    jobConf = new JobConf();
    inputFormat.setConf(jobConf);
  }

  @TempDir
  public java.nio.file.Path basePath;

  // Verify that HoodieParquetInputFormat does not return instants after pending compaction
  @Test
  public void testPendingCompactionWithActiveCommits() throws IOException {
    // setup 4 sample instants in timeline
    List<HoodieInstant> instants = new ArrayList<>();
    HoodieInstant t1 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant t2 = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant t3 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "3");
    HoodieInstant t4 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "4");
    HoodieInstant t5 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "5");
    HoodieInstant t6 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "6");

    instants.add(t1);
    instants.add(t2);
    instants.add(t3);
    instants.add(t4);
    instants.add(t5);
    instants.add(t6);
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath.toString());
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    timeline.setInstants(instants);

    // Verify getCommitsTimelineBeforePendingCompaction does not return instants after first compaction instant
    HoodieTimeline filteredTimeline = HoodieInputFormatUtils.filterInstantsTimeline(timeline);
    assertTrue(filteredTimeline.containsInstant(t1));
    assertTrue(filteredTimeline.containsInstant(t2));
    assertFalse(filteredTimeline.containsInstant(t3));
    assertFalse(filteredTimeline.containsInstant(t4));
    assertFalse(filteredTimeline.containsInstant(t5));
    assertFalse(filteredTimeline.containsInstant(t6));
    // remove compaction instant and setup timeline again
    instants.remove(t3);
    timeline = new HoodieActiveTimeline(metaClient);
    timeline.setInstants(instants);
    filteredTimeline = HoodieInputFormatUtils.filterInstantsTimeline(timeline);

    // verify all remaining instants are returned.
    assertTrue(filteredTimeline.containsInstant(t1));
    assertTrue(filteredTimeline.containsInstant(t2));
    assertFalse(filteredTimeline.containsInstant(t3));
    assertTrue(filteredTimeline.containsInstant(t4));
    assertFalse(filteredTimeline.containsInstant(t5));
    assertFalse(filteredTimeline.containsInstant(t6));

    // remove remaining compaction instant and setup timeline again
    instants.remove(t5);
    timeline = new HoodieActiveTimeline(metaClient);
    timeline.setInstants(instants);
    filteredTimeline = HoodieInputFormatUtils.filterInstantsTimeline(timeline);

    // verify all remaining instants are returned.
    assertTrue(filteredTimeline.containsInstant(t1));
    assertTrue(filteredTimeline.containsInstant(t2));
    assertFalse(filteredTimeline.containsInstant(t3));
    assertTrue(filteredTimeline.containsInstant(t4));
    assertFalse(filteredTimeline.containsInstant(t5));
    assertTrue(filteredTimeline.containsInstant(t6));
  }

  @Test
  public void testInputFormatLoad() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
    assertEquals(10, inputSplits.length);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
  }

  @Test
  public void testInputFormatLoadForNonPartitionedAndVirtualKeyedTable() throws IOException {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    File partitionDir = InputFormatTestUtil.prepareCustomizedTable(basePath, baseFileFormat, 10, "100", true, false,
        true, schema);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(basePath.toString(), "100", Option.of(commitMetadata));

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
    assertEquals(10, inputSplits.length);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
  }

  @Test
  public void testInputFormatLoadWithEmptyTable() throws IOException {
    // initial hoodie table
    String bathPathStr = "/tmp/test_empty_table";
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), bathPathStr, HoodieTableType.COPY_ON_WRITE,
            baseFileFormat);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, bathPathStr);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length);
    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 0);
    assertEquals(0, inputSplits.length);
  }

  @Test
  public void testInputFormatUpdates() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);

    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 5, "200", true);
    // Before the commit
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("Commit 200 has not been committed. We should not see files from this commit", files, "200", 0);
    InputFormatTestUtil.commit(basePath, "200");
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("5 files have been updated to commit 200. We should see 5 files from commit 200 and 5 "
        + "files from 100 commit", files, "200", 5);
    ensureFilesInCommit("5 files have been updated to commit 200. We should see 5 files from commit 100 and 5 "
        + "files from 200 commit", files, "100", 5);
  }

  @Test
  public void testSnapshotWithInvalidCommitShouldThrowException() throws IOException {
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    InputFormatTestUtil.setupSnapshotIncludePendingCommits(jobConf, "1");
    Exception exception = assertThrows(HoodieIOException.class, () -> inputFormat.listStatus(jobConf));
    assertEquals("Query instant (1) not found in the timeline", exception.getMessage());

    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "1");
    exception = assertThrows(HoodieIOException.class, () -> inputFormat.listStatus(jobConf));
    assertEquals("Query instant (1) not found in the timeline", exception.getMessage());
  }

  @Test
  public void testPointInTimeQueryWithUpdates() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);

    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 5, "200", true);
    // Before the commit
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("Commit 200 has not been committed. We should not see files from this commit", files, "200", 0);
    InputFormatTestUtil.commit(basePath, "200");

    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "100");

    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("We shouldn't have any file pertaining to commit 200", files, "200", 0);
    ensureFilesInCommit("All files should be from commit 100", files, "100", 10);

    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "200");
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);
    ensureFilesInCommit("5 files for commit 200", files, "200", 5);
    ensureFilesInCommit("5 files for commit 100", files, "100", 5);
  }

  @Test
  public void testInputFormatWithCompaction() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
    assertEquals(10, inputSplits.length);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);

    // simulate compaction requested
    createCompactionFile(basePath, "125");

    // add inserts after compaction timestamp
    InputFormatTestUtil.simulateInserts(partitionDir, baseFileExtension, "fileId2", 5, "200");
    InputFormatTestUtil.commit(basePath, "200");

    // verify snapshot reads show all new inserts even though there is pending compaction
    files = inputFormat.listStatus(jobConf);
    assertEquals(15, files.length);

    // verify that incremental reads do NOT show inserts after compaction timestamp
    InputFormatTestUtil.setupIncremental(jobConf, "100", 10);
    files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length,
        "We should exclude commit 200 when there is a pending compaction at 150");
  }

  @Test
  public void testIncrementalSimple() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString(),
            HoodieTableType.COPY_ON_WRITE, baseFileFormat);
    assertEquals(null, metaClient.getTableConfig().getDatabaseName(),
        "When hoodie.database.name is not set, it should default to null");

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length,
        "We should exclude commit 100 when returning incremental pull with start commit time as 100");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1, true);

    files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length,
        "We should exclude commit 100 when returning incremental pull with start commit time as 100");

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
            baseFileFormat, HoodieTestUtils.HOODIE_DATABASE);
    assertEquals(HoodieTestUtils.HOODIE_DATABASE, metaClient.getTableConfig().getDatabaseName(),
        String.format("The hoodie.database.name should be %s ", HoodieTestUtils.HOODIE_DATABASE));

    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.incremental.use.database is true and hoodie.database.name is not null or empty"
                + " and the incremental database name is not set, then the incremental query will not take effect");
  }

  @Test
  public void testIncrementalWithDatabaseName() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1, HoodieTestUtils.HOODIE_DATABASE, true);

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString(),
            HoodieTableType.COPY_ON_WRITE, baseFileFormat);
    assertEquals(null, metaClient.getTableConfig().getDatabaseName(),
        "When hoodie.database.name is not set, it should default to null");

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.database.name is null, then the incremental query will not take effect");

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
            baseFileFormat, "");
    assertEquals("", metaClient.getTableConfig().getDatabaseName(),
        "The hoodie.database.name should be empty");

    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.database.name is empty, then the incremental query will not take effect");

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
            baseFileFormat, HoodieTestUtils.HOODIE_DATABASE);
    assertEquals(HoodieTestUtils.HOODIE_DATABASE, metaClient.getTableConfig().getDatabaseName(),
            String.format("The hoodie.database.name should be %s ", HoodieTestUtils.HOODIE_DATABASE));

    files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length,
        "We should exclude commit 100 when returning incremental pull with start commit time as 100");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1, HoodieTestUtils.HOODIE_DATABASE, false);

    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.incremental.use.database is false and the incremental database name is set, "
                + "then the incremental query will not take effect");

    // The configuration with and without database name exists together
    InputFormatTestUtil.setupIncremental(jobConf, "1", 1, true);

    files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length,
        "When hoodie.incremental.use.database is true, "
                + "We should exclude commit 100 because the returning incremental pull with start commit time is 100");

    InputFormatTestUtil.setupIncremental(jobConf, "1", 1, false);
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.incremental.use.database is false, "
                + "We should include commit 100 because the returning incremental pull with start commit time is 1");
  }

  @Test
  public void testMultiPartitionTableIncremental() throws IOException {
    // initial commit
    java.nio.file.Path tablePath = Paths.get(basePath.toString(), "raw_trips");

    // create hudi table and insert data to it
    // create only one file
    File partitionDir1 = InputFormatTestUtil
        .prepareMultiPartitionTable(basePath, baseFileFormat, 1, "100", "1");
    createCommitFile(basePath, "100", "2016/05/1");

    // insert new data to partition "2016/05/11"
    // create only one file
    File partitionDir2 = InputFormatTestUtil
        .prepareMultiPartitionTable(basePath, baseFileFormat, 1, "100", "11");
    createCommitFile(basePath, "101", "2016/05/11");


    // now partitionDir2.getPath().contain(partitionDir1.getPath()), and hudi-1817 will occur
    assertEquals(true, partitionDir2.getPath().contains(partitionDir1.getPath()));

    // set partitionDir2 to be the inputPaths of current inputFormat
    FileInputFormat.setInputPaths(jobConf, partitionDir2.getPath());

    // set incremental startCommit=0 and numberOfCommitsToPull=3  to pull all the data from hudi table
    InputFormatTestUtil.setupIncremental(jobConf, "0", 3);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(1, files.length,
        "We should get one file from partition: " + partitionDir2.getPath());
  }

  @Test
  public void testIncrementalEmptyPartitions() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputFormatTestUtil.setupIncremental(jobConf, "000", 1);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "We should include only 1 commit 100 when returning incremental pull with start commit time as 100");
    ensureFilesInCommit("Pulling 1 commits from 000, should get us the 10 files from 100 commit", files, "100", 10);

    // Add new commit only to a new partition
    partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "200");
    createCommitFile(basePath, "200", "2017/05/01");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);
    files = inputFormat.listStatus(jobConf);

    assertEquals(0, files.length,
        "We should exclude commit 200 when returning incremental pull with start commit time as 100 as filePaths does not include new partition");
  }

  private void createCommitFile(java.nio.file.Path basePath, String commitNumber, String partitionPath)
      throws IOException {
    List<HoodieWriteStat> writeStats = HoodieTestUtils.generateFakeHoodieWriteStat(1);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    writeStats.forEach(stat -> commitMetadata.addWriteStat(partitionPath, stat));
    File file = basePath.resolve(".hoodie").resolve(commitNumber + ".commit").toFile();
    file.createNewFile();
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    fileOutputStream.write(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private File createCompactionFile(java.nio.file.Path basePath, String commitTime)
      throws IOException {
    File file = basePath.resolve(".hoodie")
        .resolve(HoodieTimeline.makeRequestedCompactionFileName(commitTime)).toFile();
    assertTrue(file.createNewFile());
    FileOutputStream os = new FileOutputStream(file);
    try {
      HoodieCompactionPlan compactionPlan = HoodieCompactionPlan.newBuilder().setVersion(2).build();
      // Write empty commit metadata
      os.write(TimelineMetadataUtils.serializeCompactionPlan(compactionPlan).get());
      return file;
    } finally {
      os.close();
    }
  }

  @Test
  public void testIncrementalWithMultipleCommits() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 5, "200", false);
    createCommitFile(basePath, "200", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 4, "300", false);
    createCommitFile(basePath, "300", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 3, "400", false);
    createCommitFile(basePath, "400", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 2, "500", false);
    createCommitFile(basePath, "500", "2016/05/01");

    InputFormatTestUtil.simulateUpdates(partitionDir, baseFileExtension, "100", 1, "600", false);
    createCommitFile(basePath, "600", "2016/05/01");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(5, files.length, "Pulling 1 commit from 100, should get us the 5 files committed at 200");
    ensureFilesInCommit("Pulling 1 commit from 100, should get us the 5 files committed at 200", files, "200", 5);

    InputFormatTestUtil.setupIncremental(jobConf, "100", 3);
    files = inputFormat.listStatus(jobConf);

    assertEquals(5, files.length, "Pulling 3 commits from 100, should get us the 3 files from 400 commit, 1 file from 300 "
        + "commit and 1 file from 200 commit");
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 3 files from 400 commit", files, "400", 3);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 300 commit", files, "300", 1);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 200 commit", files, "200", 1);

    InputFormatTestUtil.setupIncremental(jobConf, "100", HoodieHiveUtils.MAX_COMMIT_ALL);
    files = inputFormat.listStatus(jobConf);

    assertEquals(5, files.length,
        "Pulling all commits from 100, should get us the 1 file from each of 200,300,400,500,400 commits");
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 600 commit", files, "600", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 500 commit", files, "500", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 400 commit", files, "400", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 300 commit", files, "300", 1);
    ensureFilesInCommit("Pulling all commits from 100, should get us the 1 files from 200 commit", files, "200", 1);
  }

  @Disabled("enable this after enabling predicate push down")
  @Test
  public void testPredicatePushDown() throws IOException {
    // initial commit
    Schema schema = getSchemaFromResource(getClass(), "/sample1.avsc");
    String commit1 = "20160628071126";
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 10, commit1);
    InputFormatTestUtil.commit(basePath, commit1);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    // check whether we have 10 records at this point
    ensureRecordsInCommit("We need to have 10 records at this point for commit " + commit1, commit1, 10, 10);

    // update 2 records in the original parquet file and save it as commit 200
    String commit2 = "20160629193623";
    InputFormatTestUtil.simulateParquetUpdates(partitionDir, schema, commit1, 10, 2, commit2);
    InputFormatTestUtil.commit(basePath, commit2);

    InputFormatTestUtil.setupIncremental(jobConf, commit1, 1);
    // check whether we have 2 records at this point
    ensureRecordsInCommit("We need to have 2 records that was modified at commit " + commit2 + " and no more", commit2,
        2, 2);
    // Make sure we have the 10 records if we roll back the start time
    InputFormatTestUtil.setupIncremental(jobConf, "0", 2);
    ensureRecordsInCommit("We need to have 8 records that was modified at commit " + commit1 + " and no more", commit1,
        8, 10);
    ensureRecordsInCommit("We need to have 2 records that was modified at commit " + commit2 + " and no more", commit2,
        2, 10);
  }

  @Test
  public void testGetIncrementalTableNames() throws IOException {
    String[] expectedIncrTables = {"db1.raw_trips", "db2.model_trips", "db3.model_trips"};
    JobConf conf = new JobConf();
    String incrementalMode1 = String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, expectedIncrTables[0]);
    conf.set(incrementalMode1, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);
    String incrementalMode2 = String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, expectedIncrTables[1]);
    conf.set(incrementalMode2, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);
    String incrementalMode3 = String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, "db3.model_trips");
    conf.set(incrementalMode3, HoodieHiveUtils.INCREMENTAL_SCAN_MODE.toLowerCase());
    String defaultMode = String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, "db3.first_trips");
    conf.set(defaultMode, HoodieHiveUtils.DEFAULT_SCAN_MODE);
    List<String> actualIncrTables = HoodieHiveUtils.getIncrementalTableNames(Job.getInstance(conf));
    for (String expectedIncrTable : expectedIncrTables) {
      assertTrue(actualIncrTables.contains(expectedIncrTable));
    }
  }

  // test incremental read does not go past compaction instant for RO views
  @Test
  public void testIncrementalWithPendingCompaction() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // simulate compaction requested at 300
    File compactionFile = createCompactionFile(basePath, "300");

    // write inserts into new bucket
    InputFormatTestUtil.simulateInserts(partitionDir, baseFileExtension, "fileId2", 10, "400");
    createCommitFile(basePath, "400", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    InputFormatTestUtil.setupIncremental(jobConf, "0", -1);
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "Pulling all commit from beginning, should not return instants after begin compaction");
    ensureFilesInCommit("Pulling all commit from beginning, should not return instants after begin compaction",
        files, "100", 10);

    // delete compaction and verify inserts show up
    compactionFile.delete();
    InputFormatTestUtil.setupIncremental(jobConf, "0", -1);
    files = inputFormat.listStatus(jobConf);
    assertEquals(20, files.length,
        "after deleting compaction, should get all inserted files");

    ensureFilesInCommit("Pulling all commit from beginning, should return instants before requested compaction",
        files, "100", 10);
    ensureFilesInCommit("Pulling all commit from beginning, should return instants after requested compaction",
        files, "400", 10);

  }

  @Test
  public void testSnapshotPreCommitValidate() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    createCommitFile(basePath, "100", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length, "Snapshot read must return all files in partition");

    // add more files
    InputFormatTestUtil.simulateInserts(partitionDir, baseFileExtension, "fileId2-", 5, "200");
    FileCreateUtils.createInflightCommit(basePath.toString(), "200");

    // Verify that validate mode reads uncommitted files
    InputFormatTestUtil.setupSnapshotIncludePendingCommits(jobConf, "200");
    files = inputFormat.listStatus(jobConf);
    assertEquals(15, files.length, "Must return uncommitted files");
    ensureFilesInCommit("Pulling 1 commit from 100, should get us the 5 files committed at 200", files, "200", 5);
    ensureFilesInCommit("Pulling 1 commit from 100, should get us the 10 files committed at 100", files, "100", 10);

    try {
      // Verify that Validate mode throws error with invalid commit time
      InputFormatTestUtil.setupSnapshotIncludePendingCommits(jobConf, "300"); 
      inputFormat.listStatus(jobConf);
      fail("Expected list status to fail when validate is called with unknown timestamp");
    } catch (HoodieIOException e) {
      // expected because validate is called with invalid instantTime
    }
    
    //Creating a new jobCOnf Object because old one has hoodie.%.consume.commit set
    jobConf = new JobConf();
    inputFormat.setConf(jobConf);
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    // verify that snapshot mode skips uncommitted files
    InputFormatTestUtil.setupSnapshotScanMode(jobConf);
    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length, "snapshot scan mode must not return uncommitted files");
    ensureFilesInCommit("Pulling 1 commit from 100, should get us the 10 files committed at 100", files, "100", 10);
  }

  private void ensureRecordsInCommit(String msg, String commit, int expectedNumberOfRecordsInCommit,
      int totalExpected) throws IOException {
    int actualCount = 0;
    int totalCount = 0;
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    for (InputSplit split : splits) {
      RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(split, jobConf, null);
      NullWritable key = recordReader.createKey();
      ArrayWritable writable = recordReader.createValue();

      while (recordReader.next(key, writable)) {
        // writable returns an array with [field1, field2, _hoodie_commit_time,
        // _hoodie_commit_seqno]
        // Take the commit time and compare with the one we are interested in
        if (commit.equals((writable.get()[2]).toString())) {
          actualCount++;
        }
        totalCount++;
      }
    }
    assertEquals(expectedNumberOfRecordsInCommit, actualCount, msg);
    assertEquals(totalExpected, totalCount, msg);
  }
}
