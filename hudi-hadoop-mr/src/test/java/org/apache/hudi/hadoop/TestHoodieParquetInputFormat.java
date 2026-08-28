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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.hadoop.HoodieColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;
import static org.apache.hudi.hadoop.HoodieColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    HoodieInstant t1 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant t2 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant t3 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "3");
    HoodieInstant t4 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "4");
    HoodieInstant t5 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "5");
    HoodieInstant t6 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "6");

    instants.add(t1);
    instants.add(t2);
    instants.add(t3);
    instants.add(t4);
    instants.add(t5);
    instants.add(t6);
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath.toString());
    HoodieActiveTimeline timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
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
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
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
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
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
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    File partitionDir = InputFormatTestUtil.prepareCustomizedTable(basePath, baseFileFormat, 10, "100", true, false,
        true, schema);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, basePath.toString(), "100", Option.of(commitMetadata));

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
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), bathPathStr, HoodieTableType.COPY_ON_WRITE,
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

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(),
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

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
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

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(),
        HoodieTableType.COPY_ON_WRITE, baseFileFormat);
    assertEquals(null, metaClient.getTableConfig().getDatabaseName(),
        "When hoodie.database.name is not set, it should default to null");

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.database.name is null, then the incremental query will not take effect");

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
        baseFileFormat, "");
    assertEquals(null, metaClient.getTableConfig().getDatabaseName(),
        "The hoodie.database.name will be null if set to empty");

    files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length,
        "When hoodie.database.name is empty, then the incremental query will not take effect");

    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
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
    File file = basePath.resolve(".hoodie/timeline")
        .resolve(commitNumber + "_" + InProcessTimeGenerator.createNewInstantTime() + ".commit").toFile();
    file.createNewFile();
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      COMMIT_METADATA_SER_DE.getInstantWriter(commitMetadata).get().writeToStream(fileOutputStream);
      fileOutputStream.flush();
    }
  }

  private File createCompactionFile(java.nio.file.Path basePath, String commitTime)
      throws IOException {
    File file = basePath.resolve(".hoodie/timeline")
        .resolve(INSTANT_FILE_NAME_GENERATOR.makeRequestedCompactionFileName(commitTime)).toFile();
    assertTrue(file.createNewFile());
    try (FileOutputStream os = new FileOutputStream(file)) {
      HoodieCompactionPlan compactionPlan = HoodieCompactionPlan.newBuilder().setVersion(2).build();
      // Write empty commit metadata
      COMMIT_METADATA_SER_DE.getInstantWriter(compactionPlan).get().writeToStream(os);
      return file;
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
    HoodieSchema schema = getSchemaFromResource(getClass(), "/sample1.avsc");
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
    FileCreateUtilsLegacy.createInflightCommit(basePath.toString(), "200");

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

  /**
   * Test scenario where inflight commit is between completed commits.
   */
  @Test
  public void testSnapshotPreCommitValidateWithInflights() throws IOException {
    // Create commit and data files with commit 000
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 5, "000");
    createCommitFile(basePath, "000", "2016/05/01");

    // create inflight commit add more files with same file_id.
    InputFormatTestUtil.simulateInserts(partitionDir, baseFileExtension, "fileId1", 5, "100");
    FileCreateUtilsLegacy.createInflightCommit(basePath.toString(), "100");

    // Create another commit without datafiles.
    createCommitFile(basePath, "200", "2016/05/01");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    // Now, the original data files with commit time 000 should be returned.
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(5, files.length, "Snapshot read must return all files in partition");
    ensureFilesInCommit("Should return base files from commit 000, inflight data files with "
        + "greater timestamp should be filtered", files, "000", 5);

    // Create data files with same file_id for commit 200.
    InputFormatTestUtil.simulateInserts(partitionDir, baseFileExtension, "fileId1", 5, "200");

    // This time data files from commit time 200 will be returned.
    files = inputFormat.listStatus(jobConf);
    assertEquals(5, files.length, "Snapshot read must return all files in partition");
    ensureFilesInCommit("Only completed commits files should be returned.",
        files, "200", 5);
  }

  @Test
  public void testInputFormatLoadForEmptyPartitionedTable() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareTable(basePath, baseFileFormat, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the empty paths
    String emptyPath = ClassLoader.getSystemResource("emptyFile").getPath();
    FileInputFormat.setInputPaths(jobConf, emptyPath);

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
    assertEquals(1, inputSplits.length);

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(1, files.length);
  }

  @Test
  public void testLegacyReaderFailsFastOnShreddedVariantColumn() throws Exception {
    // With the file group reader disabled the split lands on Hive's plain parquet reader, which
    // silently nulls typed_value. The footer-derived file schema carries no variant logical type
    // (variant groups convert to plain records), so the guard has to fire off the shape.
    JobConf fullProjection = variantJobConf("id,v", "int,struct<metadata:binary,value:binary>", "id,v", "0,1");
    FileSplit shredded = fileSplit(InputFormatTestUtil.writeVariantParquetFile(basePath, "shredded.parquet", true));

    HoodieException failure = assertThrows(HoodieException.class,
        () -> inputFormat.getRecordReader(shredded, fullProjection, null));
    assertTrue(failure.getMessage().contains("shredded variant") && failure.getMessage().contains("'v'"),
        "The error must name the shredded variant column, got: " + failure.getMessage());

    // A projection that skips the variant column keeps reading.
    JobConf withoutVariant = variantJobConf("id,v", "int,struct<metadata:binary,value:binary>", "id", "0");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(shredded, withoutVariant));

    // The unshredded twin keeps reading at the full projection.
    FileSplit unshredded = fileSplit(InputFormatTestUtil.writeVariantParquetFile(basePath, "unshredded.parquet", false));
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(unshredded, fullProjection));

    // A bootstrap split (shouldUseFilegroupReader is false for one by construction) is guarded on
    // its bootstrap half; its own file is the skeleton, which carries meta columns only.
    BootstrapBaseFileSplit bootstrapSplit = new BootstrapBaseFileSplit(unshredded, shredded);
    HoodieException bootstrapFailure = assertThrows(HoodieException.class,
        () -> inputFormat.getRecordReader(bootstrapSplit, fullProjection, null));
    assertTrue(bootstrapFailure.getMessage().contains("'v'"),
        "The error must name the shredded variant column of the bootstrap file, got: " + bootstrapFailure.getMessage());

    // Schema-on-read is the second bypass of the file group reader: shouldUseFilegroupReader is
    // false while it is on, even with the file group reader itself back at its default.
    JobConf schemaOnRead = variantJobConf("id,v", "int,struct<metadata:binary,value:binary>", "id,v", "0,1");
    schemaOnRead.setBoolean(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), true);
    schemaOnRead.setBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), true);
    HoodieException schemaOnReadFailure = assertThrows(HoodieException.class,
        () -> inputFormat.getRecordReader(shredded, schemaOnRead, null));
    assertTrue(schemaOnReadFailure.getMessage().contains("'v'"),
        "The error must name the shredded variant column on the schema-on-read route, got: " + schemaOnReadFailure.getMessage());
  }

  @Test
  public void testLegacyReaderGuardHonoursNestedPruningAndArrayWrappers() throws Exception {
    // A shredded variant below the top level (the row writer shreds at depth) fails too, naming
    // the struct column that holds it. The Hive type carries a sibling `other` the file does not
    // hold - columns.types comes from the table - so the nested-pruning legs below have a path to
    // project that never reaches the shredded group.
    String nestedTypes = "int,struct<inner:struct<metadata:binary,value:binary>,other:string>";
    FileSplit nested = fileSplit(InputFormatTestUtil.writeNestedShreddedVariantParquetFile(basePath, "nested_shredded.parquet"));
    JobConf wholeColumn = variantJobConf("id,s", nestedTypes, "id,s", "0,1");
    HoodieException nestedFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(nested, wholeColumn));
    assertTrue(nestedFailure.getMessage().contains("'s'"),
        "The error must name the column holding the nested shredded variant, got: " + nestedFailure.getMessage());

    // Hive's read column names are top-level only: nested column pruning arrives separately, as
    // dotted paths. `select s.other` names `s` but materializes nothing of s.inner, so the guard
    // must let it through.
    JobConf otherPruned = variantJobConf("id,s", nestedTypes, "id,s", "0,1");
    otherPruned.set(READ_NESTED_COLUMN_PATH_CONF_STR, "s.other");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(nested, otherPruned));

    // A nested path that does reach the shredded group still fails, whether it names the group
    // itself or the whole column above it.
    JobConf innerPruned = variantJobConf("id,s", nestedTypes, "id,s", "0,1");
    innerPruned.set(READ_NESTED_COLUMN_PATH_CONF_STR, "s.inner");
    HoodieException prunedFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(nested, innerPruned));
    assertTrue(prunedFailure.getMessage().contains("'s'"),
        "The error must name the column holding the nested shredded variant, got: " + prunedFailure.getMessage());
    JobConf columnPruned = variantJobConf("id,s", nestedTypes, "id,s", "0,1");
    columnPruned.set(READ_NESTED_COLUMN_PATH_CONF_STR, "s");
    assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(nested, columnPruned));

    // An array of shredded variants. Neither side names the collection's own levels: the walk
    // drops the repeated group the 2-level layout puts the element in (a.array on disk), so the
    // shredded group lands at the column's own path `a` - which is where Hive's type for the
    // column puts it too.
    String arrayTypes = "int,array<struct<metadata:binary,value:binary>>";
    StoragePath arrayPath = InputFormatTestUtil.writeArrayShreddedVariantParquetFile(basePath, "array_shredded.parquet");
    assertEquals("array", fileSchemaOf(arrayPath).getType("a").asGroupType().getType(0).getName(),
        "the default fixture must be the 2-level layout, whose repeated group is the element itself");
    FileSplit array = fileSplit(arrayPath);
    JobConf wholeArray = variantJobConf("id,a", arrayTypes, "id,a", "0,1");
    HoodieException arrayFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(array, wholeArray));
    assertTrue(arrayFailure.getMessage().contains("'a'"),
        "The error must name the column holding the array of shredded variants, got: " + arrayFailure.getMessage());

    // The nested path Hive carries for the array stops at the column, above the wrapper groups:
    // it is a dotted prefix of the collected path, which the overlap check has to catch.
    JobConf arrayPruned = variantJobConf("id,a", arrayTypes, "id,a", "0,1");
    arrayPruned.set(READ_NESTED_COLUMN_PATH_CONF_STR, "a");
    HoodieException arrayPrunedFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(array, arrayPruned));
    assertTrue(arrayPrunedFailure.getMessage().contains("'a'"),
        "The error must name the column holding the array of shredded variants, got: " + arrayPrunedFailure.getMessage());

    // The 3-level list/element layout, which is what the row writer - the only production writer
    // that shreds inside a collection - emits. Here the repeated group IS a collection level, so
    // the walk has to see through it as well and still land the shredded group at `a`. The layout
    // is pinned on the footer, or the leg could silently degrade back to the 2-level one above.
    StoragePath threeLevelPath =
        InputFormatTestUtil.writeArrayShreddedVariantParquetFile(basePath, "array_shredded_3level.parquet", false, true);
    GroupType repeated = fileSchemaOf(threeLevelPath).getType("a").asGroupType().getType(0).asGroupType();
    assertEquals("list", repeated.getName(), "the 3-level layout names its repeated group `list`");
    assertEquals(1, repeated.getFieldCount(), "the 3-level layout's repeated group holds the element alone");
    assertEquals("element", repeated.getType(0).getName(), "the 3-level layout names its element `element`");
    FileSplit threeLevelArray = fileSplit(threeLevelPath);
    HoodieException threeLevelFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(threeLevelArray, wholeArray));
    assertTrue(threeLevelFailure.getMessage().contains("'a'"),
        "The error must name the column holding the 3-level array of shredded variants, got: " + threeLevelFailure.getMessage());

    // Which levels a collection contributes is structural, not a matter of their names: on the
    // 2-level layout the repeated group is the element itself, so an element member named `value`
    // is a user field and the shredded group sits at a.value - which is where the Hive type puts
    // it too. Reading that member as a collection level would collect a.value as plain `a`, the
    // two sides would not meet, and the guard would let the read through.
    String structArrayTypes = "int,array<struct<key:string,value:struct<metadata:binary,value:binary>>>";
    FileSplit structArray = fileSplit(
        InputFormatTestUtil.writeArrayShreddedVariantParquetFile(basePath, "array_struct_shredded.parquet", true));
    JobConf wholeStructArray = variantJobConf("id,a", structArrayTypes, "id,a", "0,1");
    HoodieException structArrayFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(structArray, wholeStructArray));
    assertTrue(structArrayFailure.getMessage().contains("'a'"),
        "The error must name the column whose array elements hold a shredded variant, got: " + structArrayFailure.getMessage());

    // The rule is applied by the list's own name, as AvroSchemaConverter.isElementType applies it:
    // a repeated group is the element when named `array` or exactly `<list>_tuple`, and a
    // synthetic level otherwise. So other_tuple under the list `a` is synthetic and its single
    // child is the element, landing the shredded group at `a` again. Taking any `_tuple` suffix
    // for the element would append the child's name and collect a.element, which the column's
    // Hive type never declares: the two sides would not meet and the read would go through with
    // typed_value silently dropped.
    StoragePath misnamedTuplePath =
        InputFormatTestUtil.writeMisnamedTupleListShreddedVariantParquetFile(basePath, "array_shredded_other_tuple.parquet");
    GroupType misnamedRepeated = fileSchemaOf(misnamedTuplePath).getType("a").asGroupType().getType(0).asGroupType();
    assertEquals("other_tuple", misnamedRepeated.getName(), "the fixture's repeated group must carry a foreign _tuple name");
    assertEquals(1, misnamedRepeated.getFieldCount(), "the fixture's repeated group holds the element alone");
    FileSplit misnamedTuple = fileSplit(misnamedTuplePath);
    HoodieException misnamedTupleFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(misnamedTuple, wholeArray));
    assertTrue(misnamedTupleFailure.getMessage().contains("'a'"),
        "The error must name the column whose misnamed list level holds a shredded variant, got: " + misnamedTupleFailure.getMessage());
  }

  @Test
  public void testLegacyReaderGuardSeesThroughMapEntries() throws Exception {
    // A map of shredded variants. On disk the shredded group sits at m.key_value.value, under the
    // entry level and the entry's value, both levels a Hive dotted path never names: the column's
    // Hive type puts the variant at `m` itself. The walk has to drop those two levels, or the two
    // sides never meet and the read goes through with typed_value silently dropped.
    String mapTypes = "int,map<string,struct<metadata:binary,value:binary>>";
    StoragePath mapPath = InputFormatTestUtil.writeMapShreddedVariantParquetFile(basePath, "map_shredded.parquet");
    GroupType entry = fileSchemaOf(mapPath).getType("m").asGroupType().getType(0).asGroupType();
    assertEquals("key_value", entry.getName(), "the fixture's map must keep its entries under key_value");
    assertEquals("value", entry.getType(1).getName(), "the fixture's map entry must hold the variant as `value`");
    FileSplit map = fileSplit(mapPath);
    JobConf wholeMap = variantJobConf("id,m", mapTypes, "id,m", "0,1");
    HoodieException mapFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(map, wholeMap));
    assertTrue(mapFailure.getMessage().contains("'m'"),
        "The error must name the column whose map values hold a shredded variant, got: " + mapFailure.getMessage());

    // The nested path Hive carries for the map stops at the column, as it does for a list.
    JobConf mapPruned = variantJobConf("id,m", mapTypes, "id,m", "0,1");
    mapPruned.set(READ_NESTED_COLUMN_PATH_CONF_STR, "m");
    HoodieException mapPrunedFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(map, mapPruned));
    assertTrue(mapPrunedFailure.getMessage().contains("'m'"),
        "The error must name the column whose map values hold a shredded variant, got: " + mapPrunedFailure.getMessage());
  }

  @Test
  public void testLegacyReaderGuardIsBestEffort() throws Exception {
    FileSplit shredded = fileSplit(InputFormatTestUtil.writeVariantParquetFile(basePath, "shredded.parquet", true));

    // count(*) reads no column data and reaches the guard with empty read column names, which must
    // keep reading. `select *` is not this shape: HiveInputFormat.pushProjection writes the full
    // column-name list for it, and setReadAllColumns only ever runs on ProjectionPusher's cloned
    // JobConf, downstream of getRecordReader.
    JobConf countStar = variantJobConf("id,v", "int,struct<metadata:binary,value:binary>", "", "");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(shredded, countStar));

    // A malformed columns/columns.types pairing (fewer types than columns) is best-effort: the
    // read the plain parquet reader would serve must not fail.
    JobConf mismatchedTypes = variantJobConf("id,v", "struct<metadata:binary,value:binary>", "id,v", "0,1");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(shredded, mismatchedTypes));

    // A columns.types string the guard cannot parse is best-effort too: `notatype` is no Hive
    // type, so the TypeInfoUtils parse throws while the variant marker still passes the raw screen.
    JobConf unparseableTypes =
        variantJobConf("id,v", "int,struct<metadata:binary,value:binary>,notatype", "id,v", "0,1");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(shredded, unparseableTypes));

    // An INT96 column anywhere in the file (Spark's default timestamp encoding) must not break the
    // guard: converting the footer to avro throws "INT96 is deprecated" before reaching the variant.
    JobConf withInt96 = variantJobConf("id,v,ts", "int,struct<metadata:binary,value:binary>,timestamp", "id,v", "0,1");
    StoragePath int96UnshreddedPath = InputFormatTestUtil.writeVariantParquetFile(basePath, "int96_unshredded.parquet", false, true);
    assertEquals(PrimitiveTypeName.INT96,
        fileSchemaOf(int96UnshreddedPath).getType("ts").asPrimitiveType().getPrimitiveTypeName(),
        "the ts column must really be INT96, or this leg proves nothing");
    FileSplit int96Unshredded = fileSplit(int96UnshreddedPath);
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(int96Unshredded, withInt96));
    FileSplit int96Shredded = fileSplit(InputFormatTestUtil.writeVariantParquetFile(basePath, "int96_shredded.parquet", true, true));
    HoodieException int96Failure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(int96Shredded, withInt96));
    assertTrue(int96Failure.getMessage().contains("'v'"),
        "The error must name the shredded variant column, got: " + int96Failure.getMessage());

    // A synced VARIANT is exactly struct<metadata:binary,value:binary>, so a Hive type that also
    // carries typed_value is a plain user struct of that shape, not a variant: it is not anchored,
    // even over a file whose group would match the shredded shape.
    JobConf plainStruct = variantJobConf(
        "id,v", "int,struct<metadata:binary,value:binary,typed_value:struct<a:int>>", "id,v", "0,1");
    assertDoesNotThrow(() -> HoodieParquetInputFormat.validateNoShreddedVariantRead(shredded, plainStruct));

    // The exemption is anchored on the node shape, not on typed_value being absent from the whole
    // column type string: a real variant at s.inner is still flagged when the sibling s.other is a
    // user struct whose own member happens to be named typed_value.
    FileSplit typedValueSibling = fileSplit(
        InputFormatTestUtil.writeNestedShreddedVariantParquetFile(basePath, "nested_typed_value_sibling.parquet", true));
    JobConf siblingTypes = variantJobConf(
        "id,s", "int,struct<inner:struct<metadata:binary,value:binary>,other:struct<typed_value:string>>", "id,s", "0,1");
    HoodieException siblingFailure = assertThrows(HoodieException.class,
        () -> HoodieParquetInputFormat.validateNoShreddedVariantRead(typedValueSibling, siblingTypes));
    assertTrue(siblingFailure.getMessage().contains("'s'"),
        "A typed_value member on a sibling struct must not exempt the variant next to it, got: " + siblingFailure.getMessage());
  }

  /**
   * A fresh JobConf on the legacy (file-group-reader disabled) route, projecting {@code readNames}
   * / {@code readIds} out of the {@code columns} / {@code types} table description.
   */
  private static JobConf variantJobConf(String columns, String types, String readNames, String readIds) {
    JobConf conf = new JobConf();
    conf.setBoolean(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), false);
    conf.set(IOConstants.COLUMNS, columns);
    conf.set(IOConstants.COLUMNS_TYPES, types);
    conf.set(READ_COLUMN_NAMES_CONF_STR, readNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readIds);
    return conf;
  }

  private static FileSplit fileSplit(StoragePath path) {
    return new FileSplit(new Path(path.toUri()), 0, Long.MAX_VALUE, (String[]) null);
  }

  /** The footer's file schema: what the fixture legs pin their on-disk layout on. */
  private static MessageType fileSchemaOf(StoragePath path) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(path.toUri()), new JobConf()))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
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
      recordReader.close();
    }
    assertEquals(expectedNumberOfRecordsInCommit, actualCount, msg);
    assertEquals(totalExpected, totalCount, msg);
  }

  @Test
  public void testHoodieParquetInputFormatReadTimeType() throws IOException {
    try {
      long testTimestampLong = System.currentTimeMillis();
      int testDate = 19116;// 2022-05-04

      HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(getClass(), "/test_timetype.avsc");
      String commit = "20160628071126";
      HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(),
          HoodieTableType.COPY_ON_WRITE, HoodieFileFormat.PARQUET);
      java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "06", "28"));
      String fileId = FSUtils.makeBaseFileName(commit, "1-0-1", "fileid1",
          HoodieFileFormat.PARQUET.getFileExtension());
      try (AvroParquetWriter parquetWriter = new AvroParquetWriter(
          new Path(partitionPath.resolve(fileId).toString()), schema.toAvroSchema())) {
        GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
        record.put("test_timestamp", testTimestampLong * 1000);
        record.put("test_long", testTimestampLong * 1000);
        record.put("test_date", testDate);
        record.put("_hoodie_commit_time", commit);
        record.put("_hoodie_commit_seqno", commit + 1);
        parquetWriter.write(record);
      }

      //this is not a hoodie table!!
      jobConf.set(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "false");
      jobConf.set(IOConstants.COLUMNS, "test_timestamp,test_long,test_date,_hoodie_commit_time,_hoodie_commit_seqno");
      jobConf.set(IOConstants.COLUMNS_TYPES, "timestamp,bigint,date,string,string");
      jobConf.set(READ_COLUMN_NAMES_CONF_STR, "test_timestamp,test_long,test_date,_hoodie_commit_time,_hoodie_commit_seqno");
      InputFormatTestUtil.setupPartition(basePath, partitionPath);
      InputFormatTestUtil.commit(basePath, commit);
      FileInputFormat.setInputPaths(jobConf, partitionPath.toFile().getPath());

      InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
      for (InputSplit split : splits) {
        RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat
            .getRecordReader(split, jobConf, null);
        NullWritable key = recordReader.createKey();
        ArrayWritable writable = recordReader.createValue();
        while (recordReader.next(key, writable)) {
          // test timestamp
          if (HiveVersionInfo.getShortVersion().startsWith("3")) {
            LocalDateTime localDateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(testTimestampLong), ZoneOffset.UTC);
            assertEquals(Timestamp.valueOf(localDateTime).toString(), String.valueOf(writable.get()[0]));
          } else {
            Date date = new Date();
            date.setTime(testTimestampLong);
            Timestamp actualTime = ((TimestampWritable) writable.get()[0]).getTimestamp();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            assertEquals(dateFormat.format(date), dateFormat.format(actualTime));
          }
          // test long
          assertEquals(testTimestampLong * 1000, ((LongWritable) writable.get()[1]).get());
          // test date
          assertEquals(LocalDate.ofEpochDay(testDate).toString(), String.valueOf(writable.get()[2]));
        }
      }
    } finally {
      jobConf.set(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true");
    }
  }

  /**
   * A bootstrap split carries two files: the split's own path is the skeleton, inside the table root, and
   * {@code getBootstrapFileSplit()} is the external source file, which is not. A query projecting no columns
   * at all - {@code SELECT COUNT(*)} - satisfies both "only one file is needed" conditions at once, so the
   * order they are tested in decides which file Hive is handed.
   *
   * <p>Handing Hive a path outside the table root breaks its vectorized reader, which derives partition
   * values by looking the split path up in {@code pathToPartitionInfo} (HUDI-5526, #15676). Hive 2.3 ships
   * the same reader but defaults {@code hive.vectorized.execution.enabled} to false where Hive 3 defaults it
   * to true, so this is gated by that config rather than by the Hive version.
   *
   * <p>Only the no-projection case is new behaviour: TestBootstrap and TestOrcBootstrap drive the other
   * three branches end to end, they have just been disabled (HUDI-7353) since #10551.
   */
  @Test
  public void testCountStarReadsSkeletonSoSplitPathStaysInsideTable() throws IOException {
    BootstrapBaseFileSplit split = bootstrapSplit();

    Option<FileSplit> resolved = HoodieParquetInputFormat.resolveSingleFileSplit(split, false, false);

    assertTrue(resolved.isPresent(), "a query projecting no columns must resolve to a single file");
    assertSame(split, resolved.get(),
        "it must be the skeleton, whose path is inside the table root");
  }

  /**
   * The remaining three combinations, which behave the same before and after the reorder: only meta columns
   * needs the skeleton, only data columns needs the external file, and both needs them stitched.
   */
  @ParameterizedTest
  @MethodSource("singleFileSplitCases")
  public void testSingleFileSplitSelection(boolean anyHoodieCol, boolean anyExternalCol,
                                           String expected) throws IOException {
    BootstrapBaseFileSplit split = bootstrapSplit();

    Option<FileSplit> resolved =
        HoodieParquetInputFormat.resolveSingleFileSplit(split, anyHoodieCol, anyExternalCol);

    if ("stitch".equals(expected)) {
      assertFalse(resolved.isPresent(), "both files are needed, so the caller must stitch them");
    } else {
      assertTrue(resolved.isPresent(), "a single file should have been resolved");
      assertSame("skeleton".equals(expected) ? split : split.getBootstrapFileSplit(), resolved.get(),
          "wrong file chosen for (anyHoodieCol=" + anyHoodieCol + ", anyExternalCol=" + anyExternalCol + ")");
    }
  }

  private static Stream<Arguments> singleFileSplitCases() {
    return Stream.of(
        Arguments.of(true, false, "skeleton"),
        Arguments.of(false, true, "external"),
        Arguments.of(true, true, "stitch"));
  }

  private static BootstrapBaseFileSplit bootstrapSplit() throws IOException {
    return new BootstrapBaseFileSplit(
        new FileSplit(new Path("/tbl/event_type=two/skeleton.parquet"), 0, 100, (String[]) null),
        new FileSplit(new Path("/src/event_type=two/part-0.parquet"), 0, 100, (String[]) null));
  }

  /**
   * The end-to-end shape of HUDI-5526: a bootstrap split whose skeleton and external file hold a different
   * number of rows, read through {@link HoodieParquetInputFormat#getRecordReader} with nothing projected.
   * The reader must yield the skeleton's row count. On master it yields the external file's.
   *
   * <p>No Hudi table is needed: {@code shouldUseFilegroupReader} excludes {@code BootstrapBaseFileSplit}, so
   * this falls straight through to {@code createBootstrappingRecordReader}.
   */
  @Test
  public void testNoProjectionReaderReadsSkeletonRowCount() throws Exception {
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(getClass(), "/test_timetype.avsc");
    java.nio.file.Path skeletonFile = basePath.resolve("skeleton.parquet");
    java.nio.file.Path externalFile = basePath.resolve("external.parquet");
    int skeletonRows = 3;
    int externalRows = 7;
    writeParquet(skeletonFile, schema, skeletonRows);
    writeParquet(externalFile, schema, externalRows);

    jobConf.set(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "false");
    jobConf.set(IOConstants.COLUMNS, "test_timestamp,test_long,test_date,_hoodie_commit_time,_hoodie_commit_seqno");
    jobConf.set(IOConstants.COLUMNS_TYPES, "timestamp,bigint,date,string,string");
    // SELECT COUNT(*): Hive projects no columns at all.
    jobConf.set(READ_COLUMN_NAMES_CONF_STR, "");
    jobConf.set(HoodieColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "");

    BootstrapBaseFileSplit split = new BootstrapBaseFileSplit(
        new FileSplit(new Path(skeletonFile.toString()), 0, Files.size(skeletonFile), (String[]) null),
        new FileSplit(new Path(externalFile.toString()), 0, Files.size(externalFile), (String[]) null));

    RecordReader<NullWritable, ArrayWritable> reader = inputFormat.getRecordReader(split, jobConf, null);
    try {
      NullWritable key = reader.createKey();
      ArrayWritable value = reader.createValue();
      int rows = 0;
      while (reader.next(key, value)) {
        rows++;
      }
      assertEquals(skeletonRows, rows,
          "a no-projection read must come from the skeleton file, whose path is inside the table root");
    } finally {
      reader.close();
    }
  }

  private static void writeParquet(java.nio.file.Path file, HoodieSchema schema, int numRows) throws IOException {
    try (AvroParquetWriter parquetWriter =
             new AvroParquetWriter(new Path(file.toString()), schema.toAvroSchema())) {
      for (int i = 0; i < numRows; i++) {
        GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
        record.put("test_timestamp", (long) i);
        record.put("test_long", (long) i);
        record.put("test_date", i);
        record.put("_hoodie_commit_time", "20160628071126");
        record.put("_hoodie_commit_seqno", "20160628071126_" + i);
        parquetWriter.write(record);
      }
    }
  }

}
