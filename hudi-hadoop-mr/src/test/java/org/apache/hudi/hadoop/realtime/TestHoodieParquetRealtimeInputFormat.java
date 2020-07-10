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

package org.apache.hudi.hadoop.realtime;

import static org.apache.hudi.common.testutils.HoodieTestUtils.generateFakeHoodieWriteStats;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultHadoopConf;
import static org.apache.hudi.common.testutils.HoodieTestUtils.init;
import static org.apache.hudi.common.testutils.SchemaTestUtil.generateAvroRecordFromJson;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getEvolvedSchema;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHoodieParquetRealtimeInputFormat {

  private HoodieParquetRealtimeInputFormat inputFormat;
  private JobConf jobConf;
  private Configuration hadoopConf;
  private FileSystem fs;
  private Schema schema;
  int numberOfRecords = 150;
  int numRecordsPerBaseFile = numberOfRecords / 2;
  int numberOfUniqueLogRecords = numberOfRecords / 6;
  int numberOfFiles = 1;
  String fileID = "fileid0";
  String partitionPath = "2016/05/01";
  String baseInstant1 = "100";
  String deltaCommitTime1 = "101";
  String deltaCommitTime2 = "102";
  String deltaCommitTime3 = "103";
  String baseInstant2 = "200";
  String deltaCommitTime4 = "201";
  String deltaCommitTime5 = "202";

  @TempDir
  public java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() throws IOException {
    inputFormat = new HoodieParquetRealtimeInputFormat();
    jobConf = new JobConf();
    jobConf.set(MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(1024 * 1024));
    hadoopConf = getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath.toString(), hadoopConf);
    schema = HoodieAvroUtils.addMetadataFields(getEvolvedSchema());
    setPropsForInputFormat(inputFormat, jobConf, schema);
  }

  private static void setPropsForInputFormat(HoodieParquetRealtimeInputFormat inputFormat, JobConf jobConf,
                                             Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(Schema.Field::name).collect(Collectors.joining(","));
    String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    Configuration conf = getDefaultHadoopConf();

    String hiveColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";

    String hiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes("string,string,string,bigint,string,string");
    hiveColumnTypes = hiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
    inputFormat.setConf(conf);
    jobConf.addResource(conf);
  }

  private void prepareTestData() throws IOException, InterruptedException {
    // [inserts][initial commit] => creates one parquet file
    init(hadoopConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, numberOfFiles, numRecordsPerBaseFile, baseInstant1);
    createCommitFile(basePath, baseInstant1, partitionPath);

    moreUpdates(partitionDir, baseInstant1, deltaCommitTime1, 0);

    moreUpdates(partitionDir, baseInstant1, deltaCommitTime2, 1 * numberOfUniqueLogRecords);

    moreUpdates(partitionDir, baseInstant1, deltaCommitTime3, 2 * numberOfUniqueLogRecords);

    InputFormatTestUtil.prepareParquetTable(basePath, schema, numberOfFiles, numRecordsPerBaseFile, baseInstant2);
    createCommitFile(basePath, baseInstant2, partitionPath);

    moreUpdates(partitionDir, baseInstant2, deltaCommitTime4, 0);

    moreUpdates(partitionDir, baseInstant2, deltaCommitTime5, 1 * numberOfUniqueLogRecords);

    // Add the paths to jobConf
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
  }

  // moreUpdates creates a new log file on top of latest FileSlice and publishes a new deltacommit
  private void moreUpdates(File partitionDir, String baseCommit, String deltaCommit, int offset) throws IOException, InterruptedException {
    HoodieLogFormat.Writer writer = writeLogFileWithUniqueUpdates(partitionDir, schema, fileID, baseCommit, deltaCommit,
            numberOfUniqueLogRecords, offset);
    long size = writer.getCurrentSize();
    writer.close();
    assertTrue(size > 0, "block - size should be > 0");
    createDeltaCommitFile(basePath, deltaCommit, partitionPath, writer);
  }

  private HoodieLogFormat.Writer writeLogFileWithUniqueUpdates(
          File partitionDir,
          Schema schema,
          String fileId,
          String baseCommit,
          String newCommit,
          int numberOfRecords,
          int offset) throws InterruptedException, IOException {
    return writeDataBlockToLogFile(partitionDir, schema, fileId, baseCommit, newCommit, numberOfRecords, offset, 0);
  }

  private HoodieLogFormat.Writer writeDataBlockToLogFile(File partitionDir, Schema schema, String fileId,
                                                         String baseCommit, String newCommit, int numberOfRecords, int offset, int logVersion)
          throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(partitionDir.getPath()))
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId).withLogVersion(logVersion)
            .withLogWriteToken("1-0-1").overBaseCommit(baseCommit).withFs(fs).build();
    List<IndexedRecord> records = new ArrayList<>();
    for (int i = offset; i < offset + numberOfRecords; i++) {
      records.add(generateAvroRecordFromJson(schema, i, newCommit, fileID));
    }
    Schema writeSchema = records.get(0).getSchema();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    return writer;
  }

  private void createCommitFile(java.nio.file.Path basePath, String commitNumber, String relativePartitionPath)
          throws IOException {
    String dataFileName = FSUtils.makeDataFileName(commitNumber, InputFormatTestUtil.TEST_WRITE_TOKEN, fileID);
    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", "01"));
    Path fullPath = new Path(partitionPath.resolve(dataFileName).toString());
    String relFilePath =  fullPath.toString().replaceFirst(basePath.toString() + "/", "");
    HoodieWriteStat writeStat = HoodieTestUtils.generateFakeHoodieWriteStat(relFilePath, relativePartitionPath);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat(relativePartitionPath, writeStat);
    java.nio.file.Path file = Files.createFile(basePath.resolve(Paths.get(".hoodie", commitNumber + ".commit")));
    FileOutputStream fileOutputStream = new FileOutputStream(file.toAbsolutePath().toString());
    fileOutputStream.write(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private void createDeltaCommitFile(java.nio.file.Path basePath, String commitNumber, String partitionPath, HoodieLogFormat.Writer writer)
          throws IOException {
    HoodieWriteStat writeStat = generateFakeHoodieWriteStats(basePath, writer.getLogFile());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat(partitionPath, writeStat);
    java.nio.file.Path file = Files.createFile(basePath.resolve(Paths.get(".hoodie/", commitNumber + ".deltacommit")));
    FileOutputStream fileOutputStream = new FileOutputStream(file.toAbsolutePath().toString());
    fileOutputStream.write(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private void testIncrementalQuery(String msg, String endCommitTime, Option<String> beginCommitTime,
                                    int expectedNumberOfRecordsInCommit) throws IOException {
    int actualCount = 0;
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    assertEquals(1, splits.length);
    for (InputSplit split : splits) {
      RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(split, jobConf, null);
      NullWritable key = recordReader.createKey();
      ArrayWritable writable = recordReader.createValue();

      while (recordReader.next(key, writable)) {
        // writable returns an array with schema like org.apache.hudi.common.util.TestRecord.java
        // Take the commit time and compare with the one we are interested in
        Writable[] values = writable.get();
        String commitTime = values[HOODIE_COMMIT_TIME_COL_POS].toString();
        boolean endCommitTimeMatches = HoodieTimeline.compareTimestamps(endCommitTime, HoodieTimeline.GREATER_THAN_OR_EQUALS, commitTime);
        boolean beginCommitTimeMatches = !beginCommitTime.isPresent() || HoodieTimeline.compareTimestamps(beginCommitTime.get(), HoodieTimeline.LESSER_THAN, commitTime);
        assertTrue(beginCommitTimeMatches && endCommitTimeMatches);
        actualCount++;
      }
    }
    assertEquals(expectedNumberOfRecordsInCommit, actualCount, msg);
  }

  private void ensureRecordsInCommitRange(InputSplit split, String beginCommitTime, String endCommitTime, int totalExpected)
          throws IOException {
    int actualCount = 0;
    assertTrue(split instanceof HoodieRealtimeFileSplit);
    RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(split, jobConf, null);
    NullWritable key = recordReader.createKey();
    ArrayWritable writable = recordReader.createValue();

    while (recordReader.next(key, writable)) {
      // writable returns an array with schema like org.apache.hudi.common.util.TestRecord.java
      // Take the commit time and compare with the one we are interested in
      Writable[] values = writable.get();
      String commitTime = values[HOODIE_COMMIT_TIME_COL_POS].toString();

      // The begin and end commits are inclusive
      boolean endCommitTimeMatches = HoodieTimeline.compareTimestamps(endCommitTime, HoodieTimeline.GREATER_THAN_OR_EQUALS, commitTime);
      boolean beginCommitTimeMatches = HoodieTimeline.compareTimestamps(beginCommitTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, commitTime);
      assertTrue(beginCommitTimeMatches && endCommitTimeMatches);
      actualCount++;
    }
    assertEquals(totalExpected, actualCount, "Expected num of records(" + totalExpected + ") do not match "
            + "actual number of records(" + actualCount + ") for the split: " + ((HoodieRealtimeFileSplit)split).toString());
  }

  @Test // Simple querying on MOR table. Does not test incremental querying.
  public void testInputFormatLoadAndUpdate() throws IOException, InterruptedException {
    prepareTestData();

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 1);
    assertEquals(1, inputSplits.length);

    FileStatus[]  files = inputFormat.listStatus(jobConf);
    assertEquals(1, files.length);

    /*
     * Since the begin and end commits are included the expected number of records is same as numRecordsPerBaseFile
     * as the records in log files are really updates for existing keys and are merged by the RecordReader
     */
    ensureRecordsInCommitRange(inputSplits[0], baseInstant2, deltaCommitTime5, numRecordsPerBaseFile);
  }

  @Test
  public void testIncrementalWithNoNewCommits() throws IOException, InterruptedException {
    prepareTestData();
    InputFormatTestUtil.setupIncremental(jobConf, deltaCommitTime5, 1);
    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(0, files.length, "We should exclude commit 202 when returning incremental pull with start commit time as 202");
  }

  @Test
  public void testChangesSinceLastCommit() throws IOException, InterruptedException {
    prepareTestData();

    // query for latest changes since (deltaCommitTime4,] (meaning > deltaCommitTime4 and <= deltaCommitTime5)
    InputFormatTestUtil.setupIncremental(jobConf, deltaCommitTime4, 1);

    /*
    * Since the begin commit is not included in incremental query, here the expected output is the num of records that
    * were updated in the one log file belonging to deltaCommitTime5
    */
    testIncrementalQuery("Number of actual records from Incremental querying does not match expected: " + numberOfUniqueLogRecords,
            deltaCommitTime5, Option.of(deltaCommitTime4), numberOfUniqueLogRecords);
  }

  @Test
  public void testChangesHappenedInTimePeriod() throws IOException, InterruptedException {
    prepareTestData();

    // query for changes in the period (baseInstant1,deltaCommitTime2] (meaning > baseInstant1 and <= deltaCommitTime2)
    InputFormatTestUtil.setupIncremental(jobConf, baseInstant1, 2);
    /*
    * Since the begin commit is not included in incremental query, here the expected output is the num of records that
    * were updated in the two log files belonging to deltaCommitTime1 and deltaCommitTime2. And we set up test data
    * such that the log records belong to unique record keys. This might not be true in real world
    */
    testIncrementalQuery("Number of actual records from Incremental querying does not match expected: " + numberOfUniqueLogRecords * 2,
            deltaCommitTime2, Option.of(baseInstant1), numberOfUniqueLogRecords * 2);

    // query for changes in the period (deltaCommitTime1,deltaCommitTime4] meaning > deltaCommitTime1 and <= deltaCommitTime4
    InputFormatTestUtil.setupIncremental(jobConf, deltaCommitTime1, 4);
    /*
     * Here the expected output is the num of records that matches the filter in first and second file slices. From
     * first file slice records updated as part of deltaCommitTime2 and deltaCommitTime3 are considered
     * (this would be 2 * 25 = 50 unique updates). From second file slice base file (75 records) and updates belonging to
     * deltaCommitTime4 (25 updates to the baseInstant 2) would be considered. In total expected match would be 75 since
     * the base file is the superset of all unique record keys and others are all updates.
     */
    testIncrementalQuery("Number of actual records from Incremental querying does not match expected: " + numRecordsPerBaseFile,
            deltaCommitTime4, Option.of(deltaCommitTime1), numRecordsPerBaseFile);
  }
}
