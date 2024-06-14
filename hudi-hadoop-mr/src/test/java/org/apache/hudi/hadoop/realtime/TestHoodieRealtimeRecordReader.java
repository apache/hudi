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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.RealtimeFileStatus;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestHoodieRealtimeRecordReader {

  private static final String PARTITION_COLUMN = "datestr";
  private JobConf baseJobConf;
  private HoodieStorage storage;
  private FileSystem fs;
  private StorageConfiguration<Configuration> storageConf;

  @BeforeEach
  public void setUp() {
    storageConf = HoodieTestUtils.getDefaultStorageConf();
    storageConf.set("fs.defaultFS", "file:///");
    storageConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    baseJobConf = new JobConf(storageConf.unwrap());
    baseJobConf.set(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(1024 * 1024));
    fs = HadoopFSUtils.getFs(basePath.toUri().toString(), baseJobConf);
    storage = new HoodieHadoopStorage(fs);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(basePath.toString()), true);
      fs.close();
    }
    if (baseJobConf != null) {
      baseJobConf.clear();
    }
  }

  @TempDir
  public java.nio.file.Path basePath;

  private Writer writeLogFile(File partitionDir, Schema schema, String fileId, String baseCommit, String newCommit,
                              int numberOfRecords) throws InterruptedException, IOException {
    return InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, fileId,
        baseCommit, newCommit,
        numberOfRecords, 0,
        0);
  }

  private void setHiveColumnNameProps(List<Schema.Field> fields, JobConf jobConf, boolean isPartitioned) {
    String names = fields.stream().map(Field::name).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);

    String hiveOrderedColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase(PARTITION_COLUMN))
        .map(Field::name).collect(Collectors.joining(","));
    if (isPartitioned) {
      hiveOrderedColumnNames += "," + PARTITION_COLUMN;
      jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, PARTITION_COLUMN);
    }
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveOrderedColumnNames);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReader(ExternalSpillableMap.DiskMapType diskMapType,
                         boolean isCompressionEnabled,
                         boolean partitioned) throws Exception {
    testReaderInternal(diskMapType, isCompressionEnabled, partitioned);
  }

  @Test
  public void testHFileInlineReader() throws Exception {
    testReaderInternal(ExternalSpillableMap.DiskMapType.BITCASK, false, false,
        HoodieLogBlock.HoodieLogBlockType.HFILE_DATA_BLOCK);
  }

  @Test
  public void testParquetInlineReader() throws Exception {
    testReaderInternal(ExternalSpillableMap.DiskMapType.BITCASK, false, false,
        HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK);
  }

  private void testReaderInternal(ExternalSpillableMap.DiskMapType diskMapType,
                                  boolean isCompressionEnabled,
                                  boolean partitioned) throws Exception {
    testReaderInternal(diskMapType, isCompressionEnabled, partitioned, HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);
  }

  private void testReaderInternal(ExternalSpillableMap.DiskMapType diskMapType,
                                  boolean isCompressionEnabled,
                                  boolean partitioned, HoodieLogBlock.HoodieLogBlockType logBlockType) throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = partitioned ? InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ)
        : InputFormatTestUtil.prepareNonPartitionedParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ);

    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.DELTA_COMMIT_ACTION);
    FileCreateUtils.createDeltaCommit(basePath.toString(), baseInstant, commitMetadata);
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    List<Pair<String, Integer>> logVersionsWithAction = new ArrayList<>();
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 1));
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 2));
    // TODO: HUDI-154 Once Hive 2.x PR (PR-674) is merged, enable this change
    // logVersionsWithAction.add(Pair.of(HoodieTimeline.ROLLBACK_ACTION, 3));
    FileSlice fileSlice =
        new FileSlice(partitioned ? HadoopFSUtils.getRelativePartitionPath(new Path(basePath.toString()),
            new Path(partitionDir.getAbsolutePath())) : "default", baseInstant, "fileid0");
    logVersionsWithAction.forEach(logVersionWithAction -> {
      try {
        // update files or generate new log file
        int logVersion = logVersionWithAction.getRight();
        String action = logVersionWithAction.getKey();
        int baseInstantTs = Integer.parseInt(baseInstant);
        String instantTime = String.valueOf(baseInstantTs + logVersion);
        String latestInstant =
            action.equals(HoodieTimeline.ROLLBACK_ACTION) ? String.valueOf(baseInstantTs + logVersion - 2)
                : instantTime;

        HoodieLogFormat.Writer writer;
        if (action.equals(HoodieTimeline.ROLLBACK_ACTION)) {
          writer = InputFormatTestUtil.writeRollback(partitionDir, storage, "fileid0", baseInstant,
              instantTime,
              String.valueOf(baseInstantTs + logVersion - 1), logVersion);
        } else {
          writer =
              InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid0",
                  baseInstant,
                  instantTime, 120, 0, logVersion, logBlockType);
        }
        long size = writer.getCurrentSize();
        writer.close();
        assertTrue(size > 0, "block - size should be > 0");
        FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);

        // create a split with baseFile (parquet file written earlier) and new log file(s)
        fileSlice.addLogFile(writer.getLogFile());
        HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
            new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + baseInstant + ".parquet"), 0, 1, baseJobConf),
            basePath.toUri().toString(), fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
            .collect(Collectors.toList()),
            instantTime,
            false,
            Option.empty());

        // create a RecordReader to be used by HoodieRealtimeRecordReader
        RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
            new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
        JobConf jobConf = new JobConf(baseJobConf);
        List<Schema.Field> fields = schema.getFields();
        setHiveColumnNameProps(fields, jobConf, partitioned);

        jobConf.setEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
        jobConf.setBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), isCompressionEnabled);

        // validate record reader compaction
        long logTmpFileStartTime = System.currentTimeMillis();
        HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

        // use reader to read base Parquet File and log file, merge in flight and return latest commit
        // here all 100 records should be updated, see above
        // another 20 new insert records should also output with new commit time.
        NullWritable key = recordReader.createKey();
        ArrayWritable value = recordReader.createValue();
        int recordCnt = 0;
        while (recordReader.next(key, value)) {
          Writable[] values = value.get();
          // check if the record written is with latest commit, here "101"
          assertEquals(latestInstant, values[0].toString());
          key = recordReader.createKey();
          value = recordReader.createValue();
          recordCnt++;
        }
        recordReader.getPos();
        assertEquals(1.0, recordReader.getProgress(), 0.05);
        assertEquals(120, recordCnt);
        recordReader.close();
        // the temp file produced by logScanner should be deleted
        assertTrue(!getLogTempFile(logTmpFileStartTime, System.currentTimeMillis(), diskMapType.toString()).exists());
      } catch (Exception ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    });

    // Add Rollback last version to next log-file

  }

  private File getLogTempFile(long startTime, long endTime, String diskType) {
    return Arrays.stream(new File("/tmp").listFiles())
        .filter(f -> f.isDirectory() && f.getName().startsWith("hudi-" + diskType) && f.lastModified() > startTime && f.lastModified() < endTime)
        .findFirst()
        .orElseGet(() -> new File(""));
  }

  @Test
  public void testUnMergedReader() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    final int numRecords = 1000;
    final int firstBatchLastRecordKey = numRecords - 1;
    final int secondBatchLastRecordKey = 2 * numRecords - 1;
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, numRecords, instantTime,
        HoodieTableType.MERGE_ON_READ);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.DELTA_COMMIT_ACTION);
    FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    // insert new records to log file
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid0",
            instantTime, newCommitTime,
            numRecords, numRecords, 0);
    long size = writer.getCurrentSize();
    writer.close();
    assertTrue(size > 0, "block - size should be > 0");
    FileCreateUtils.createDeltaCommit(basePath.toString(), newCommitTime, commitMetadata);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + instantTime + ".parquet"), 0, 1, baseJobConf),
        basePath.toUri().toString(), Collections.singletonList(writer.getLogFile()), newCommitTime, false, Option.empty());

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
    JobConf jobConf = new JobConf(baseJobConf);
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, jobConf, true);
    // Enable merge skipping.
    jobConf.set(REALTIME_SKIP_MERGE_PROP, "true");

    // validate unmerged record reader
    RealtimeUnmergedRecordReader recordReader = new RealtimeUnmergedRecordReader(split, jobConf, reader);

    // use reader to read base Parquet File and log file
    // here all records should be present. Also ensure log records are in order.
    NullWritable key = recordReader.createKey();
    ArrayWritable value = recordReader.createValue();
    int numRecordsAtCommit1 = 0;
    int numRecordsAtCommit2 = 0;
    Set<Integer> seenKeys = new HashSet<>();
    int lastSeenKeyFromLog = firstBatchLastRecordKey;
    while (recordReader.next(key, value)) {
      Writable[] values = value.get();
      String gotCommit = values[0].toString();
      String keyStr = values[2].toString();
      int gotKey = Integer.parseInt(keyStr.substring("key".length()));
      if (gotCommit.equals(newCommitTime)) {
        numRecordsAtCommit2++;
        assertTrue(gotKey > firstBatchLastRecordKey);
        assertTrue(gotKey <= secondBatchLastRecordKey);
        assertEquals(gotKey, lastSeenKeyFromLog + 1);
        lastSeenKeyFromLog++;
      } else {
        numRecordsAtCommit1++;
        assertTrue(gotKey >= 0);
        assertTrue(gotKey <= firstBatchLastRecordKey);
      }
      // Ensure unique key
      assertFalse(seenKeys.contains(gotKey));
      seenKeys.add(gotKey);
      key = recordReader.createKey();
      value = recordReader.createValue();
    }
    assertEquals(numRecords, numRecordsAtCommit1);
    assertEquals(numRecords, numRecordsAtCommit2);
    assertEquals(2 * numRecords, seenKeys.size());
    assertEquals(1.0, recordReader.getProgress(), 0.05);
    recordReader.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReaderWithNestedAndComplexSchema(ExternalSpillableMap.DiskMapType diskMapType,
                                                   boolean isCompressionEnabled) throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getComplexEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    int numberOfRecords = 100;
    int numberOfLogRecords = numberOfRecords / 2;
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, numberOfRecords,
        instantTime, HoodieTableType.MERGE_ON_READ);

    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(basePath.toString(), instantTime, Option.of(commitMetadata));
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    // update files or generate new log file
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        writeLogFile(partitionDir, schema, "fileid0", instantTime, newCommitTime, numberOfLogRecords);
    long size = writer.getCurrentSize();
    writer.close();
    assertTrue(size > 0, "block - size should be > 0");
    commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.DELTA_COMMIT_ACTION);
    FileCreateUtils.createDeltaCommit(basePath.toString(), newCommitTime, commitMetadata);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + instantTime + ".parquet"), 0, 1, baseJobConf),
        basePath.toUri().toString(), Collections.singletonList(writer.getLogFile()), newCommitTime, false, Option.empty());

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
    JobConf jobConf = new JobConf(baseJobConf);
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, jobConf, true);

    jobConf.setEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
    jobConf.setBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), isCompressionEnabled);

    // validate record reader compaction
    HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

    // use reader to read base Parquet File and log file, merge in flight and return latest commit
    // here the first 50 records should be updated, see above
    NullWritable key = recordReader.createKey();
    ArrayWritable value = recordReader.createValue();
    int numRecordsRead = 0;
    while (recordReader.next(key, value)) {
      int currentRecordNo = numRecordsRead;
      ++numRecordsRead;
      Writable[] values = value.get();
      String recordCommitTime;
      // check if the record written is with latest commit, here "101"
      if (numRecordsRead > numberOfLogRecords) {
        recordCommitTime = instantTime;
      } else {
        recordCommitTime = newCommitTime;
      }
      String recordCommitTimeSuffix = "@" + recordCommitTime;

      assertEquals(values[0].toString(), recordCommitTime);
      key = recordReader.createKey();
      value = recordReader.createValue();

      // Assert type STRING
      assertEquals(values[5].toString(), "field" + currentRecordNo, "test value for field: field1");
      assertEquals(values[6].toString(), "field" + currentRecordNo + recordCommitTimeSuffix,
          "test value for field: field2");
      assertEquals(values[7].toString(), "name" + currentRecordNo,
          "test value for field: name");

      // Assert type INT
      IntWritable intWritable = (IntWritable) values[8];
      assertEquals(intWritable.get(), currentRecordNo + recordCommitTime.hashCode(),
          "test value for field: favoriteIntNumber");

      // Assert type LONG
      LongWritable longWritable = (LongWritable) values[9];
      assertEquals(longWritable.get(), currentRecordNo + recordCommitTime.hashCode(),
          "test value for field: favoriteNumber");

      // Assert type FLOAT
      FloatWritable floatWritable = (FloatWritable) values[10];
      assertEquals(floatWritable.get(), (float) ((currentRecordNo + recordCommitTime.hashCode()) / 1024.0), 0,
          "test value for field: favoriteFloatNumber");

      // Assert type DOUBLE
      DoubleWritable doubleWritable = (DoubleWritable) values[11];
      assertEquals(doubleWritable.get(), (currentRecordNo + recordCommitTime.hashCode()) / 1024.0, 0,
          "test value for field: favoriteDoubleNumber");

      // Assert type MAP
      ArrayWritable mapItem = (ArrayWritable) values[12];
      Writable mapItemValue1 = mapItem.get()[0];
      Writable mapItemValue2 = mapItem.get()[1];

      assertEquals(((ArrayWritable) mapItemValue1).get()[0].toString(), "mapItem1",
          "test value for field: tags");
      assertEquals(((ArrayWritable) mapItemValue2).get()[0].toString(), "mapItem2",
          "test value for field: tags");
      assertEquals(((ArrayWritable) mapItemValue1).get().length, 2,
          "test value for field: tags");
      assertEquals(((ArrayWritable) mapItemValue2).get().length, 2,
          "test value for field: tags");
      Writable mapItemValue1value = ((ArrayWritable) mapItemValue1).get()[1];
      Writable mapItemValue2value = ((ArrayWritable) mapItemValue2).get()[1];
      assertEquals(((ArrayWritable) mapItemValue1value).get()[0].toString(), "item" + currentRecordNo,
          "test value for field: tags[\"mapItem1\"].item1");
      assertEquals(((ArrayWritable) mapItemValue2value).get()[0].toString(), "item2" + currentRecordNo,
          "test value for field: tags[\"mapItem2\"].item1");
      assertEquals(((ArrayWritable) mapItemValue1value).get()[1].toString(), "item" + currentRecordNo + recordCommitTimeSuffix,
          "test value for field: tags[\"mapItem1\"].item2");
      assertEquals(((ArrayWritable) mapItemValue2value).get()[1].toString(), "item2" + currentRecordNo + recordCommitTimeSuffix,
          "test value for field: tags[\"mapItem2\"].item2");

      // Assert type RECORD
      ArrayWritable recordItem = (ArrayWritable) values[13];
      Writable[] nestedRecord = recordItem.get();
      assertFalse(((BooleanWritable) nestedRecord[0]).get(), "test value for field: testNestedRecord.isAdmin");
      assertEquals(nestedRecord[1].toString(), "UserId" + currentRecordNo + recordCommitTimeSuffix,
          "test value for field: testNestedRecord.userId");

      // Assert type ARRAY
      ArrayWritable arrayValue = (ArrayWritable) values[14];
      Writable[] arrayValues = arrayValue.get();
      for (int i = 0; i < arrayValues.length; i++) {
        assertEquals("stringArray" + i + recordCommitTimeSuffix, arrayValues[i].toString(),
            "test value for field: stringArray");
      }
      reader.close();
    }
    recordReader.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testSchemaEvolutionAndRollbackBlockInLastLogFile(ExternalSpillableMap.DiskMapType diskMapType,
                                                               boolean isCompressionEnabled) throws Exception {
    // initial commit
    List<HoodieLogFile> logFiles = new ArrayList<>();
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    int numberOfRecords = 100;
    int numberOfLogRecords = numberOfRecords / 2;
    File partitionDir =
        InputFormatTestUtil.prepareSimpleParquetTable(basePath, schema, 1, numberOfRecords,
            instantTime, HoodieTableType.MERGE_ON_READ);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(basePath.toString(), instantTime, Option.of(commitMetadata));
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());
    List<Field> firstSchemaFields = schema.getFields();

    // update files and generate new log file but don't commit
    schema = SchemaTestUtil.getComplexEvolvedSchema();
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid0",
            instantTime, newCommitTime,
            numberOfLogRecords, 0, 1);
    long size = writer.getCurrentSize();
    logFiles.add(writer.getLogFile());
    writer.close();
    assertTrue(size > 0, "block - size should be > 0");

    // write rollback for the previous block in new log file version
    newCommitTime = "102";
    writer =
        InputFormatTestUtil.writeRollbackBlockToLogFile(partitionDir, storage, schema, "fileid0",
            instantTime,
            newCommitTime, "101", 1);
    logFiles.add(writer.getLogFile());
    writer.close();

    commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
            WriteOperationType.UPSERT,
            schema.toString(), HoodieTimeline.DELTA_COMMIT_ACTION);
    FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1_" + instantTime + ".parquet"), 0, 1,
            baseJobConf),
        basePath.toUri().toString(), logFiles, newCommitTime, false, Option.empty());

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
    JobConf jobConf = new JobConf(baseJobConf);
    List<Schema.Field> fields = schema.getFields();

    assertFalse(firstSchemaFields.containsAll(fields));

    // Try to read all the fields passed by the new schema
    setHiveColumnNameProps(fields, jobConf, true);

    jobConf.setEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
    jobConf.setBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), isCompressionEnabled);

    HoodieRealtimeRecordReader recordReader;
    try {
      // validate record reader compaction
      recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);
      throw new RuntimeException("should've failed the previous line");
    } catch (HoodieException e) {
      // expected, field not found since the data written with the evolved schema was rolled back
    }

    // Try to read all the fields passed by the new schema
    setHiveColumnNameProps(firstSchemaFields, jobConf, true);
    // This time read only the fields which are part of parquet
    recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);
    // use reader to read base Parquet File and log file
    NullWritable key = recordReader.createKey();
    ArrayWritable value = recordReader.createValue();
    while (recordReader.next(key, value)) {
      // keep reading
    }
    recordReader.close();
    reader.close();
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.BITCASK;
    boolean isCompressionEnabled = true;
    // initial commit
    List<HoodieLogFile> logFiles = new ArrayList<>();
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    int numberOfRecords = 100;
    int numberOfLogRecords = numberOfRecords / 2;
    File partitionDir =
        InputFormatTestUtil.prepareSimpleParquetTable(basePath, schema, 1, numberOfRecords,
            instantTime, HoodieTableType.MERGE_ON_READ);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(basePath.toString(), instantTime, Option.of(commitMetadata));
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());
    List<Field> firstSchemaFields = schema.getFields();

    // 2nd commit w/ evolved schema
    Schema evolvedSchema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedCompatibleSchema());
    List<Field> secondSchemaFields = evolvedSchema.getFields();
    String newCommitTime = "101";
    File partitionDir1 =
        InputFormatTestUtil.prepareSimpleParquetTable(basePath, evolvedSchema, 1, numberOfRecords,
            instantTime, HoodieTableType.MERGE_ON_READ, "2017", "05", "01");
    HoodieCommitMetadata commitMetadata1 = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        evolvedSchema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(basePath.toString(), newCommitTime, Option.of(commitMetadata1));
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir1.getPath());

    // create a split with baseFile from 1st commit.
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1_" + instantTime + ".parquet"), 0, 1, baseJobConf),
        basePath.toUri().toString(), logFiles, newCommitTime, false, Option.empty());

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
    JobConf jobConf = new JobConf(baseJobConf);

    // Try to read all the fields passed by the new schema
    setHiveColumnNameProps(secondSchemaFields, jobConf, true);
    // This time read only the fields which are part of parquet
    HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);
    // use reader to read base Parquet File and log file
    NullWritable key = recordReader.createKey();
    ArrayWritable value = recordReader.createValue();
    while (recordReader.next(key, value)) {
      // keep reading
    }
    recordReader.close();
    reader.close();
  }

  private static Stream<Arguments> testArguments() {
    // Arg1: ExternalSpillableMap Type, Arg2: isDiskMapCompressionEnabled, Arg3: partitioned
    return Stream.of(
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false, true),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true, true)
    );
  }

  @Test
  public void testIncrementalWithOnlylog() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    final int numRecords = 1000;
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, numRecords, instantTime,
        HoodieTableType.MERGE_ON_READ);
    createDeltaCommitFile(basePath, instantTime, "2016/05/01", "2016/05/01/fileid0_1-0-1_100.parquet", "fileid0", schema.toString());
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    // insert new records to log file
    try {
      String newCommitTime = "102";
      HoodieLogFormat.Writer writer =
          InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid0",
              instantTime, newCommitTime,
              numRecords, numRecords, 0);
      writer.close();
      createDeltaCommitFile(basePath, newCommitTime, "2016/05/01", "2016/05/01/.fileid0_100.log.1_1-0-1", "fileid0", schema.toString());

      InputFormatTestUtil.setupIncremental(baseJobConf, "101", 1);

      HoodieParquetRealtimeInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
      inputFormat.setConf(baseJobConf);
      InputSplit[] splits = inputFormat.getSplits(baseJobConf, 1);
      assertEquals(1, splits.length);
      JobConf newJobConf = new JobConf(baseJobConf);
      List<Schema.Field> fields = schema.getFields();
      setHiveColumnNameProps(fields, newJobConf, false);
      newJobConf.set("columns.types", "string,string,string,string,string,string,string,string,bigint,string,string");
      RecordReader<NullWritable, ArrayWritable> reader = inputFormat.getRecordReader(splits[0], newJobConf, Reporter.NULL);
      // use reader to read log file.
      NullWritable key = reader.createKey();
      ArrayWritable value = reader.createValue();
      while (reader.next(key, value)) {
        Writable[] values = value.get();
        // since we set incremental start commit as 101 and commit_number as 1.
        // the data belong to commit 102 should be read out.
        assertEquals(newCommitTime, values[0].toString());
        key = reader.createKey();
        value = reader.createValue();
      }
      reader.close();
    } catch (IOException e) {
      throw new HoodieException(e.getMessage(), e);
    }
  }

  @Test
  public void testIncrementalWithReplace() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ);
    createDeltaCommitFile(basePath, baseInstant, "2016/05/01", "2016/05/01/fileid0_1-0-1_100.parquet", "fileid0", schema.toString());
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    InputFormatTestUtil.simulateInserts(partitionDir, ".parquet", "fileid1", 1, "200");
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileId = new ArrayList<>();
    replacedFileId.add("fileid0");
    partitionToReplaceFileIds.put("2016/05/01", replacedFileId);
    createReplaceCommitFile(basePath,
        "200", "2016/05/01", "2016/05/01/fileid10_1-0-1_200.parquet", "fileid10", partitionToReplaceFileIds);

    InputFormatTestUtil.setupIncremental(baseJobConf, "0", 1);

    HoodieParquetRealtimeInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(baseJobConf);
    InputSplit[] splits = inputFormat.getSplits(baseJobConf, 1);
    assertTrue(splits.length == 1);
    JobConf newJobConf = new JobConf(baseJobConf);
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, newJobConf, false);
    newJobConf.set("columns.types", "string,string,string,string,string,string,string,string,bigint,string,string");
    RecordReader<NullWritable, ArrayWritable> reader = inputFormat.getRecordReader(splits[0], newJobConf, Reporter.NULL);

    // use reader to read log file.
    NullWritable key = reader.createKey();
    ArrayWritable value = reader.createValue();
    while (reader.next(key, value)) {
      Writable[] values = value.get();
      // since we set incremental start commit as 0 and commit_number as 1.
      // the data belong to commit 100 should be read out.
      assertEquals("100", values[0].toString());
      key = reader.createKey();
      value = reader.createValue();
    }
    reader.close();
  }

  private void createReplaceCommitFile(
      java.nio.file.Path basePath,
      String commitNumber,
      String partitionPath,
      String filePath,
      String fileId,
      Map<String, List<String>> partitionToReplaceFileIds) throws IOException {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    HoodieWriteStat writeStat = createHoodieWriteStat(basePath, commitNumber, partitionPath, filePath, fileId);
    writeStats.add(writeStat);
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    writeStats.forEach(stat -> replaceMetadata.addWriteStat(partitionPath, stat));
    File file = basePath.resolve(".hoodie").resolve(commitNumber + ".replacecommit").toFile();
    file.createNewFile();
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    fileOutputStream.write(getUTF8Bytes(replaceMetadata.toJsonString()));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private HoodieWriteStat createHoodieWriteStat(java.nio.file.Path basePath, String commitNumber, String partitionPath, String filePath, String fileId) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setNumDeletes(0);
    writeStat.setNumUpdateWrites(100);
    writeStat.setNumWrites(100);
    writeStat.setPath(filePath);
    writeStat.setFileSizeInBytes(new File(new Path(basePath.toString(), filePath).toString()).length());
    writeStat.setPartitionPath(partitionPath);
    writeStat.setTotalLogFilesCompacted(100L);
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(100);
    runtimeStats.setTotalCreateTime(100);
    runtimeStats.setTotalUpsertTime(100);
    writeStat.setRuntimeStats(runtimeStats);
    return writeStat;
  }

  private void createDeltaCommitFile(
      java.nio.file.Path basePath,
      String commitNumber,
      String partitionPath,
      String filePath,
      String fileId,
      String schemaStr) throws IOException {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    HoodieWriteStat writeStat = createHoodieWriteStat(basePath, commitNumber, partitionPath, filePath, fileId);
    writeStats.add(writeStat);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    writeStats.forEach(stat -> commitMetadata.addWriteStat(partitionPath, stat));
    if (schemaStr != null) {
      commitMetadata.getExtraMetadata().put(HoodieCommitMetadata.SCHEMA_KEY, schemaStr);
    }
    File file = basePath.resolve(".hoodie").resolve(commitNumber + ".deltacommit").toFile();
    file.createNewFile();
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    fileOutputStream.write(getUTF8Bytes(commitMetadata.toJsonString()));
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  @Test
  public void testLogOnlyReader() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    URI baseUri = basePath.toUri();
    HoodieTestUtils.init(storageConf, baseUri.toString(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = InputFormatTestUtil.prepareNonPartitionedParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ);
    FileCreateUtils.createDeltaCommit(basePath.toString(), baseInstant);
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.toURI().toString());

    FileSlice fileSlice = new FileSlice("default", baseInstant, "fileid1");
    try {
      // update files or generate new log file
      int logVersion = 1;
      int baseInstantTs = Integer.parseInt(baseInstant);
      String instantTime = String.valueOf(baseInstantTs + logVersion);
      HoodieLogFormat.Writer writer =
          InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid1",
              baseInstant,
              instantTime, 100, 0, logVersion);
      long size = writer.getCurrentSize();
      writer.close();
      assertTrue(size > 0, "block - size should be > 0");
      HoodieCommitMetadata commitMetadata =
          CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
              WriteOperationType.UPSERT,
              schema.toString(), HoodieTimeline.COMMIT_ACTION);
      FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);
      // create a split with new log file(s)
      fileSlice.addLogFile(new HoodieLogFile(writer.getLogFile().getPath(), size));
      RealtimeFileStatus realtimeFileStatus = new RealtimeFileStatus(
          new FileStatus(writer.getLogFile().getFileSize(), false, 1, 1, 0,
              new Path(writer.getLogFile().getPath().toUri())),
          baseUri.toString(),
          fileSlice.getLogFiles().collect(Collectors.toList()),
          false,
          Option.empty());
      realtimeFileStatus.setMaxCommitTime(instantTime);
      HoodieRealtimePath realtimePath = (HoodieRealtimePath) realtimeFileStatus.getPath();
      HoodieRealtimeFileSplit split =
          new HoodieRealtimeFileSplit(new FileSplit(realtimePath, 0, 0, new String[] {""}), realtimePath);

      JobConf newJobConf = new JobConf(baseJobConf);
      List<Schema.Field> fields = schema.getFields();
      setHiveColumnNameProps(fields, newJobConf, false);
      // create a dummy RecordReader to be used by HoodieRealtimeRecordReader
      RecordReader<NullWritable, ArrayWritable> reader = new HoodieRealtimeRecordReader(split, newJobConf, new HoodieEmptyRecordReader(split, newJobConf));
      // use reader to read log file.
      NullWritable key = reader.createKey();
      ArrayWritable value = reader.createValue();
      while (reader.next(key, value)) {
        Writable[] values = value.get();
        assertEquals(instantTime, values[0].toString());
        key = reader.createKey();
        value = reader.createValue();
      }
      reader.close();
    } catch (Exception e) {
      throw new HoodieException(e.getMessage(), e);
    }
  }

  @Test
  public void testRealtimeInputFormatEmptyFileSplit() throws Exception {
    // Add the empty paths
    String emptyPath = ClassLoader.getSystemResource("emptyFile").getPath();
    FileInputFormat.setInputPaths(baseJobConf, emptyPath);

    HoodieParquetRealtimeInputFormat inputFormat =  new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(baseJobConf);

    InputSplit[] inputSplits = inputFormat.getSplits(baseJobConf, 10);
    assertEquals(1, inputSplits.length);
    assertEquals(true, inputSplits[0] instanceof RealtimeSplit);

    FileStatus[] files = inputFormat.listStatus(baseJobConf);
    assertEquals(1, files.length);
    assertEquals(true, files[0] instanceof RealtimeFileStatus);
  }

  @Test
  public void testIncrementalWithCompaction() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(storageConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ);
    createDeltaCommitFile(basePath, baseInstant, "2016/05/01", "2016/05/01/fileid0_1-0-1_100.parquet", "fileid0", schema.toString());
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    createCompactionFile(basePath, "125");

    // add inserts after compaction timestamp
    InputFormatTestUtil.simulateInserts(partitionDir, ".parquet", "fileId2", 5, "200");
    InputFormatTestUtil.commit(basePath, "200");

    InputFormatTestUtil.setupIncremental(baseJobConf, "100", 10);

    // verify that incremental reads do NOT show inserts after compaction timestamp
    HoodieParquetRealtimeInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(baseJobConf);
    InputSplit[] splits = inputFormat.getSplits(baseJobConf, 1);
    assertTrue(splits.length == 0);
  }

  @Test
  public void testAvroToArrayWritable() throws IOException {
    Schema schema = SchemaTestUtil.getEvolvedSchema();
    GenericRecord record = SchemaTestUtil.generateAvroRecordFromJson(schema, 1, "100", "100", false);
    ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, schema);
    assertEquals(schema.getFields().size(), aWritable.get().length);

    // In some queries, generic records that Hudi gets are just part of the full records.
    // Here test the case that some fields are missing in the record.
    Schema schemaWithMetaFields = HoodieAvroUtils.addMetadataFields(schema);
    ArrayWritable aWritable2 = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, schemaWithMetaFields);
    assertEquals(schemaWithMetaFields.getFields().size(), aWritable2.get().length);
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
}
