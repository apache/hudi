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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.InputFormatTestUtil;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHoodieRealtimeRecordReader {

  private static final String PARTITION_COLUMN = "datestr";
  @Rule
  public TemporaryFolder basePath = new TemporaryFolder();
  private JobConf jobConf;
  private FileSystem fs;
  private Configuration hadoopConf;

  @Before
  public void setUp() {
    jobConf = new JobConf();
    jobConf.set(AbstractRealtimeRecordReader.MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(1024 * 1024));
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath.getRoot().getAbsolutePath(), hadoopConf);
  }

  private Writer writeLogFile(File partitionDir, Schema schema, String fileId, String baseCommit, String
      newCommit,
      int numberOfRecords) throws InterruptedException, IOException {
    return InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, fileId, baseCommit, newCommit,
        numberOfRecords, 0,
        0);
  }

  @Test
  public void testReader() throws Exception {
    testReader(true);
  }

  @Test
  public void testNonPartitionedReader() throws Exception {
    testReader(false);
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

  private void testReader(boolean partitioned) throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = partitioned ? InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 100, baseInstant)
        : InputFormatTestUtil.prepareNonPartitionedParquetTable(basePath, schema, 1, 100, baseInstant);
    InputFormatTestUtil.commit(basePath, baseInstant);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    List<Pair<String, Integer>> logVersionsWithAction = new ArrayList<>();
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 1));
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 2));
    // TODO: HUDI-154 Once Hive 2.x PR (PR-674) is merged, enable this change
    // logVersionsWithAction.add(Pair.of(HoodieTimeline.ROLLBACK_ACTION, 3));
    FileSlice fileSlice =
        new FileSlice(partitioned ? FSUtils.getRelativePartitionPath(new Path(basePath.getRoot().getAbsolutePath()),
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
          writer = InputFormatTestUtil.writeRollback(partitionDir, fs, "fileid0", baseInstant, instantTime,
              String.valueOf(baseInstantTs + logVersion - 1), logVersion);
        } else {
          writer =
              InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid0", baseInstant,
                  instantTime, 100, 0, logVersion);
        }
        long size = writer.getCurrentSize();
        writer.close();
        assertTrue("block - size should be > 0", size > 0);

        // create a split with baseFile (parquet file written earlier) and new log file(s)
        fileSlice.addLogFile(writer.getLogFile());
        HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
            new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + baseInstant + ".parquet"), 0, 1, jobConf),
            basePath.getRoot().getPath(), fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
            .map(h -> h.getPath().toString()).collect(Collectors.toList()),
            instantTime);

        // create a RecordReader to be used by HoodieRealtimeRecordReader
        RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
            new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), jobConf, null);
        JobConf jobConf = new JobConf();
        List<Schema.Field> fields = schema.getFields();
        setHiveColumnNameProps(fields, jobConf, partitioned);

        // validate record reader compaction
        HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

        // use reader to read base Parquet File and log file, merge in flight and return latest commit
        // here all 100 records should be updated, see above
        NullWritable key = recordReader.createKey();
        ArrayWritable value = recordReader.createValue();
        while (recordReader.next(key, value)) {
          Writable[] values = value.get();
          // check if the record written is with latest commit, here "101"
          assertEquals(latestInstant, values[0].toString());
          key = recordReader.createKey();
          value = recordReader.createValue();
        }
        recordReader.getPos();
        assertEquals(1.0, recordReader.getProgress(), 0.05);
        recordReader.close();
      } catch (Exception ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    });

    // Add Rollback last version to next log-file

  }

  @Test
  public void testUnMergedReader() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    final int numRecords = 1000;
    final int firstBatchLastRecordKey = numRecords - 1;
    final int secondBatchLastRecordKey = 2 * numRecords - 1;
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, numRecords, instantTime);
    InputFormatTestUtil.commit(basePath, instantTime);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    // insert new records to log file
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid0", instantTime, newCommitTime,
            numRecords, numRecords, 0);
    long size = writer.getCurrentSize();
    writer.close();
    assertTrue("block - size should be > 0", size > 0);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    String logFilePath = writer.getLogFile().getPath().toString();
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + instantTime + ".parquet"), 0, 1, jobConf),
        basePath.getRoot().getPath(), Collections.singletonList(logFilePath), newCommitTime);

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), jobConf, null);
    JobConf jobConf = new JobConf();
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
        Assert.assertTrue(gotKey > firstBatchLastRecordKey);
        Assert.assertTrue(gotKey <= secondBatchLastRecordKey);
        assertEquals(gotKey, lastSeenKeyFromLog + 1);
        lastSeenKeyFromLog++;
      } else {
        numRecordsAtCommit1++;
        Assert.assertTrue(gotKey >= 0);
        Assert.assertTrue(gotKey <= firstBatchLastRecordKey);
      }
      // Ensure unique key
      Assert.assertFalse(seenKeys.contains(gotKey));
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

  @Test
  public void testReaderWithNestedAndComplexSchema() throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getComplexEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    int numberOfRecords = 100;
    int numberOfLogRecords = numberOfRecords / 2;
    File partitionDir = InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, numberOfRecords, instantTime);
    InputFormatTestUtil.commit(basePath, instantTime);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    // update files or generate new log file
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        writeLogFile(partitionDir, schema, "fileid0", instantTime, newCommitTime, numberOfLogRecords);
    long size = writer.getCurrentSize();
    writer.close();
    assertTrue("block - size should be > 0", size > 0);
    InputFormatTestUtil.deltaCommit(basePath, newCommitTime);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    String logFilePath = writer.getLogFile().getPath().toString();
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + instantTime + ".parquet"), 0, 1, jobConf),
        basePath.getRoot().getPath(), Collections.singletonList(logFilePath), newCommitTime);

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), jobConf, null);
    JobConf jobConf = new JobConf();
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, jobConf, true);

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
      assertEquals("test value for field: field1", values[5].toString(), "field" + currentRecordNo);
      assertEquals("test value for field: field2", values[6].toString(),
          "field" + currentRecordNo + recordCommitTimeSuffix);
      assertEquals("test value for field: name", values[7].toString(), "name" + currentRecordNo);

      // Assert type INT
      IntWritable intWritable = (IntWritable) values[8];
      assertEquals("test value for field: favoriteIntNumber", intWritable.get(),
          currentRecordNo + recordCommitTime.hashCode());

      // Assert type LONG
      LongWritable longWritable = (LongWritable) values[9];
      assertEquals("test value for field: favoriteNumber", longWritable.get(),
          currentRecordNo + recordCommitTime.hashCode());

      // Assert type FLOAT
      FloatWritable floatWritable = (FloatWritable) values[10];
      assertEquals("test value for field: favoriteFloatNumber", floatWritable.get(),
          (float) ((currentRecordNo + recordCommitTime.hashCode()) / 1024.0), 0);

      // Assert type DOUBLE
      DoubleWritable doubleWritable = (DoubleWritable) values[11];
      assertEquals("test value for field: favoriteDoubleNumber", doubleWritable.get(),
          (currentRecordNo + recordCommitTime.hashCode()) / 1024.0, 0);

      // Assert type MAP
      ArrayWritable mapItem = (ArrayWritable) values[12];
      Writable mapItemValue1 = mapItem.get()[0];
      Writable mapItemValue2 = mapItem.get()[1];

      assertEquals("test value for field: tags", ((ArrayWritable) mapItemValue1).get()[0].toString(),
          "mapItem1");
      assertEquals("test value for field: tags", ((ArrayWritable) mapItemValue2).get()[0].toString(),
          "mapItem2");
      assertEquals("test value for field: tags", ((ArrayWritable) mapItemValue1).get().length, 2);
      assertEquals("test value for field: tags", ((ArrayWritable) mapItemValue2).get().length, 2);
      Writable mapItemValue1value = ((ArrayWritable) mapItemValue1).get()[1];
      Writable mapItemValue2value = ((ArrayWritable) mapItemValue2).get()[1];
      assertEquals("test value for field: tags[\"mapItem1\"].item1",
          ((ArrayWritable) mapItemValue1value).get()[0].toString(), "item" + currentRecordNo);
      assertEquals("test value for field: tags[\"mapItem2\"].item1",
          ((ArrayWritable) mapItemValue2value).get()[0].toString(), "item2" + currentRecordNo);
      assertEquals("test value for field: tags[\"mapItem1\"].item2",
          ((ArrayWritable) mapItemValue1value).get()[1].toString(), "item" + currentRecordNo + recordCommitTimeSuffix);
      assertEquals("test value for field: tags[\"mapItem2\"].item2",
          ((ArrayWritable) mapItemValue2value).get()[1].toString(), "item2" + currentRecordNo + recordCommitTimeSuffix);

      // Assert type RECORD
      ArrayWritable recordItem = (ArrayWritable) values[13];
      Writable[] nestedRecord = recordItem.get();
      assertFalse("test value for field: testNestedRecord.isAdmin", ((BooleanWritable) nestedRecord[0]).get());
      assertEquals("test value for field: testNestedRecord.userId", nestedRecord[1].toString(),
          "UserId" + currentRecordNo + recordCommitTimeSuffix);

      // Assert type ARRAY
      ArrayWritable arrayValue = (ArrayWritable) values[14];
      Writable[] arrayValues = arrayValue.get();
      for (int i = 0; i < arrayValues.length; i++) {
        assertEquals("test value for field: stringArray", "stringArray" + i + recordCommitTimeSuffix,
            arrayValues[i].toString());
      }
    }
  }

  @Test
  public void testSchemaEvolutionAndRollbackBlockInLastLogFile() throws Exception {
    // initial commit
    List<String> logFilePaths = new ArrayList<>();
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    HoodieTestUtils.init(hadoopConf, basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    String instantTime = "100";
    int numberOfRecords = 100;
    int numberOfLogRecords = numberOfRecords / 2;
    File partitionDir =
        InputFormatTestUtil.prepareSimpleParquetTable(basePath, schema, 1, numberOfRecords, instantTime);
    InputFormatTestUtil.commit(basePath, instantTime);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    List<Field> firstSchemaFields = schema.getFields();

    // update files and generate new log file but don't commit
    schema = SchemaTestUtil.getComplexEvolvedSchema();
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid0", instantTime, newCommitTime,
            numberOfLogRecords, 0, 1);
    long size = writer.getCurrentSize();
    logFilePaths.add(writer.getLogFile().getPath().toString());
    writer.close();
    assertTrue("block - size should be > 0", size > 0);

    // write rollback for the previous block in new log file version
    newCommitTime = "102";
    writer = InputFormatTestUtil.writeRollbackBlockToLogFile(partitionDir, fs, schema, "fileid0", instantTime,
        newCommitTime, "101", 1);
    logFilePaths.add(writer.getLogFile().getPath().toString());
    writer.close();
    InputFormatTestUtil.deltaCommit(basePath, newCommitTime);

    // create a split with baseFile (parquet file written earlier) and new log file(s)
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(partitionDir + "/fileid0_1_" + instantTime + ".parquet"), 0, 1, jobConf),
        basePath.getRoot().getPath(), logFilePaths, newCommitTime);

    // create a RecordReader to be used by HoodieRealtimeRecordReader
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), jobConf, null);
    JobConf jobConf = new JobConf();
    List<Schema.Field> fields = schema.getFields();

    assertFalse(firstSchemaFields.containsAll(fields));

    // Try to read all the fields passed by the new schema
    setHiveColumnNameProps(fields, jobConf, true);

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
  }
}
