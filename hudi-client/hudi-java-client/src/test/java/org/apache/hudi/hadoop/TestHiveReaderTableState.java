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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem.Call;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests how Hive record readers resolve the table they read.
 */
class TestHiveReaderTableState extends HoodieJavaClientTestHarness {

  private static final String PARTITION_COLUMN = "datestr";
  private static final HoodieSchema SCHEMA = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA));

  /**
   * Record readers read their splits without touching the table's .hoodie folder: the table state, the table schema
   * and the latest commit come with the split.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testReadSplitsWithoutMetaFolderAccess(HoodieTableType tableType) throws Exception {
    HoodieWriteConfig config = initTable(tableType);
    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 20);
    write(config, insertTime, inserts, true);
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    write(config, updateTime, dataGen.generateUpdates(updateTime, inserts.subList(0, 10)), false);

    JobConf jobConf = newJobConf();
    RecordingLocalFileSystem.register(jobConf);
    Map<String, String> riders = read(tableType, jobConf, true);

    assertTrue(RecordingLocalFileSystem.count(Call.inScope()) > 0, "The readers must go through the recording file system");
    assertEquals(0, RecordingLocalFileSystem.count(Call.inScope().and(Call.underMetaFolder())),
        () -> RecordingLocalFileSystem.describe(Call.inScope().and(Call.underMetaFolder())));
    assertEquals(20, riders.size());
    assertEquals(10, riders.values().stream().filter(rider -> rider.equals("rider-" + updateTime)).count());
    assertEquals(10, riders.values().stream().filter(rider -> rider.equals("rider-" + insertTime)).count());
  }

  /**
   * Read options of the job, such as a merge mode, do not override the table config persisted with the table.
   */
  @Test
  void testReadOptionsDoNotOverrideTableConfig() throws Exception {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    HoodieWriteConfig config = initTable(tableType);
    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 10);
    write(config, insertTime, inserts, true);
    // Updates with a lower ordering value than the inserts lose under the persisted event time ordering.
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    write(config, updateTime, dataGen.generateUpdatesWithTimestamp(updateTime, inserts, 0L), false);

    JobConf jobConf = newJobConf();
    jobConf.set(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.COMMIT_TIME_ORDERING.name());
    Map<String, String> riders = read(tableType, jobConf, false);

    assertEquals(10, riders.size());
    assertTrue(riders.values().stream().allMatch(rider -> rider.equals("rider-" + insertTime)), riders.toString());
  }

  /**
   * A commit still inflight when the splits are listed is not the latest commit the splits are read as of.
   */
  @Test
  void testSplitsAreReadAsOfLatestCompletedCommit() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    HoodieWriteConfig config = initTable(tableType);
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    write(config, commitTime, dataGen.generateInserts(commitTime, 10), true);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String inflightTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, inflightTime);
    client.insert(dataGen.generateInserts(inflightTime, 5), inflightTime);

    JobConf jobConf = newJobConf();
    InputSplit[] splits = listSplits(newInputFormat(tableType, jobConf), jobConf);
    assertTrue(splits.length > 0);
    for (InputSplit split : splits) {
      assertEquals(commitTime, HiveReaderTableState.of(split).get().getLatestCommitTime());
    }
    assertEquals(10, read(tableType, jobConf, false).size());
  }

  /**
   * On a merge-on-read table before version 8, a log block of a delta commit that is not completed is skipped when it
   * is older than the latest commit, and a block of a completed one is read, through the committed instants the split
   * carries.
   */
  @Test
  void testLogBlocksOfTableVersionSixAreCheckedAgainstShippedCommittedInstants() throws Exception {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    HoodieWriteConfig config = initTable(tableType, Option.of(HoodieTableVersion.SIX));
    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 10);
    write(config, insertTime, inserts, true);
    String committedUpdateTime = WriteClientTestUtils.createNewInstantTime();
    write(config, committedUpdateTime, dataGen.generateUpdates(committedUpdateTime, inserts.subList(0, 5)), false);
    String failedUpdateTime = WriteClientTestUtils.createNewInstantTime();
    write(config, failedUpdateTime, dataGen.generateUpdates(failedUpdateTime, inserts.subList(0, 5)), false);
    String latestTime = WriteClientTestUtils.createNewInstantTime();
    write(config, latestTime, dataGen.generateUpdates(latestTime, inserts.subList(5, 10)), false);
    revertToInflight(failedUpdateTime);
    assertEquals(HoodieTableVersion.SIX, HoodieTestUtils.createMetaClient(storageConf, basePath).getTableConfig().getTableVersion());

    JobConf jobConf = newJobConf();
    jobConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    RecordingLocalFileSystem.register(jobConf);
    Map<String, String> riders = read(tableType, jobConf, true);

    assertTrue(RecordingLocalFileSystem.count(Call.inScope()) > 0, "The readers must go through the recording file system");
    assertEquals(0, RecordingLocalFileSystem.count(Call.inScope().and(Call.underMetaFolder())),
        () -> RecordingLocalFileSystem.describe(Call.inScope().and(Call.underMetaFolder())));
    Map<String, String> expected = new HashMap<>();
    inserts.subList(0, 5).forEach(record -> expected.put(record.getRecordKey(), "rider-" + committedUpdateTime));
    inserts.subList(5, 10).forEach(record -> expected.put(record.getRecordKey(), "rider-" + latestTime));
    assertEquals(expected, riders);
  }

  private HoodieWriteConfig initTable(HoodieTableType tableType) throws IOException {
    return initTable(tableType, Option.empty());
  }

  private HoodieWriteConfig initTable(HoodieTableType tableType, Option<HoodieTableVersion> tableVersion) throws IOException {
    storage.deleteDirectory(new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
    Properties props = new Properties();
    tableVersion.ifPresent(version -> props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(version.versionCode())));
    props.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    props.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    metaClient = HoodieTestUtils.init(storageConf, basePath, tableType, props);
    Map<String, String> writeProps = new HashMap<>();
    writeProps.put(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    HoodieWriteConfig.Builder builder = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING)
        .withProps(writeProps);
    tableVersion.ifPresent(version -> builder.withWriteTableVersion(version.versionCode())
        .withAutoUpgradeVersion(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build()));
    return builder.build();
  }

  /**
   * Makes a completed commit look like a failed write, by deleting its completed instant file.
   */
  private void revertToInflight(String instantTime) throws IOException {
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    List<StoragePath> completed = storage.listDirectEntries(metaPath).stream()
        .map(StoragePathInfo::getPath)
        .filter(path -> path.getName().startsWith(instantTime) && path.getName().endsWith(HoodieTimeline.DELTA_COMMIT_EXTENSION))
        .collect(Collectors.toList());
    assertEquals(1, completed.size(), "completed instant files of " + instantTime);
    storage.deleteFile(completed.get(0));
  }

  private void write(HoodieWriteConfig config, String instantTime, List<HoodieRecord> records, boolean insert) {
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<WriteStatus> statuses = insert ? client.insert(records, instantTime) : client.upsert(records, instantTime);
    client.commit(instantTime, statuses);
  }

  private JobConf newJobConf() {
    JobConf jobConf = new JobConf(storageConf.unwrapAs(org.apache.hadoop.conf.Configuration.class));
    List<HoodieSchemaField> fields = SCHEMA.getFields();
    String names = fields.stream().map(HoodieSchemaField::name).collect(Collectors.joining(","));
    String hiveColumnNames = fields.stream().map(HoodieSchemaField::name)
        .filter(name -> !name.equalsIgnoreCase(PARTITION_COLUMN)).collect(Collectors.joining(",")) + "," + PARTITION_COLUMN;
    String hiveColumnTypes = HoodieSchemaUtils.addMetadataColumnTypes(TRIP_HIVE_COLUMN_TYPES) + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
    jobConf.set(IOConstants.COLUMNS, hiveColumnNames);
    jobConf.set(IOConstants.COLUMNS_TYPES, hiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
        fields.stream().map(field -> String.valueOf(field.pos())).collect(Collectors.joining(",")));
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, PARTITION_COLUMN);
    return jobConf;
  }

  /**
   * Lists the splits of the table and reads them one after the other, returning the rider of every record key. With
   * {@code recordTaskAccesses}, only the record readers count as tasks.
   */
  private Map<String, String> read(HoodieTableType tableType, JobConf jobConf, boolean recordTaskAccesses) throws Exception {
    HoodieParquetInputFormat inputFormat = newInputFormat(tableType, jobConf);
    RecordingLocalFileSystem.reset();
    InputSplit[] splits = listSplits(inputFormat, jobConf);

    int keyPos = SCHEMA.getField("_row_key").get().pos();
    int riderPos = SCHEMA.getField("rider").get().pos();
    Map<String, String> riders = new HashMap<>();
    try (RecordingLocalFileSystem.Scope ignored = RecordingLocalFileSystem.withScope(() -> recordTaskAccesses)) {
      for (InputSplit split : splits) {
        RecordReader<NullWritable, ArrayWritable> reader = inputFormat.getRecordReader(shipped(split), jobConf, null);
        NullWritable key = reader.createKey();
        ArrayWritable value = reader.createValue();
        while (reader.next(key, value)) {
          Writable[] values = value.get();
          riders.put(values[keyPos].toString(), values[riderPos].toString());
        }
        reader.close();
      }
    }
    return riders;
  }

  private static HoodieParquetInputFormat newInputFormat(HoodieTableType tableType, JobConf jobConf) {
    HoodieParquetInputFormat inputFormat = tableType == HoodieTableType.MERGE_ON_READ
        ? new HoodieParquetRealtimeInputFormat() : new HoodieParquetInputFormat();
    inputFormat.setConf(jobConf);
    return inputFormat;
  }

  private InputSplit[] listSplits(HoodieParquetInputFormat inputFormat, JobConf jobConf) throws IOException {
    FileInputFormat.setInputPaths(jobConf, Arrays.stream(dataGen.getPartitionPaths())
        .map(partition -> new StoragePath(basePath, partition).toString()).collect(Collectors.joining(",")));
    return inputFormat.getSplits(jobConf, 1);
  }

  /**
   * Serializes and deserializes the split the way it travels to a task.
   */
  private static InputSplit shipped(InputSplit split) throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    split.write(out);
    Constructor<? extends InputSplit> constructor = split.getClass().getDeclaredConstructor();
    constructor.setAccessible(true);
    InputSplit copy = constructor.newInstance();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(out.getData(), 0, out.getLength())));
    return copy;
  }
}
