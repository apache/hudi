/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED;
import static org.apache.hudi.common.config.HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY;
import static org.apache.hudi.config.HoodieWriteConfig.RECORD_MERGER_IMPLS;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_RECORD_POSITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeHandleWithSparkMerger extends SparkClientFunctionalTestHarness {
  private static final Schema SCHEMA = getAvroSchema("AvroSchema", "AvroSchemaNS");
  private HoodieTableMetaClient metaClient;

  public static String getPartitionPath() {
    return "2023-10-01";
  }

  @BeforeEach
  public void setUp() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(
        HoodieTableConfig.BASE_FILE_FORMAT.key(),
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    properties.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "record_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(),"partition_path");
    properties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    metaClient = getHoodieMetaClient(storageConf(), basePath(), HoodieTableType.MERGE_ON_READ, properties);
  }

  @Test
  public void testDefaultMerger() throws Exception {
    HoodieWriteConfig writeConfig = buildDefaultWriteConfig(SCHEMA);
    HoodieRecordMerger merger = writeConfig.getRecordMerger();
    assertTrue(merger instanceof DefaultMerger);
    assertTrue(writeConfig.getBooleanOrDefault(FILE_GROUP_READER_ENABLED.key(), false));
    insertAndUpdate(writeConfig, 114);
  }

  @Test
  public void testNoFlushMerger() throws Exception {
    HoodieWriteConfig writeConfig = buildNoFlushWriteConfig(SCHEMA);
    HoodieRecordMerger merger = writeConfig.getRecordMerger();
    assertTrue(merger instanceof NoFlushMerger);
    assertTrue(writeConfig.getBooleanOrDefault(FILE_GROUP_READER_ENABLED.key(), false));
    insertAndUpdate(writeConfig, 64);
  }

  @Test
  public void testCustomMerger() throws Exception {
    HoodieWriteConfig writeConfig = buildCustomWriteConfig(SCHEMA);
    HoodieRecordMerger merger = writeConfig.getRecordMerger();
    assertTrue(merger instanceof CustomMerger);
    assertTrue(writeConfig.getBooleanOrDefault(FILE_GROUP_READER_ENABLED.key(), false));
    insertAndUpdate(writeConfig, 95);
  }

  public List<HoodieRecord> generateRecords(int numOfRecords, String commitTime) throws Exception {
    Dataset<Row> rows = SparkDatasetTestUtils.getRandomRowsWithCommitTime(
        new SQLContext(jsc()), numOfRecords, getPartitionPath(), false, commitTime);
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(rows, SparkDatasetTestUtils.ENCODER);
    return internalRows.stream()
        .map(r -> new HoodieSparkRecord(new HoodieKey(r.getString(2), r.getString(3)),
            r,
            SparkDatasetTestUtils.STRUCT_TYPE,
            false)).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateRecordUpdates(List<HoodieKey> keys, String commitTime) throws Exception {
    Dataset<Row> rows = SparkDatasetTestUtils.getRandomRowsWithKeys(
        new SQLContext(jsc()), keys, false, commitTime);
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(rows, SparkDatasetTestUtils.ENCODER);
    return internalRows.stream()
        .map(r -> new HoodieSparkRecord(new HoodieKey(r.getString(2), r.getString(3)),
            r,
            SparkDatasetTestUtils.STRUCT_TYPE,
            false)).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateEmptyRecords(List<HoodieKey> keys) {
    List<HoodieRecord> records = new ArrayList<>();
    for (HoodieKey key : keys) {
      records.add(new HoodieEmptyRecord(key, HoodieOperation.DELETE, 1, HoodieRecord.HoodieRecordType.SPARK));
    }
    return records;
  }

  public static List<HoodieKey> getKeys(List<HoodieRecord> records) {
    return records.stream().map(r -> r.getKey()).collect(Collectors.toList());
  }

  private static Schema getAvroSchema(String schemaName, String schemaNameSpace) {
    return AvroConversionUtils.convertStructTypeToAvroSchema(SparkDatasetTestUtils.STRUCT_TYPE, schemaName, schemaNameSpace);
  }

  public HoodieWriteConfig getWriteConfig(Schema avroSchema) {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(
        RECORD_MERGER_IMPLS.key(),
        "org.apache.hudi.DefaultSparkRecordMerger");
    extraProperties.setProperty(
        LOGFILE_DATA_BLOCK_FORMAT.key(),
        "parquet");
    extraProperties.setProperty(
        HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "record_key");
    extraProperties.setProperty(
        FILE_GROUP_READER_ENABLED.key(),
        "true");
    extraProperties.setProperty(
        WRITE_RECORD_POSITIONS.key(),
        "true");
    extraProperties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(),"partition_path");

    return getConfigBuilder(true)
        .withPath(basePath())
        .withSchema(avroSchema.toString())
        .withProperties(extraProperties)
        .build();
  }

  public DefaultWriteConfig buildDefaultWriteConfig(Schema avroSchema) {
    HoodieWriteConfig config = getWriteConfig(avroSchema);
    return new DefaultWriteConfig(config);
  }

  public NoFlushWriteConfig buildNoFlushWriteConfig(Schema avroSchema) {
    HoodieWriteConfig config = getWriteConfig(avroSchema);
    return new NoFlushWriteConfig(config);
  }

  public CustomWriteConfig buildCustomWriteConfig(Schema avroSchema) {
    HoodieWriteConfig config = getWriteConfig(avroSchema);
    return new CustomWriteConfig(config);
  }

  public HoodieTableFileSystemView getFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
  }

  public List<FileSlice> getLatestFileSlices(String partitionPath) {
    return getFileSystemView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
  }

  public Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId) {
    return getFileSystemView().getLatestFileSlice(partitionPath, fileId);
  }

  public Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId) {
    return getLatestFileSlice(partitionPath, fileId).map(fs -> fs.getBaseFile().get());
  }

  public List<HoodieLogFile> getLatestLogFiles(String partitionPath, String fileId) {
    Option<FileSlice> fileSliceOpt = getLatestFileSlice(partitionPath, fileId);
    if (fileSliceOpt.isPresent()) {
      return fileSliceOpt.get().getLogFiles().collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public List<String> getFileIds(String partitionPath) {
    List<FileSlice> fileSlices = getLatestFileSlices(partitionPath);
    return fileSlices.stream().map(fs -> fs.getFileId()).collect(Collectors.toList());
  }

  public void checkDataEquality(int numRecords) {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        RECORD_MERGER_IMPLS.key(),
        "org.apache.hudi.DefaultSparkRecordMerger");
    properties.put(
        LOGFILE_DATA_BLOCK_FORMAT.key(),
        "parquet");
    properties.put(
        PAYLOAD_ORDERING_FIELD_PROP_KEY,
        HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName());
    properties.put(
        FILE_GROUP_READER_ENABLED.key(),
        "true");
    properties.put(
        WRITE_RECORD_POSITIONS.key(),
        "true");
    Dataset<Row> rows = spark()
        .read()
        .options(properties)
        .format("org.apache.hudi")
        .load(basePath());
    List<Row> result = rows.collectAsList();
    assertEquals(numRecords, result.size());
  }

  public void insertAndUpdate(HoodieWriteConfig writeConfig, int expectedRecordNum) throws Exception {
    // Check if the table type is correct.
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(writeConfig, context(), reloadedMetaClient);
    assertEquals(hoodieTable.getMetaClient().getTableType(), HoodieTableType.MERGE_ON_READ);

    // Write and read.
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {

      // (1) Write: insert.
      String instantTime = "001";
      writeClient.startCommitWithTime(instantTime);
      List<HoodieRecord> records = generateRecords(100, instantTime);
      Stream<HoodieBaseFile> baseFileStream = insertRecordsToMORTable(reloadedMetaClient, records, writeClient, writeConfig, instantTime);
      assertTrue(baseFileStream.findAny().isPresent());

      // Check metadata files.
      Option<HoodieInstant> deltaCommit = reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals(instantTime, deltaCommit.get().getTimestamp(), "Delta commit should be specified value");

      // Check data files.
      List<String> fileIds = getFileIds(getPartitionPath());
      assertEquals(1, fileIds.size());

      Option<HoodieBaseFile> baseFileOption = getLatestBaseFile(getPartitionPath(), fileIds.get(0));
      assertTrue(baseFileOption.isPresent());

      List<HoodieLogFile> logFiles = getLatestLogFiles(getPartitionPath(), fileIds.get(0));
      assertTrue(logFiles.isEmpty());
      checkDataEquality(100);

      // (2) Write: append.
      instantTime = "002";
      writeClient.startCommitWithTime(instantTime);

      List<HoodieRecord> records2 = generateEmptyRecords(getKeys(records).subList(0, 17)); // 17 records with old keys.
      List<HoodieRecord> records3 = generateRecordUpdates(getKeys(records).subList(17, 36), "001"); // 19 update records.
      List<HoodieRecord> records4 = generateRecords(31, instantTime); // 31 new records.
      records2.addAll(records3);
      records2.addAll(records4);
      assertEquals(67, records2.size());
      updateRecordsInMORTable(reloadedMetaClient, records2, writeClient, writeConfig, instantTime, false);

      // Check metadata files.
      deltaCommit = reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());

      // Check data files.
      List<String> fileIds2 = getFileIds(getPartitionPath());
      assertFalse(fileIds2.isEmpty());
      // One partition one file group.
      assertEquals(1, fileIds2.size());

      baseFileOption = getLatestBaseFile(getPartitionPath(), fileIds2.get(0));
      assertTrue(baseFileOption.isPresent());

      // Check data after
      checkDataEquality(expectedRecordNum);

      // (3) Write: append, generate the log file.
      instantTime = "003";
      writeClient.startCommitWithTime(instantTime);

      List<HoodieRecord> records5 = generateEmptyRecords(getKeys(records).subList(50, 59)); // 9 deletes only
      assertEquals(9, records5.size());
      updateRecordsInMORTable(reloadedMetaClient, records5, writeClient, writeConfig, instantTime, false);
      checkDataEquality(expectedRecordNum - 9);
    }
  }

  public static class TestHoodieWriteConfig extends HoodieWriteConfig {
    TestHoodieWriteConfig(HoodieWriteConfig writeConfig) {
      super(writeConfig.getEngineType(), writeConfig.getProps());
    }
  }

  public static class DefaultWriteConfig extends TestHoodieWriteConfig {
    DefaultWriteConfig(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public HoodieRecordMerger getRecordMerger() {
      return new DefaultMerger();
    }
  }

  public static class NoFlushWriteConfig extends TestHoodieWriteConfig {
    NoFlushWriteConfig(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public HoodieRecordMerger getRecordMerger() {
      return new NoFlushMerger();
    }
  }

  public static class CustomWriteConfig extends TestHoodieWriteConfig {
    CustomWriteConfig(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public HoodieRecordMerger getRecordMerger() {
      return new CustomMerger();
    }
  }

  public static class DefaultMerger extends DefaultSparkRecordMerger {
    @Override
    public boolean shouldFlush(HoodieRecord record, Schema schema, TypedProperties props) {
      return true;
    }
  }

  public static class NoFlushMerger extends DefaultSparkRecordMerger {
    @Override
    public boolean shouldFlush(HoodieRecord record, Schema schema, TypedProperties props) {
      return false;
    }
  }

  public static class CustomMerger extends DefaultSparkRecordMerger {
    @Override
    public boolean shouldFlush(HoodieRecord record, Schema schema, TypedProperties props) throws IOException {
      return !((HoodieSparkRecord) record).getData().getString(0).equals("001");
    }
  }
}
