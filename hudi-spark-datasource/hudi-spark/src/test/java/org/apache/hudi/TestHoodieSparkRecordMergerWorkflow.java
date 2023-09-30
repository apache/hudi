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

import org.apache.avro.Schema;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.realtime.HoodieMergeOnReadSnapshotReader;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSparkRecordMergerWorkflow extends SparkClientFunctionalTestHarness {
  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieSparkRecordMergerWorkflow.class);
  private static final String FILE = "/Users/linliu/projects/hudi/log/info.txt";
  private BufferedWriter writer;

  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(
        HoodieTableConfig.BASE_FILE_FORMAT.key(),
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    properties.setProperty(
        "hoodie.payload.ordering.field",
        "_hoodie_record_key");
    metaClient = getHoodieMetaClient(hadoopConf(), basePath(), HoodieTableType.MERGE_ON_READ, properties);
    writer = new BufferedWriter(new FileWriter(FILE, true));
  }

  public static String getPartitionPath() {
    return "2023-09-25";
  }

  public List<HoodieRecord> generateRecords(int numOfRecords) throws Exception {
    Dataset<Row> rows = SparkDatasetTestUtils.getRandomRows(new SQLContext(jsc()), numOfRecords, getPartitionPath(), false);
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(rows, SparkDatasetTestUtils.ENCODER);
    return internalRows.stream()
        .map(r -> new HoodieSparkRecord(new HoodieKey(r.getString(2), r.getString(3)),
            r,
            SparkDatasetTestUtils.STRUCT_TYPE,
            false)).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateRecordUpdates(List<HoodieKey> keys) throws Exception {
    Dataset<Row> rows = SparkDatasetTestUtils.getRandomRowsWithKeys(new SQLContext(jsc()), keys, false);
    List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(rows, SparkDatasetTestUtils.ENCODER);
    return internalRows.stream()
        .map(r -> new HoodieSparkRecord(new HoodieKey(r.getString(2), r.getString(3)),
            r,
            SparkDatasetTestUtils.STRUCT_TYPE,
            false)).collect(Collectors.toList());
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
        "hoodie.datasource.write.record.merger.impls",
        "org.apache.hudi.HoodieSparkRecordMerger");
    extraProperties.setProperty(
        "hoodie.logfile.data.block.format",
        "parquet");
    extraProperties.setProperty(
        "hoodie.payload.ordering.field",
        "_hoodie_record_key");
    extraProperties.setProperty(
        "hoodie.datasource.write.row.writer.enable",
        "true");

    return getConfigBuilder(true)
        .withPath(basePath())
        .withSchema(avroSchema.toString())
        .withProperties(extraProperties)
        .build();
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

  public Option<String> getOneFileId(String partitionPath) {
    List<FileSlice> fileSlices = getLatestFileSlices(partitionPath);
    if (fileSlices.isEmpty()) {
      return Option.empty();
    }
    return Option.of(fileSlices.get(0).getFileId());
  }

  public void checkDataIntegrityUsingSnapshotReader(
      String partitionPath, String fileId, String latestInstantTime, Schema schema, JobConf jobConf, List<HoodieRecord> records) throws IOException {
    Option<FileSlice> fileSliceOption = getLatestFileSlice(partitionPath, fileId);
    assertTrue(fileSliceOption.isPresent());

    Option<HoodieBaseFile> baseFileOption = fileSliceOption.get().getBaseFile();
    assertTrue(baseFileOption.isPresent());

    List<HoodieLogFile> logFiles = fileSliceOption.get().getLogFiles().collect(Collectors.toList());

    HoodieMergeOnReadSnapshotReader snapshotReader = new HoodieMergeOnReadSnapshotReader(
        basePath(),
        baseFileOption.get().getPath(),
        logFiles,
        latestInstantTime,
        schema,
        jobConf,
        0,
        10000000
        );

    Map<String, HoodieRecord> r = snapshotReader.getRecordsByKey();
    List<HoodieRecord> obtainedRecords = r.values().stream().collect(Collectors.toList());
    assertEquals(records.size(), obtainedRecords.size());

    Set<HoodieKey> keys = records.stream().map(HoodieRecord::getKey).collect(Collectors.toSet());
    Set<HoodieKey> obtainedKeys = obtainedRecords.stream().map(HoodieRecord::getKey).collect(Collectors.toSet());

    Set<HoodieKey> allKeys = new HashSet<>(keys);
    allKeys.retainAll(obtainedKeys);
    assertEquals(keys.size(), allKeys.size());
  }

  public void checkDataIntegrityUsingROReader(List<HoodieRecord> records) {
    SparkRDDReadClient readClient = new SparkRDDReadClient<>(context(), basePath(), sqlContext());
    List<HoodieKey> keys = records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
    JavaRDD<HoodieKey> keysRDD = jsc().parallelize(keys, 1);
    List<Row> rows = readClient.readROView(keysRDD, 1).collectAsList();
    assertEquals(records.size(), rows.size());
  }

  public void checkDataIntegrity(int numRecords) {
    List<Row> rows = spark()
        .read()
        .format("org.apache.hudi")
        .load(basePath() + "/" + getPartitionPath())
        .collectAsList();
    assertEquals(numRecords, rows.size());
  }

  @Test
  public void testSparkRecordWorkflow() throws Exception {
    Schema avroSchema = getAvroSchema("AvroSchema", "AvroSchemaNS");
    HoodieWriteConfig writeConfig = getWriteConfig(avroSchema);

    // Check if the merger is correct.
    HoodieRecordMerger merger = writeConfig.getRecordMerger();
    assertTrue(merger instanceof HoodieSparkRecordMerger);
    writer.write("------------- Check merge type DONE");
    writer.newLine();

    // Check if the table type is correct.
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(writeConfig, context(), reloadedMetaClient);
    assertEquals(hoodieTable.getMetaClient().getTableType(), HoodieTableType.MERGE_ON_READ);
    LOG.error("------------- Check table type DONE");
    writer.newLine();

    // Write and read.
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      List<HoodieRecord> records = generateRecords(10);

      // (1) Write: insert.
      String instantTime = "001";
      writeClient.startCommitWithTime(instantTime);
      insertRecordsToMORTable(
          reloadedMetaClient, records, writeClient, writeConfig, instantTime);
      writer.write("------------- Insert the first batch of data DONE");
      writer.newLine();
      checkDataIntegrity(records.size());

      // (2) Write: new records.
      instantTime = "002";
      writeClient.startCommitWithTime(instantTime);

      List<HoodieRecord> records2 = generateRecords(10);
      updateRecordsInMORTable(
          reloadedMetaClient, records2, writeClient, writeConfig, instantTime, false);
      writer.write("------------- Insert the second batch of data DONE");
      writer.newLine();
      checkDataIntegrity(records.size() + records.size());

      // (3) Write: append.
      instantTime = "003";
      writeClient.startCommitWithTime(instantTime);

      List<HoodieRecord> records3 = generateRecordUpdates(getKeys(records2));
      updateRecordsInMORTable(reloadedMetaClient, records3, writeClient, writeConfig, instantTime, false);
      writer.write("------------- Update the first batch of data DONE");
      writer.newLine();
      checkDataIntegrity(records.size() + records2.size());
    }
    writer.close();
  }
}
