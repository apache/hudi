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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the base files of a table of a non-native format get the three-level Parquet list layout when the
 * write config does not name the table format, which the write handles then take from the table config.
 */
class TestHoodieJavaClientParquetListLayoutForTestFormat extends HoodieJavaClientTestHarness {

  private static final String PARTITION = "2016/03/15";
  private static final Schema SCHEMA = SchemaBuilder.record("rec").fields()
      .requiredString("_row_key")
      .requiredString("partition_path")
      .requiredLong("timestamp")
      .name("values").type().array().items().nullable().intType().noDefault()
      .endRecord();

  @Override
  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      initPath();
    }
    storageConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, properties, null)
        .initTable(storageConf.newInstance(), basePath);
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    TestTableFormat.tearDown();
    FileSystem.closeAll();
  }

  @Test
  void testInsertAndUpsertWriteThreeLevelListsWithoutTableFormatInWriteConfig() throws Exception {
    assertEquals("test-format", metaClient.getTableConfig().getString(HoodieTableConfig.TABLE_FORMAT));
    HoodieWriteConfig config = getConfigBuilder(SCHEMA.toString(), HoodieIndex.IndexType.SIMPLE)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    assertFalse(config.contains(HoodieTableConfig.TABLE_FORMAT));

    Map<String, List<Integer>> expected = new HashMap<>();
    expected.put("key0", Arrays.asList(1, null, 3));
    expected.put("key1", Collections.singletonList(null));
    expected.put("key2", Collections.emptyList());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    // The insert goes through the create handle
    write(client, expected, false);
    assertBaseFile(expected);

    // The upsert rewrites the base file through the merge handle
    Map<String, List<Integer>> updates = new HashMap<>();
    updates.put("key2", Arrays.asList(null, null));
    updates.put("key3", Arrays.asList(4, null));
    write(client, updates, true);
    expected.putAll(updates);
    assertBaseFile(expected);
  }

  private void write(HoodieJavaWriteClient client, Map<String, List<Integer>> values, boolean upsert) {
    List<HoodieRecord> records = values.entrySet().stream().map(entry -> {
      GenericRecord record = new GenericData.Record(SCHEMA);
      record.put("_row_key", entry.getKey());
      record.put("partition_path", PARTITION);
      record.put("timestamp", 0L);
      record.put("values", entry.getValue());
      return (HoodieRecord) new HoodieAvroIndexedRecord(new HoodieKey(entry.getKey(), PARTITION), record);
    }).collect(Collectors.toList());
    String instantTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<WriteStatus> statuses = upsert ? client.upsert(records, instantTime) : client.insert(records, instantTime);
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instantTime, statuses));
  }

  private void assertBaseFile(Map<String, List<Integer>> expected) throws IOException {
    List<HoodieBaseFile> baseFiles;
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      baseFiles = fsView.getLatestBaseFiles(PARTITION).collect(Collectors.toList());
    }
    assertEquals(1, baseFiles.size());
    Path path = new Path(baseFiles.get(0).getPath());
    Configuration conf = new Configuration();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      // "list" is the repeated field of the three-level layout, "array" the one of the legacy two-level layout
      assertEquals("list", reader.getFooter().getFileMetaData().getSchema().getType("values").asGroupType().getType(0).getName());
    }
    Map<String, List<Integer>> actual = new HashMap<>();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, conf)).build()) {
      for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
        actual.put(record.get("_row_key").toString(), new ArrayList<>((List<Integer>) record.get("values")));
      }
    }
    assertEquals(expected, actual);
  }
}
