/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBlobSupport extends SparkClientFunctionalTestHarness {
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("test_blobs", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT), null, null),
      HoodieSchemaField.of("data", HoodieSchema.createBlob(), null, null)));

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testEndToEnd(HoodieTableType tableType) throws IOException {
    String filePath1 = createTestFile("file1.bin", 1000);
    String filePath2 = createTestFile("file2.bin", 1000);

    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.recordkey.field", "id");
    properties.put("hoodie.datasource.write.partitionpath.field", "");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "");
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.PARQUET.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, properties);
    HoodieWriteConfig config = getConfigBuilder(true).withSchema(SCHEMA.toString()).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String commit1 = client.startCommit();
      List<HoodieRecord> firstBatch = createTestRecords(filePath1);
      List<WriteStatus> statuses = client.insert(jsc().parallelize(firstBatch, 1), commit1).collect();
      client.commit(commit1, jsc().parallelize(statuses, 1));

      // create a second commit
      String commit2 = client.startCommit();
      List<HoodieRecord> secondBatch = createTestRecords(filePath2);
      statuses = client.upsert(jsc().parallelize(secondBatch, 1), commit2).collect();
      client.commit(commit2, jsc().parallelize(statuses, 1));
    }
    Dataset<Row> table = spark().read().format("hudi").load(basePath());
    List<Row> rows = table.collectAsList();
    assertEquals(10, rows.size());
    rows.forEach(row -> {
      Row data = row.getStruct(row.fieldIndex("data"));
      Row reference = data.getStruct(data.fieldIndex("reference"));
      String filePath = reference.getString(reference.fieldIndex("file"));
      assertTrue(filePath.endsWith("file2.bin"));
    });

    table.createOrReplaceTempView("hudi_table_view");
    List<Row> sqlRows = spark().sql("SELECT id, value, resolve_bytes(data) as full_bytes from hudi_table_view").collectAsList();
    assertEquals(10, sqlRows.size());
  }

  private List<HoodieRecord> createTestRecords(String filePath) {
    return IntStream.range(0, 10).mapToObj(i -> {
      String id = "id_" + i;
      HoodieKey key = new HoodieKey(id, "");

      GenericRecord fileReference = new GenericData.Record(
          SCHEMA.getField("data").get().schema().getField("reference").get().getNonNullSchema().toAvroSchema());
      fileReference.put("file", filePath);
      fileReference.put("position", i * 100);
      fileReference.put("length", 100);
      fileReference.put("managed", false);

      GenericRecord blobRecord = new GenericData.Record(SCHEMA.getField("data").get().schema().toAvroSchema());
      blobRecord.put("storage_type", "out_of_line");
      blobRecord.put("reference", fileReference);

      GenericRecord record = new GenericData.Record(SCHEMA.toAvroSchema());
      record.put("id", id);
      record.put("value", i);
      record.put("data", blobRecord);

      return new HoodieAvroIndexedRecord(key, record);
    }).collect(Collectors.toList());
  }

  private String createTestFile(String name, int size) throws IOException {
    File file = new File(tempDir.toString(), name);
    byte[] bytes = new byte[size];
    IntStream.range(0, size).forEach(i -> bytes[i] = (byte) (i % 256));
    Files.write(file.toPath(), bytes);
    return file.getAbsolutePath();
  }
}
