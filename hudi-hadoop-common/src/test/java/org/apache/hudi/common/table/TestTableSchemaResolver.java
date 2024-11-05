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

package org.apache.hudi.common.table;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.functional.TestHoodieLogFormat.getDataBlock;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TableSchemaResolver}.
 */
public class TestTableSchemaResolver {

  @TempDir
  public java.nio.file.Path tempDir;

  @Test
  public void testRecreateSchemaWhenDropPartitionColumns() {
    Schema originSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // case2
    String[] pts1 = new String[0];
    Schema s2 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts1));
    assertEquals(originSchema, s2);

    // case3: partition_path is in originSchema
    String[] pts2 = {"partition_path"};
    Schema s3 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts2));
    assertEquals(originSchema, s3);

    // case4: user_partition is not in originSchema
    String[] pts3 = {"user_partition"};
    Schema s4 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts3));
    assertNotEquals(originSchema, s4);
    assertTrue(s4.getFields().stream().anyMatch(f -> f.name().equals("user_partition")));
    Schema.Field f = s4.getField("user_partition");
    assertEquals(f.schema(), AvroSchemaUtils.createNullableSchema(Schema.Type.STRING));

    // case5: user_partition is in originSchema, but partition_path is in originSchema
    String[] pts4 = {"user_partition", "partition_path"};
    try {
      TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts3));
    } catch (HoodieSchemaException e) {
      assertTrue(e.getMessage().contains("Partial partition fields are still in the schema"));
    }
  }

  @Test
  public void testReadSchemaFromLogFile() throws IOException, URISyntaxException, InterruptedException {
    String testDir = initTestDir("read_schema_from_log_file");
    StoragePath partitionPath = new StoragePath(testDir, "partition1");
    Schema expectedSchema = getSimpleSchema();
    StoragePath logFilePath = writeLogFile(partitionPath, expectedSchema);
    assertEquals(expectedSchema, TableSchemaResolver.readSchemaFromLogFile(new HoodieHadoopStorage(
        logFilePath, HoodieTestUtils.getDefaultStorageConfWithDefaults()), logFilePath));
  }

  private String initTestDir(String folderName) throws IOException {
    java.nio.file.Path basePath = tempDir.resolve(folderName);
    java.nio.file.Files.createDirectories(basePath);
    return basePath.toString();
  }

  private StoragePath writeLogFile(StoragePath partitionPath, Schema schema) throws IOException, URISyntaxException, InterruptedException {
    HoodieStorage storage = HoodieTestUtils.getStorage(partitionPath);
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(AVRO_DATA_BLOCK, records, header);
    writer.appendBlock(dataBlock);
    writer.close();
    return writer.getLogFile().getPath();
  }
}
