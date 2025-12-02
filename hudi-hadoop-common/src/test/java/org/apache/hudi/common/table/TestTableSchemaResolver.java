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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;
import static org.apache.hudi.common.testutils.HoodieCommonTestHarness.getDataBlock;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TableSchemaResolver}.
 */
class TestTableSchemaResolver {

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  void testRecreateSchemaWhenDropPartitionColumns() {
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
  void testGetTableSchema() throws Exception {
    // Setup: Create mock metaClient and configure behavior
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    Schema expectedSchema = getSimpleSchema();

    // Mock table setup
    when(metaClient.getTableConfig().populateMetaFields()).thenReturn(true);
    when(metaClient.getTableConfig().getTableCreateSchema())
        .thenReturn(Option.of(expectedSchema));

    when(metaClient.getActiveTimeline().getLastCommitMetadataWithValidSchema())
        .thenReturn(Option.empty());

    // Create resolver and call both methods
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    // Test 1: getTableSchema() - should use table config's populateMetaFields (true)
    Schema avroSchema = resolver.getTableAvroSchema();
    HoodieSchema hoodieSchema = resolver.getTableSchema();
    assertNotNull(hoodieSchema);
    assertEquals(avroSchema, hoodieSchema.getAvroSchema());

    // Test 2: getTableSchema(true) - explicitly include metadata fields
    Schema avroSchemaWithMetadata = resolver.getTableAvroSchema(true);
    HoodieSchema hoodieSchemaWithMetadata = resolver.getTableSchema(true);
    assertNotNull(hoodieSchemaWithMetadata);
    assertEquals(avroSchemaWithMetadata, hoodieSchemaWithMetadata.getAvroSchema());

    // Test 3: getTableSchema(false) - explicitly exclude metadata fields
    Schema avroSchemaWithoutMetadata = resolver.getTableAvroSchema(false);
    HoodieSchema hoodieSchemaWithoutMetadata = resolver.getTableSchema(false);
    assertNotNull(hoodieSchemaWithoutMetadata);
    assertEquals(avroSchemaWithoutMetadata, hoodieSchemaWithoutMetadata.getAvroSchema());
  }

  @Test
  void testReadSchemaFromLogFile() throws IOException, URISyntaxException, InterruptedException {
    String testDir = initTestDir("read_schema_from_log_file");
    StoragePath partitionPath = new StoragePath(testDir, "partition1");
    Schema expectedSchema = getSimpleSchema();
    StoragePath logFilePath = writeLogFile(partitionPath, expectedSchema);
    assertEquals(expectedSchema, TableSchemaResolver.readSchemaFromLogFile(new HoodieHadoopStorage(
        logFilePath, HoodieTestUtils.getDefaultStorageConfWithDefaults()), logFilePath));
  }

  @Test
  void testHasOperationFieldFileInspectionOrdering() throws IOException {
    StorageConfiguration conf = new HadoopStorageConfiguration(false);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getStorageConf()).thenReturn(conf);

    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/hudi_table"));
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create 3 base files and 2 log files
    HoodieWriteStat baseFileWriteStat = buildWriteStat("partition1/baseFile1.parquet", 10, 100);
    HoodieWriteStat logFileWriteStat = buildWriteStat("partition1/" + FSUtils.makeLogFileName("001", ".log", "100", 2, "1-0-1"), 0, 10);
    // we don't expect any interactions with this write stat since the code should exit as soon as a valid schema is found
    HoodieWriteStat baseFileWriteStat2 = mock(HoodieWriteStat.class);
    // files that only have deletes will be skipped
    HoodieWriteStat logFileWithOnlyDeletes = buildWriteStat("partition1/" + FSUtils.makeLogFileName("002", ".log", "100", 2, "1-0-1"), 0, 0);
    HoodieWriteStat baseFileWithOnlyDeletes = buildWriteStat("partition2/baseFile2.parquet", 0, 0);

    commitMetadata.addWriteStat("partition1", baseFileWriteStat);
    commitMetadata.addWriteStat("partition1", logFileWithOnlyDeletes);
    commitMetadata.addWriteStat("partition1", logFileWriteStat);
    commitMetadata.addWriteStat("partition1", baseFileWriteStat2);
    commitMetadata.addWriteStat("partition2", baseFileWithOnlyDeletes);
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "001", InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline().filterCompletedInstants()).thenReturn(timeline);
    when(timeline.getReverseOrderedInstants()).thenReturn(Stream.of(instant));
    when(timeline.readCommitMetadata(instant)).thenReturn(commitMetadata);

    // mock calls to read schema
    try (MockedStatic<HoodieIOFactory> ioFactoryMockedStatic = mockStatic(HoodieIOFactory.class);
         MockedStatic<TableSchemaResolver> tableSchemaResolverMockedStatic = mockStatic(TableSchemaResolver.class)) {
      // return null for first parquet file to force iteration to inspect the next file
      Schema schema = Schema.createRecord("test_schema", null, "test_namespace", false);
      schema.setFields(Arrays.asList(new Schema.Field("int_field", Schema.create(Schema.Type.INT)), new Schema.Field("_hoodie_operation", Schema.create(Schema.Type.STRING))));

      // mock parquet file schema reading to return null for the first base file to force iteration
      HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
      FileFormatUtils fileFormatUtils = mock(FileFormatUtils.class);
      StoragePath parquetPath = new StoragePath("/tmp/hudi_table/partition1/baseFile1.parquet");
      when(ioFactory.getFileFormatUtils(parquetPath)).thenReturn(fileFormatUtils);
      when(fileFormatUtils.readAvroSchema(any(), eq(parquetPath))).thenReturn(null);
      ioFactoryMockedStatic.when(() -> HoodieIOFactory.getIOFactory(any())).thenReturn(ioFactory);
      // mock log file schema reading to return the expected schema
      tableSchemaResolverMockedStatic.when(() -> TableSchemaResolver.readSchemaFromLogFile(any(), eq(new StoragePath("/tmp/hudi_table/" + logFileWriteStat.getPath()))))
          .thenReturn(schema);

      assertTrue(schemaResolver.hasOperationField());
    }
    verifyNoInteractions(baseFileWriteStat2);
  }

  private static HoodieWriteStat buildWriteStat(String path, int numInserts, int numUpdateWrites) {
    HoodieWriteStat logFileWriteStat = new HoodieWriteStat();
    logFileWriteStat.setPath(path);
    logFileWriteStat.setNumInserts(numInserts);
    logFileWriteStat.setNumUpdateWrites(numUpdateWrites);
    logFileWriteStat.setNumDeletes(1);
    return logFileWriteStat;
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
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
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
