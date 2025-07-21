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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
    Path partitionPath = new Path(testDir, "partition1");
    Schema expectedSchema = getSimpleSchema();
    Path logFilePath = writeLogFile(partitionPath, expectedSchema);
    assertEquals(
        new AvroSchemaConverter().convert(expectedSchema),
        TableSchemaResolver.readSchemaFromLogFile(
            logFilePath.getFileSystem(new Configuration()), logFilePath));
  }

  @Test
  void testHasOperationFieldFileInspectionOrdering() {
    Configuration conf = new Configuration(false);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getHadoopConf()).thenReturn(conf);

    when(metaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    when(metaClient.getBasePathV2()).thenReturn(new Path("/tmp/hudi_table"));
    TableSchemaResolver schemaResolver = spy(new TableSchemaResolver(metaClient));
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create 3 base files and 2 log files
    HoodieWriteStat baseFileWriteStat = buildWriteStat("partition1/baseFile1.parquet", 10, 100);
    HoodieWriteStat logFileWriteStat = buildWriteStat("partition1/logFile1.log", 0, 10);
    // we don't expect any interactions with this write stat since the code should exit as soon as a valid schema is found
    HoodieWriteStat baseFileWriteStat2 = mock(HoodieWriteStat.class);
    // files that only have deletes will be skipped
    HoodieWriteStat logFileWithOnlyDeletes = buildWriteStat("partition1/logFile2.log", 0, 0);
    HoodieWriteStat baseFileWithOnlyDeletes = buildWriteStat("partition2/baseFile2.parquet", 0, 0);

    commitMetadata.addWriteStat("partition1", baseFileWriteStat);
    commitMetadata.addWriteStat("partition1", logFileWithOnlyDeletes);
    commitMetadata.addWriteStat("partition1", logFileWriteStat);
    commitMetadata.addWriteStat("partition1", baseFileWriteStat2);
    commitMetadata.addWriteStat("partition2", baseFileWithOnlyDeletes);
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "001");
    when(metaClient.getActiveTimeline().getLastCommitMetadataWithValidData()).thenReturn(Option.of(Pair.of(instant, commitMetadata)));

    // mock calls to read schema
    try (MockedStatic<ParquetFileReader> fileReaderMockedStatic = mockStatic(ParquetFileReader.class);
         MockedStatic<TableSchemaResolver> tableSchemaResolverMockedStatic = mockStatic(TableSchemaResolver.class)) {
      // return null for first parquet file to force iteration to inspect the next file
      ParquetMetadata parquetMetadata = mock(ParquetMetadata.class, RETURNS_DEEP_STUBS);
      MessageType messageType = new MessageType("TestSchema", Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("int_field"),
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("_hoodie_operation"));
      when(parquetMetadata.getFileMetaData().getSchema()).thenReturn(null);
      fileReaderMockedStatic
          .when(() -> ParquetFileReader.readFooter(any(Configuration.class), eq(new Path("/tmp/hudi_table/partition1/baseFile1.parquet")), any()))
          .thenReturn(parquetMetadata);
      tableSchemaResolverMockedStatic.when(() -> TableSchemaResolver.readSchemaFromLogFile(any(), eq(new Path("/tmp/hudi_table/partition1/logFile1.log"))))
          .thenReturn(messageType);

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

  private Path writeLogFile(Path partitionPath, Schema schema) throws IOException, URISyntaxException, InterruptedException {
    FileSystem fs = partitionPath.getFileSystem(new Configuration());
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
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
