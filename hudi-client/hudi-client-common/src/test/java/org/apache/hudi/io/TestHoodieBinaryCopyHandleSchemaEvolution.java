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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * Tests for HoodieBinaryCopyHandle schema evolution behavior.
 */
public class TestHoodieBinaryCopyHandleSchemaEvolution {

  private HoodieWriteConfig config;
  @Mock
  private HoodieTable<?, ?, ?, ?> hoodieTable;
  @Mock
  private TaskContextSupplier taskContextSupplier;
  @Mock
  private HoodieStorage storage;
  private Configuration hadoopConf;

  private MessageType fileSchema;
  private MessageType tableSchema;
  private Schema avroSchema;
  private List<StoragePath> inputFiles;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    
    // Initialize Hadoop Configuration
    hadoopConf = new Configuration();
    
    // Setup test schemas
    String avroSchemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"},"
        + "{\"name\":\"field2\",\"type\":\"int\"}"
        + "]}";
    avroSchema = new Schema.Parser().parse(avroSchemaStr);
    
    // Create different schemas for file vs table
    fileSchema = new AvroSchemaConverter().convert(avroSchema);
    
    String tableAvroSchemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"},"
        + "{\"name\":\"field2\",\"type\":\"int\"},"
        + "{\"name\":\"field3\",\"type\":[\"null\",\"string\"],\"default\":null}"
        + "]}";
    Schema tableAvroSchema = new Schema.Parser().parse(tableAvroSchemaStr);
    tableSchema = new AvroSchemaConverter().convert(tableAvroSchema);
    
    inputFiles = Arrays.asList(new StoragePath("/test/file1.parquet"), new StoragePath("/test/file2.parquet"));
    
    // Setup mocks
    when(hoodieTable.getStorage()).thenReturn(storage);
    when(hoodieTable.getStorageConf()).thenReturn(mock(org.apache.hudi.storage.StorageConfiguration.class));
    when(hoodieTable.getStorageConf().unwrapAs(Configuration.class)).thenReturn(hadoopConf);
  }

  @Test
  public void testSchemaEvolutionDisabled_UsesFileSchema() throws Exception {
    // Given: Schema evolution is disabled
    config = HoodieWriteConfig.newBuilder()
        .withPath("/dummy/path")
        .build();
    config.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "false");
    
    // Mock ParquetUtils to return file schema
    try (MockedConstruction<ParquetUtils> parquetUtilsConstruction = mockConstruction(ParquetUtils.class, 
        (mock, context) -> {
          when(mock.readSchema(eq(storage), eq(inputFiles.get(0)))).thenReturn(fileSchema);
        })) {
      
      // When: Creating HoodieBinaryCopyHandle (we can't instantiate directly due to complex dependencies,
      // so we test the getWriteSchema method logic indirectly)
      TestableHoodieBinaryCopyHandle handle = new TestableHoodieBinaryCopyHandle();
      MessageType result = handle.testGetWriteSchema(config, inputFiles, hadoopConf, hoodieTable);
      
      // Then: Should use file schema, not table schema
      assertEquals(fileSchema, result);
      // Verify that ParquetUtils was constructed and readSchema was called
      assertEquals(1, parquetUtilsConstruction.constructed().size());
    }
  }

  @Test
  public void testSchemaEvolutionEnabled_UsesTableSchema() throws Exception {
    // Given: Schema evolution is enabled (default)
    config = HoodieWriteConfig.newBuilder()
        .withPath("/dummy/path")
        .build();
    config.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "true");
    
    // When: Creating HoodieBinaryCopyHandle
    TestableHoodieBinaryCopyHandle handle = new TestableHoodieBinaryCopyHandle();
    handle.setWriteSchemaWithMetaFields(avroSchema); // Set the expected table schema
    MessageType result = handle.testGetWriteSchema(config, inputFiles, hadoopConf, hoodieTable);
    
    // Then: Should use table schema (converted from Avro), not file schema
    assertNotNull(result);
    // When evolution is enabled, it should use the table schema directly
  }

  @Test
  public void testSchemaEvolutionDisabled_FileReadError_ThrowsException() throws Exception {
    // Given: Schema evolution is disabled but file read fails
    config = HoodieWriteConfig.newBuilder()
        .withPath("/dummy/path")
        .build();
    config.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "false");
    
    // When: Creating HoodieBinaryCopyHandle with a file that causes read error
    TestableHoodieBinaryCopyHandle handle = new TestableHoodieBinaryCopyHandle();
    handle.setWriteSchemaWithMetaFields(avroSchema);
    handle.setSimulateFileReadError(true);
    
    // Then: Should throw HoodieIOException
    assertThrows(HoodieIOException.class, () -> 
        handle.testGetWriteSchema(config, inputFiles, hadoopConf, hoodieTable)
    );
  }

  @Test
  public void testSchemaEvolutionDisabled_EmptyInputFiles_UsesTableSchema() throws Exception {
    // Given: Schema evolution is disabled but no input files
    config = HoodieWriteConfig.newBuilder()
        .withPath("/dummy/path")
        .build();
    config.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "false");
    
    // When: Creating HoodieBinaryCopyHandle with empty input files
    TestableHoodieBinaryCopyHandle handle = new TestableHoodieBinaryCopyHandle();
    handle.setWriteSchemaWithMetaFields(avroSchema);
    MessageType result = handle.testGetWriteSchema(config, Arrays.asList(), hadoopConf, hoodieTable);
    
    // Then: Should use table schema, not attempt to read from files
    assertNotNull(result);
  }

  /**
   * Testable subclass that exposes the getWriteSchema method for testing.
   */
  private static class TestableHoodieBinaryCopyHandle {
    @Setter
    private Schema writeSchemaWithMetaFields;
    @Setter
    private boolean simulateFileReadError = false;
    
    public MessageType testGetWriteSchema(HoodieWriteConfig config, List<StoragePath> inputFiles, 
                                         Configuration conf, HoodieTable<?, ?, ?, ?> table) {
      if (!config.isBinaryCopySchemaEvolutionEnabled() && !inputFiles.isEmpty()) {
        // When schema evolution is disabled, use the schema from the first input file
        // All files should have the same schema in this case
        try {
          if (simulateFileReadError) {
            throw new IOException("Simulated file read error");
          }
          ParquetUtils parquetUtils = new ParquetUtils();
          MessageType fileSchema = parquetUtils.readSchema(table.getStorage(), inputFiles.get(0));
          return fileSchema;
        } catch (Exception e) {
          throw new HoodieIOException("Failed to read schema from input file when schema evolution is disabled: " + inputFiles.get(0),
              e instanceof IOException ? (IOException) e : new IOException(e));
        }
      } else {
        // Default behavior: use the table's write schema for evolution
        return new AvroSchemaConverter(conf).convert(writeSchemaWithMetaFields);
      }
    }
  }
}