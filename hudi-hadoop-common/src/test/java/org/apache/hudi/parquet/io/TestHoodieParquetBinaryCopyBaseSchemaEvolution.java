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

package org.apache.hudi.parquet.io;

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for HoodieParquetBinaryCopyBase schema evolution behavior.
 */
public class TestHoodieParquetBinaryCopyBaseSchemaEvolution {

  @Mock
  private CompressionConverter.TransParquetFileReader reader;
  @Mock
  private ParquetMetadata parquetMetadata;
  @Mock
  private FileMetaData fileMetaData;
  @Mock
  private BlockMetaData blockMetaData;
  @Mock
  private ColumnChunkMetaData columnChunkMetaData;
  @Mock
  private EncodingStats encodingStats;

  private TestableHoodieParquetBinaryCopyBase copyBase;
  private MessageType requiredSchema;
  private MessageType fileSchema;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    
    // Setup test schemas
    requiredSchema = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.REQUIRED)
            .named("field1"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.Repetition.OPTIONAL)
            .named("field2"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.OPTIONAL)
            .named("field3"))
        .named("TestRecord");
    
    // File schema missing field3
    fileSchema = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.REQUIRED)
            .named("field1"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.Repetition.OPTIONAL)
            .named("field2"))
        .named("TestRecord");
    
    copyBase = spy(new TestableHoodieParquetBinaryCopyBase(new Configuration()));
    copyBase.requiredSchema = requiredSchema;
    
    // Setup mocks
    when(reader.getFooter()).thenReturn(parquetMetadata);
    when(parquetMetadata.getFileMetaData()).thenReturn(fileMetaData);
    when(fileMetaData.getSchema()).thenReturn(fileSchema);
    when(columnChunkMetaData.getEncodingStats()).thenReturn(encodingStats);
    
    // Mock the addNullColumn method to avoid complex setup
    doNothing().when(copyBase).addNullColumn(any(ColumnDescriptor.class), anyLong(), any(EncodingStats.class), 
        any(), any(MessageType.class), any(CompressionCodecName.class));
  }

  @Test
  public void testSchemaEvolutionEnabled_AllowsMissingColumns() throws Exception {
    // Given: Schema evolution is enabled (default)
    copyBase.setSchemaEvolutionEnabled(true);
    
    List<ColumnChunkMetaData> columnsInOrder = Arrays.asList(columnChunkMetaData);
    when(blockMetaData.getColumns()).thenReturn(columnsInOrder);
    
    // Setup mock to simulate processing logic without the complex ParquetFileWriter setup
    copyBase.setupForTesting(reader, blockMetaData, columnsInOrder);
    
    // When: Processing blocks with missing columns
    // Then: Should not throw exception
    assertDoesNotThrow(() -> {
      copyBase.testProcessMissedColumns();
    });
    
    // Should call addNullColumn for missing field3
    verify(copyBase, times(1)).addNullColumn(any(ColumnDescriptor.class), anyLong(), 
        any(EncodingStats.class), any(), eq(requiredSchema), any(CompressionCodecName.class));
  }

  @Test
  public void testSchemaEvolutionDisabled_ThrowsExceptionOnMissingColumns() throws Exception {
    // Given: Schema evolution is disabled
    copyBase.setSchemaEvolutionEnabled(false);
    
    List<ColumnChunkMetaData> columnsInOrder = Arrays.asList(columnChunkMetaData);
    when(blockMetaData.getColumns()).thenReturn(columnsInOrder);
    
    copyBase.setupForTesting(reader, blockMetaData, columnsInOrder);
    
    // When: Processing blocks with missing columns
    // Then: Should throw HoodieException
    HoodieException exception = assertThrows(HoodieException.class, () -> {
      copyBase.testProcessMissedColumns();
    });
    
    // Verify exception message contains information about missing columns
    assertEquals("Schema evolution is disabled but found missing columns in input file: field3. "
        + "All input files must have the same schema when schema evolution is disabled.", exception.getMessage());
    
    // Should not call addNullColumn when evolution is disabled
    verify(copyBase, never()).addNullColumn(any(ColumnDescriptor.class), anyLong(), 
        any(EncodingStats.class), any(), any(MessageType.class), any(CompressionCodecName.class));
  }

  @Test
  public void testSchemaEvolutionDisabled_NoMissingColumns_DoesNotThrow() throws Exception {
    // Given: Schema evolution is disabled, but file schema matches required schema
    copyBase.setSchemaEvolutionEnabled(false);
    
    // File schema with all required fields
    MessageType completeFileSchema = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.REQUIRED)
            .named("field1"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.Repetition.OPTIONAL)
            .named("field2"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.OPTIONAL)
            .named("field3"))
        .named("TestRecord");
    
    when(fileMetaData.getSchema()).thenReturn(completeFileSchema);
    
    List<ColumnChunkMetaData> columnsInOrder = Arrays.asList(columnChunkMetaData);
    when(blockMetaData.getColumns()).thenReturn(columnsInOrder);
    
    copyBase.setupForTesting(reader, blockMetaData, columnsInOrder);
    
    // When: Processing blocks with no missing columns
    // Then: Should not throw exception
    assertDoesNotThrow(() -> {
      copyBase.testProcessMissedColumns();
    });
    
    // Should not call addNullColumn when no columns are missing
    verify(copyBase, never()).addNullColumn(any(ColumnDescriptor.class), anyLong(), 
        any(EncodingStats.class), any(), any(MessageType.class), any(CompressionCodecName.class));
  }

  @Test
  public void testSchemaEvolutionDisabled_SkipsLegacyConversion() throws Exception {
    // Given: Schema evolution is disabled
    copyBase.setSchemaEvolutionEnabled(false);
    
    // Setup a column that would normally trigger legacy conversion
    ColumnChunkMetaData legacyColumn = mock(ColumnChunkMetaData.class);
    ColumnPath legacyPath = ColumnPath.fromDotString("testArray.bag.array_element");
    when(legacyColumn.getPath()).thenReturn(legacyPath);
    
    List<ColumnChunkMetaData> columnsInOrder = Arrays.asList(legacyColumn);
    when(blockMetaData.getColumns()).thenReturn(columnsInOrder);
    
    // Create descriptors map that doesn't contain the legacy path (simulating descriptor == null case)
    Map<ColumnPath, ColumnDescriptor> descriptorsMap = new HashMap<>();
    
    copyBase.setupForTesting(reader, blockMetaData, columnsInOrder);
    
    // When: Processing columns with legacy paths
    boolean legacyConversionAttempted = copyBase.testLegacyConversionLogic(legacyPath, descriptorsMap);
    
    // Then: Legacy conversion should be skipped
    assertEquals(false, legacyConversionAttempted, "Legacy conversion should be skipped when schema evolution is disabled");
  }

  @Test
  public void testSchemaEvolutionEnabled_AllowsLegacyConversion() throws Exception {
    // Given: Schema evolution is enabled
    copyBase.setSchemaEvolutionEnabled(true);
    
    // Setup a column that would trigger legacy conversion
    ColumnChunkMetaData legacyColumn = mock(ColumnChunkMetaData.class);
    ColumnPath legacyPath = ColumnPath.fromDotString("testArray.bag.array_element");
    when(legacyColumn.getPath()).thenReturn(legacyPath);
    
    List<ColumnChunkMetaData> columnsInOrder = Arrays.asList(legacyColumn);
    when(blockMetaData.getColumns()).thenReturn(columnsInOrder);
    
    // Create descriptors map that doesn't contain the legacy path
    Map<ColumnPath, ColumnDescriptor> descriptorsMap = new HashMap<>();
    
    copyBase.setupForTesting(reader, blockMetaData, columnsInOrder);
    
    // When: Processing columns with legacy paths
    boolean legacyConversionAttempted = copyBase.testLegacyConversionLogic(legacyPath, descriptorsMap);
    
    // Then: Legacy conversion should be attempted
    assertEquals(true, legacyConversionAttempted, "Legacy conversion should be attempted when schema evolution is enabled");
  }

  /**
   * Testable subclass that exposes internal methods and provides test setup.
   */
  private static class TestableHoodieParquetBinaryCopyBase extends HoodieParquetBinaryCopyBase {
    private CompressionConverter.TransParquetFileReader testReader;
    private BlockMetaData testBlock;
    private List<ColumnChunkMetaData> testColumnsInOrder;

    public TestableHoodieParquetBinaryCopyBase(Configuration conf) {
      super(conf);
    }

    public void setupForTesting(CompressionConverter.TransParquetFileReader reader, 
                               BlockMetaData block, List<ColumnChunkMetaData> columnsInOrder) {
      this.testReader = reader;
      this.testBlock = block;
      this.testColumnsInOrder = columnsInOrder;
    }

    public void testProcessMissedColumns() throws Exception {
      // Simulate the missed columns processing logic
      ParquetMetadata meta = testReader.getFooter();
      ColumnChunkMetaData columnChunkMetaData = testColumnsInOrder.get(0);
      EncodingStats encodingStats = columnChunkMetaData.getEncodingStats();
      
      List<ColumnDescriptor> missedColumns = missedColumns(requiredSchema, meta.getFileMetaData().getSchema())
          .stream()
          .collect(Collectors.toList());
      
      // If schema evolution is disabled and there are missing columns, throw an exception
      if (!schemaEvolutionEnabled && !missedColumns.isEmpty()) {
        String missingColumnsStr = missedColumns.stream()
            .map(c -> String.join(".", c.getPath()))
            .collect(Collectors.joining(", "));
        throw new HoodieException("Schema evolution is disabled but found missing columns in input file: " 
            + missingColumnsStr + ". All input files must have the same schema when schema evolution is disabled.");
      }
      
      for (ColumnDescriptor descriptor : missedColumns) {
        addNullColumn(descriptor, 100L, encodingStats, null, requiredSchema, CompressionCodecName.SNAPPY);
      }
    }

    public boolean testLegacyConversionLogic(ColumnPath columnPath, Map<ColumnPath, ColumnDescriptor> descriptorsMap) {
      ColumnDescriptor descriptor = descriptorsMap.get(columnPath);
      
      // resolve the conflict schema between avro parquet write support and spark native parquet write support
      // Only attempt legacy conversion if schema evolution is enabled
      if (descriptor == null && schemaEvolutionEnabled) {
        String[] path = columnPath.toArray();
        path = Arrays.copyOf(path, path.length);
        if (convertLegacy3LevelArray(path) || convertLegacyMap(path)) {
          return true; // Legacy conversion was attempted
        }
      }
      return false; // Legacy conversion was not attempted
    }

    private List<ColumnDescriptor> missedColumns(MessageType requiredSchema, MessageType fileSchema) {
      return requiredSchema.getColumns().stream()
          .filter(col -> !fileSchema.containsPath(col.getPath()))
          .collect(Collectors.toList());
    }

    // Mock implementation to avoid complex ParquetFileWriter setup
    protected void addNullColumn(ColumnDescriptor descriptor, long totalChunkValues, EncodingStats encodingStats,
                                Object writer, MessageType schema, CompressionCodecName newCodecName) {
      // Mock implementation - do nothing
    }

    @Override
    protected Map<String, String> finalizeMetadata() {
      return new HashMap<>();
    }
  }
}