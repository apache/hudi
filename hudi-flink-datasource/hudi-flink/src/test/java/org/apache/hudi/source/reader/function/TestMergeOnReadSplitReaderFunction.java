/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link MergeOnReadSplitReaderFunction}.
 */
public class TestMergeOnReadSplitReaderFunction {

  private HoodieTable<RowData, ?, ?, ?> mockTable;
  private HoodieReaderContext<RowData> mockReaderContext;
  private HoodieSchema tableSchema;
  private HoodieSchema requiredSchema;
  private HoodieTableMetaClient mockMetaClient;

  @BeforeEach
  public void setUp() {
    mockTable = mock(HoodieTable.class);
    mockReaderContext = mock(HoodieReaderContext.class);
    mockMetaClient = mock(HoodieTableMetaClient.class);

    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);

    // Create mock schemas
    tableSchema = mock(HoodieSchema.class);
    requiredSchema = mock(HoodieSchema.class);
  }

  @Test
  public void testConstructorValidatesTableSchema() {
    // Test that constructor requires non-null tableSchema
    assertThrows(IllegalArgumentException.class, () -> {
      new MergeOnReadSplitReaderFunction<>(
          mockTable,
          mockReaderContext,
          null,  // null tableSchema should throw
          requiredSchema,
          "AVRO_PAYLOAD",
          Option.empty()
      );
    });
  }

  @Test
  public void testConstructorValidatesRequiredSchema() {
    // Test that constructor requires non-null requiredSchema
    assertThrows(IllegalArgumentException.class, () -> {
      new MergeOnReadSplitReaderFunction<>(
          mockTable,
          mockReaderContext,
          tableSchema,
          null,  // null requiredSchema should throw
          "AVRO_PAYLOAD",
          Option.empty()
      );
    });
  }

  @Test
  public void testConstructorWithValidParameters() {
    // Should not throw exception with valid parameters
    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithInternalSchema() {
    InternalSchema internalSchema = mock(InternalSchema.class);

    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema)
        );

    assertNotNull(function);
  }

  @Test
  public void testClosedReaderIsNull() throws Exception {
    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    // Close should not throw exception even when fileGroupReader is null
    function.close();
  }

  @Test
  public void testMergeTypeConfiguration() {
    // Test different merge types
    String[] mergeTypes = {
        "AVRO_PAYLOAD",
        "CUSTOM_PAYLOAD",
        HoodieReaderConfig.MERGE_TYPE.defaultValue()
    };

    for (String mergeType : mergeTypes) {
      MergeOnReadSplitReaderFunction<?, ?, ?> function =
          new MergeOnReadSplitReaderFunction<>(
              mockTable,
              mockReaderContext,
              tableSchema,
              requiredSchema,
              mergeType,
              Option.empty()
          );

      assertNotNull(function);
    }
  }

  @Test
  public void testMultipleCloseCalls() throws Exception {
    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    // Multiple close calls should not throw exception
    function.close();
    function.close();
    function.close();
  }

  @Test
  public void testSchemaHandling() {
    HoodieSchema customTableSchema = mock(HoodieSchema.class);
    HoodieSchema customRequiredSchema = mock(HoodieSchema.class);

    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            customTableSchema,
            customRequiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }

  @Test
  public void testInternalSchemaHandling() {
    InternalSchema internalSchema1 = mock(InternalSchema.class);
    InternalSchema internalSchema2 = mock(InternalSchema.class);

    // Test with present internal schema
    MergeOnReadSplitReaderFunction<?, ?, ?> function1 =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema1)
        );
    assertNotNull(function1);

    // Test with different internal schema
    MergeOnReadSplitReaderFunction<?, ?, ?> function2 =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema2)
        );
    assertNotNull(function2);

    // Test with empty internal schema
    MergeOnReadSplitReaderFunction<?, ?, ?> function3 =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );
    assertNotNull(function3);
  }

  @Test
  public void testHoodieTableIntegration() {
    // Verify that the function properly interacts with HoodieTable
    HoodieTable<RowData, ?, ?, ?> customTable = mock(HoodieTable.class);
    HoodieTableMetaClient customMetaClient = mock(HoodieTableMetaClient.class);

    when(customTable.getMetaClient()).thenReturn(customMetaClient);

    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            customTable,
            mockReaderContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }

  @Test
  public void testReaderContextIntegration() {
    // Test with different reader contexts
    HoodieReaderContext<RowData> customContext = mock(HoodieReaderContext.class);

    MergeOnReadSplitReaderFunction<?, ?, ?> function =
        new MergeOnReadSplitReaderFunction<>(
            mockTable,
            customContext,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }
}
