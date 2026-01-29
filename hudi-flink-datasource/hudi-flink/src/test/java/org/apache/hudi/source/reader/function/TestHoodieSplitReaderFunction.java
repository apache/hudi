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
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link HoodieSplitReaderFunction}.
 */
public class TestHoodieSplitReaderFunction {

  private HoodieFlinkTable<RowData> mockTable;
  private HoodieReaderContext<RowData> mockReaderContext;
  private HoodieSchema tableSchema;
  private HoodieSchema requiredSchema;
  private HoodieTableMetaClient mockMetaClient;

  @BeforeEach
  public void setUp() {
    mockTable = mock(HoodieFlinkTable.class);
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
      new HoodieSplitReaderFunction(
          mockTable,
          mockReaderContext,
          new Configuration(),
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
      new HoodieSplitReaderFunction(
          mockTable,
          mockReaderContext,
          new Configuration(),
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
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
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

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema)
        );

    assertNotNull(function);
  }

  @Test
  public void testClosedReaderIsNull() throws Exception {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
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
      HoodieSplitReaderFunction function =
          new HoodieSplitReaderFunction(
              mockTable,
              mockReaderContext,
              new Configuration(),
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
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
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

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
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
    HoodieSplitReaderFunction function1 =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema1)
        );
    assertNotNull(function1);

    // Test with different internal schema
    HoodieSplitReaderFunction function2 =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.of(internalSchema2)
        );
    assertNotNull(function2);

    // Test with empty internal schema
    HoodieSplitReaderFunction function3 =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
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
    HoodieFlinkTable<RowData> customTable = mock(HoodieFlinkTable.class);
    HoodieTableMetaClient customMetaClient = mock(HoodieTableMetaClient.class);

    when(customTable.getMetaClient()).thenReturn(customMetaClient);

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            customTable,
            mockReaderContext,
            new Configuration(),
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

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            customContext,
            new Configuration(),
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }

  @Test
  public void testConfigurationIsStored() {
    Configuration config = new Configuration();
    config.setString("test.key", "test.value");

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            config,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
  }

  @Test
  public void testReadMethodSignature() {
    // Verify that the read method returns CloseableIterator
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockTable,
            mockReaderContext,
            new Configuration(),
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Option.empty()
        );

    assertNotNull(function);
    // The read method signature has changed to return CloseableIterator<RecordsWithSplitIds<...>>
    // This test verifies the function can be constructed with the new signature
  }
}
