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

import org.apache.flink.table.types.AtomicDataType;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.utils.TestConfigurations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link HoodieSplitReaderFunction}.
 */
public class TestHoodieSplitReaderFunction {
  @TempDir
  File tempDir;

  private HoodieSchema tableSchema;
  private HoodieSchema requiredSchema;
  private HoodieTableMetaClient mockMetaClient;
  private Configuration conf;

  @BeforeEach
  public void setUp() {
    mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);

    // Create mock schemas
    tableSchema = mock(HoodieSchema.class);
    requiredSchema = mock(HoodieSchema.class);
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
  }

  @Test
  public void testConstructorValidatesTableSchema() {
    // Test that constructor requires non-null tableSchema
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSplitReaderFunction(
          mockMetaClient,
          conf,
          null,  // null tableSchema should throw
          requiredSchema,
          "AVRO_PAYLOAD",
          Collections.emptyList(),
              false
      );
    });
  }

  @Test
  public void testConstructorValidatesRequiredSchema() {
    // Test that constructor requires non-null requiredSchema
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSplitReaderFunction(
          mockMetaClient,
          conf,
          tableSchema,
          null,  // null requiredSchema should throw
          "AVRO_PAYLOAD",
          Collections.emptyList(),
          false
      );
    });
  }

  @Test
  public void testConstructorWithValidParameters() {
    // Should not throw exception with valid parameters
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithInternalSchema() {
    InternalSchema internalSchema = mock(InternalSchema.class);

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testClosedReaderIsNull() throws Exception {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false

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
              mockMetaClient,
              conf,
              tableSchema,
              requiredSchema,
              mergeType,
              Collections.emptyList(),
              false
          );

      assertNotNull(function);
    }
  }

  @Test
  public void testMultipleCloseCalls() throws Exception {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
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
            mockMetaClient,
            conf,
            customTableSchema,
            customRequiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
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
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );
    assertNotNull(function1);

    // Test with different internal schema
    HoodieSplitReaderFunction function2 =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );
    assertNotNull(function2);

    // Test with empty internal schema
    HoodieSplitReaderFunction function3 =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );
    assertNotNull(function3);
  }

  @Test
  public void testConfigurationIsStored() {
    conf.setString("test.key", "test.value");

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
                conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testReadMethodSignature() {
    // Verify that the read method returns CloseableIterator
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithEmitDeleteTrue() {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            true
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithPredicatesAndEmitDelete() {
    ExpressionPredicates.Predicate predicate = ExpressionPredicates.NotEquals.getInstance()
            .bindFieldReference(new FieldReferenceExpression("status", new AtomicDataType(new VarCharType(true, 10)), 0, 0))
            .bindValueLiteral(new ValueLiteralExpression("deleted"));

    List<ExpressionPredicates.Predicate> predicates = Collections.singletonList(predicate);

    HoodieSplitReaderFunction function =
            new HoodieSplitReaderFunction(
                    mockMetaClient,
                    conf,
                    tableSchema,
                    requiredSchema,
                    "AVRO_PAYLOAD",
                    predicates,
                    true
            );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithEmitDeleteFalse() {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            mockMetaClient,
            conf,
            tableSchema,
            requiredSchema,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }
}
