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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
  private InternalSchemaManager mockInternalSchemaManager;
  private Configuration conf;

  @BeforeEach
  public void setUp() {
    mockMetaClient = mock(HoodieTableMetaClient.class);
    mockInternalSchemaManager = mock(InternalSchemaManager.class);
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
          conf,
          null,  // null tableSchema should throw
          requiredSchema,
          mockInternalSchemaManager,
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
          conf,
          tableSchema,
          null,  // null requiredSchema should throw
          mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
              conf,
              tableSchema,
              requiredSchema,
              mockInternalSchemaManager,

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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
            conf,
            customTableSchema,
            customRequiredSchema,
            mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
                "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );
    assertNotNull(function1);

    // Test with different internal schema
    HoodieSplitReaderFunction function2 =
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
                "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );
    assertNotNull(function2);

    // Test with empty internal schema
    HoodieSplitReaderFunction function3 =
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,

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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
                "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testReadMethodSignature() {
    // Verify the reader function constructs via its public signature (the read path is driven
    // through open/readBatch on the split-fetcher thread).
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
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
                    conf,
                    tableSchema,
                    requiredSchema,
                    mockInternalSchemaManager,
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
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
                "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorValidatesInternalSchemaManager() {
    // Test that constructor requires non-null InternalSchemaManager
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSplitReaderFunction(
          conf,
          tableSchema,
          requiredSchema,
          null,  // null InternalSchemaManager should throw
          "AVRO_PAYLOAD",
          Collections.emptyList(),
          false
      );
    });
  }

  @Test
  public void testInternalSchemaManagerIsStored() {
    InternalSchemaManager customManager = mock(InternalSchemaManager.class);

    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            requiredSchema,
            customManager,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function, "Function should be created with custom InternalSchemaManager");
  }

  // -------------------------------------------------------------------------
  //  Constructor tests (limit is now enforced in HoodieSourceSplitReader)
  // -------------------------------------------------------------------------

  @Test
  public void testConstructorWithEmitDelete() {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            requiredSchema,
            mockInternalSchemaManager,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        );

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithNullTableSchemaThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        new HoodieSplitReaderFunction(
            conf,
            null,
            requiredSchema,
            mockInternalSchemaManager,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        ));
  }

  @Test
  public void testConstructorWithNullRequiredSchemaThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        new HoodieSplitReaderFunction(
            conf,
            tableSchema,
            null,
            mockInternalSchemaManager,
            "AVRO_PAYLOAD",
            Collections.emptyList(),
            false
        ));
  }

  @Test
  public void testDefaultConstructor() {
    HoodieSplitReaderFunction function =
        new HoodieSplitReaderFunction(
            conf, tableSchema, requiredSchema, mockInternalSchemaManager,
            "AVRO_PAYLOAD", Collections.emptyList(), false);

    assertNotNull(function);
  }

  // -------------------------------------------------------------------------
  //  createRecordIterator — file-group reader cleanup when iterator init fails
  // -------------------------------------------------------------------------

  @Test
  public void testCreateRecordIteratorClosesReaderWhenInitFails() throws Exception {
    // getClosableIterator() runs initRecordIterators(), which can open the reader's I/O resources and
    // then throw. The reader is only a local in createRecordIterator, so the failure path must close
    // it (nothing else would), preserving the original failure as the cause.
    HoodieFileGroupReader<RowData> reader = mockReader();
    IOException initFailure = new IOException("init failed");
    when(reader.getClosableIterator()).thenThrow(initFailure);

    HoodieSplitReaderFunction function = readerFunctionReturning(reader);
    HoodieSourceSplit split = createSplit();

    try (MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(mockMetaClient);

      HoodieIOException thrown =
          assertThrows(HoodieIOException.class, () -> function.createRecordIterator(split));
      assertSame(initFailure, thrown.getCause(), "the original init failure must be the cause");
    }
    verify(reader, times(1)).close();
  }

  @Test
  public void testCreateRecordIteratorSuppressesCloseError() throws Exception {
    // A close() failure on the cleanup path must not mask the init failure: it is attached as a
    // suppressed exception on the original.
    HoodieFileGroupReader<RowData> reader = mockReader();
    IOException initFailure = new IOException("init failed");
    IOException closeFailure = new IOException("close failed");
    when(reader.getClosableIterator()).thenThrow(initFailure);
    doThrow(closeFailure).when(reader).close();

    HoodieSplitReaderFunction function = readerFunctionReturning(reader);
    HoodieSourceSplit split = createSplit();

    try (MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(mockMetaClient);

      HoodieIOException thrown =
          assertThrows(HoodieIOException.class, () -> function.createRecordIterator(split));
      assertSame(initFailure, thrown.getCause());
      assertEquals(1, initFailure.getSuppressed().length, "close failure must be suppressed, not lost");
      assertSame(closeFailure, initFailure.getSuppressed()[0]);
    }
  }

  @SuppressWarnings("unchecked")
  private static HoodieFileGroupReader<RowData> mockReader() {
    return mock(HoodieFileGroupReader.class);
  }

  private HoodieSplitReaderFunction readerFunctionReturning(HoodieFileGroupReader<RowData> reader) {
    return new HoodieSplitReaderFunction(
        conf, tableSchema, requiredSchema, mockInternalSchemaManager,
        "AVRO_PAYLOAD", Collections.emptyList(), false) {
      @Override
      protected HoodieRecordReader<RowData> createRecordReader(HoodieSourceSplit split, HoodieTableMetaClient metaClient) {
        return reader;
      }
    };
  }

  private static HoodieSourceSplit createSplit() {
    return new HoodieSourceSplit(
        1, "base", Option.of(Collections.emptyList()), "/tbl", "/part",
        "read_optimized", "19700101000000000", "file1", Option.empty());
  }
}
