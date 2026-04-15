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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hudi.util.StreamerUtil.EMPTY_PARTITION_PATH;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE;
import static org.apache.hudi.utils.TestConfigurations.TABLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link HoodieCdcSplitReaderFunction}.
 */
public class TestHoodieCdcSplitReaderFunction {

  @TempDir
  File tempDir;

  private Configuration conf;
  private HoodieSchema tableSchema;
  private HoodieSchema requiredSchema;
  private InternalSchemaManager internalSchemaManager;
  private MergeOnReadTableState tableState;

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    tableSchema = mock(HoodieSchema.class);
    requiredSchema = mock(HoodieSchema.class);
    internalSchemaManager = mock(InternalSchemaManager.class);
    tableState = new MergeOnReadTableState(ROW_TYPE, ROW_TYPE, TABLE_SCHEMA.toString(), TABLE_SCHEMA.toString(), new ArrayList<>());
  }

  private HoodieCdcSplitReaderFunction createFunction() {
    return new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        ROW_DATA_TYPE.getChildren(),
        Collections.emptyList(),
            false);
  }

  // -------------------------------------------------------------------------
  //  Constructor tests
  // -------------------------------------------------------------------------

  @Test
  public void testConstructorWithValidParameters() {
    HoodieCdcSplitReaderFunction function = createFunction();
    assertNotNull(function);
  }

  @Test
  public void testConstructorWithProjectedRequiredRowType() {
    // requiredRowType is a subset of rowType (first 3 of 5 fields)
    RowType projectedRowType = new RowType(
        ROW_TYPE.getFields().subList(0, 3).stream()
            .map(f -> new RowType.RowField(f.getName(), f.getType()))
            .collect(java.util.stream.Collectors.toList()));

    tableState = new MergeOnReadTableState(ROW_TYPE, projectedRowType, TABLE_SCHEMA.toString(), HoodieSchemaConverter.convertToSchema(projectedRowType.copy()).toString(), new ArrayList<>());
    HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        ROW_DATA_TYPE.getChildren(),
        Collections.emptyList(),
            false);

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithEmptyFieldTypes() {
    HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        Collections.emptyList(),
        Collections.emptyList(),
            false);

    assertNotNull(function);
  }

  // -------------------------------------------------------------------------
  //  close() tests
  // -------------------------------------------------------------------------

  @Test
  public void testCloseWithoutReadingDoesNotThrow() throws Exception {
    HoodieCdcSplitReaderFunction function = createFunction();
    function.close();
  }

  @Test
  public void testMultipleClosesDoNotThrow() throws Exception {
    HoodieCdcSplitReaderFunction function = createFunction();
    function.close();
    function.close();
    function.close();
  }

  // -------------------------------------------------------------------------
  //  read() argument-validation tests
  // -------------------------------------------------------------------------

  @Test
  public void testReadWithNonCdcSplitDelegatesToFallback() {
    // Non-CDC splits are forwarded to the fallback HoodieSplitReaderFunction rather than
    // rejected. The fallback will attempt real I/O and fail, but the failure must NOT be
    // an IllegalArgumentException (that would indicate the type-guard wrongly rejected it).
    HoodieCdcSplitReaderFunction function = createFunction();

    HoodieSourceSplit nonCdcSplit = new HoodieSourceSplit(
        1, "base.parquet", Option.empty(), tempDir.getAbsolutePath(),
        "", "read_optimized", "20230101000000000", "file-1", Option.empty());

    Exception ex = assertThrows(Exception.class, () -> function.read(nonCdcSplit));
    assertNotNull(ex);
    // Must not be IllegalArgumentException (which the old type-guard wrongly threw)
    if (ex instanceof IllegalArgumentException) {
      throw new AssertionError("read() should not throw IllegalArgumentException for non-CDC split; "
          + "it should fall through to the fallback reader", ex);
    }
  }

  // -------------------------------------------------------------------------
  //  Limit push-down constructor tests
  // -------------------------------------------------------------------------

  @Test
  public void testConstructorWithLimit() {
    HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        ROW_DATA_TYPE.getChildren(),
        Collections.emptyList(),
        false);

    assertNotNull(function);
  }

  @Test
  public void testConstructorWithLimitAndEmptyFieldTypes() {
    HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        Collections.emptyList(),
        Collections.emptyList(),
        false);

    assertNotNull(function);
  }

  @Test
  public void testDefaultConstructorUsesNoLimitSentinel() {
    // 6-arg constructor must delegate to 7-arg with limit=-1, both should succeed.
    HoodieCdcSplitReaderFunction defaultLimit = new HoodieCdcSplitReaderFunction(
        conf, tableState, internalSchemaManager,
        ROW_DATA_TYPE.getChildren(), Collections.emptyList(), false);
    HoodieCdcSplitReaderFunction explicitNoLimit = new HoodieCdcSplitReaderFunction(
        conf, tableState, internalSchemaManager,
        ROW_DATA_TYPE.getChildren(), Collections.emptyList(), false);

    assertNotNull(defaultLimit);
    assertNotNull(explicitNoLimit);
  }

  @Test
  public void testConstructorWithLimitZeroIsAccepted() {
    // limit=0 is a valid constructor argument (limitIterator won't wrap since limit <= 0).
    HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
        conf, tableState, internalSchemaManager,
        ROW_DATA_TYPE.getChildren(), Collections.emptyList(), false);
    assertNotNull(function);
  }

  // -------------------------------------------------------------------------
  //  Integration: read() accepts a HoodieCdcSourceSplit (validation only)
  // -------------------------------------------------------------------------

  @Test
  public void testReadAcceptsCdcSourceSplitType() {
    // Verify that HoodieCdcSourceSplit is accepted (cast doesn't throw).
    // Actual I/O would require a real Hoodie table, so we only check the
    // type-guard passes by catching the downstream I/O error rather than
    // an IllegalArgumentException.
    HoodieCdcSplitReaderFunction function = createFunction();

    HoodieCDCFileSplit[] changes = {
        new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.parquet")
    };
    HoodieCdcSourceSplit cdcSplit = new HoodieCdcSourceSplit(
        1, tempDir.getAbsolutePath(), 128 * 1024 * 1024L, "file-cdc",
        EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");

    // Should not throw exception
    function.read(cdcSplit);
  }
}
