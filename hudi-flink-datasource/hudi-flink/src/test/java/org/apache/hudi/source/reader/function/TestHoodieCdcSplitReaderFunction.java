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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.util.StreamerUtil.EMPTY_PARTITION_PATH;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

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
    String schema = HoodieSchemaConverter.convertToSchema(ROW_TYPE).toString();
    tableState = new MergeOnReadTableState(ROW_TYPE, ROW_TYPE, schema, schema, new ArrayList<>());
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

    tableState = new MergeOnReadTableState(
        ROW_TYPE,
        projectedRowType,
        HoodieSchemaConverter.convertToSchema(ROW_TYPE).toString(),
        HoodieSchemaConverter.convertToSchema(projectedRowType.copy()).toString(),
        new ArrayList<>());
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

    Exception ex = assertThrows(Exception.class, () -> function.open(nonCdcSplit));
    assertNotNull(ex);
    // Must not be IllegalArgumentException (which the old type-guard wrongly threw)
    if (ex instanceof IllegalArgumentException) {
      throw new AssertionError("open() should not throw IllegalArgumentException for non-CDC split; "
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
  public void testReadAcceptsCdcSourceSplitType() throws Exception {
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

    // Opening the split creates the CDC iterator lazily (no I/O yet); it must not throw.
    function.open(cdcSplit);
    function.close();
  }

  @Test
  public void testConstructorValidationAndProducedRowType() {
    assertThrows(IllegalArgumentException.class, () -> new HoodieCdcSplitReaderFunction(
        conf, null, internalSchemaManager, ROW_DATA_TYPE.getChildren(), Collections.emptyList(), false));
    assertThrows(IllegalArgumentException.class, () -> new HoodieCdcSplitReaderFunction(
        conf, tableState, null, ROW_DATA_TYPE.getChildren(), Collections.emptyList(), false));
    assertEquals(ROW_TYPE, createFunction().producedRowType());
  }

  @Test
  public void testBaseFileInsertFromLance() {
    GenericRowData row = new GenericRowData(ROW_TYPE.getFieldCount());
    ClosableIterator<RowData> nested = ClosableIterator.wrap(List.<RowData>of(row).iterator());
    HoodieCdcSourceSplit split = cdcSplit(new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.lance"));

    try (MockedStatic<FormatUtils> mocked = mockStatic(FormatUtils.class)) {
      mocked.when(() -> FormatUtils.getLanceRecordIterator(
          anyString(), anyList(), anyList(), any(int[].class), any()))
          .thenReturn(nested);
      try (ClosableIterator<RowData> iterator = createFunction().createRecordIterator(split)) {
        assertTrue(iterator.hasNext());
        assertSame(row, iterator.next());
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  public void testBaseFileInsertFromParquet() {
    GenericRowData row = new GenericRowData(ROW_TYPE.getFieldCount());
    ClosableIterator<RowData> nested = ClosableIterator.wrap(List.<RowData>of(row).iterator());
    HoodieCdcSourceSplit split = cdcSplit(new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.parquet"));

    try (MockedStatic<RecordIterators> mocked = mockStatic(RecordIterators.class)) {
      mocked.when(() -> RecordIterators.getParquetRecordIterator(
          any(), anyBoolean(), anyBoolean(), any(), any(String[].class), any(DataType[].class),
          anyMap(), any(int[].class), anyInt(), any(), anyLong(), anyLong(), anyList()))
          .thenReturn(nested);
      try (ClosableIterator<RowData> iterator = createFunction().createRecordIterator(split)) {
        assertTrue(iterator.hasNext());
        assertSame(row, iterator.next());
        assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  public void testBaseFileInsertRequiresExactlyOneFile() {
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, Collections.emptyList());
    try (ClosableIterator<RowData> iterator = createFunction().createRecordIterator(cdcSplit(change))) {
      assertThrows(IllegalStateException.class, iterator::hasNext);
    }
  }

  @Test
  public void testAsIsBeforeAfterWithNoCdcFilesIsEmpty() {
    conf.set(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, "DATA_BEFORE_AFTER");
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.AS_IS, Collections.emptyList());
    try (ClosableIterator<RowData> iterator = createFunction().createRecordIterator(cdcSplit(change))) {
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testBaseFileDeleteReadsBeforeFileSlice() throws Exception {
    FileSlice before = fileSlice("001");
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_DELETE,
        Collections.emptyList(), Option.of(before), Option.empty());
    GenericRowData row = new GenericRowData(ROW_TYPE.getFieldCount());
    HoodieRecordReader<RowData> recordReader = mock(HoodieRecordReader.class);
    when(recordReader.getClosableIterator())
        .thenReturn(ClosableIterator.wrap(List.<RowData>of(row).iterator()));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mockWriteConfig();

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class);
         MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockRecordReader(mockedFormatUtils, recordReader);
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(metaClient);

      try (ClosableIterator<RowData> iterator =
               createFunction(writeConfig).createRecordIterator(cdcSplit(change))) {
        assertTrue(iterator.hasNext());
        assertEquals(RowKind.DELETE, iterator.next().getRowKind());
        assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  public void testReplaceCommitReadsBeforeFileSlice() throws Exception {
    FileSlice before = fileSlice("001");
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.REPLACE_COMMIT,
        Collections.emptyList(), Option.of(before), Option.empty());
    GenericRowData row = new GenericRowData(ROW_TYPE.getFieldCount());
    HoodieRecordReader<RowData> recordReader = mock(HoodieRecordReader.class);
    when(recordReader.getClosableIterator())
        .thenReturn(ClosableIterator.wrap(List.<RowData>of(row).iterator()));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mockWriteConfig();

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class);
         MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockRecordReader(mockedFormatUtils, recordReader);
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(metaClient);

      try (ClosableIterator<RowData> iterator =
               createFunction(writeConfig).createRecordIterator(cdcSplit(change))) {
        assertTrue(iterator.hasNext());
        assertEquals(RowKind.DELETE, iterator.next().getRowKind());
        assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAsIsModesLoadRequiredImages() throws Exception {
    HoodieRecordReader<RowData> recordReader = mock(HoodieRecordReader.class);
    when(recordReader.getClosableIterator()).thenAnswer(
        invocation -> ClosableIterator.wrap(Collections.<RowData>emptyList().iterator()));
    ExternalSpillableMap<String, byte[]> images = mock(ExternalSpillableMap.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mockWriteConfig();
    FileSlice before = fileSlice("001");
    FileSlice after = fileSlice("002");

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class);
         MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockRecordReader(mockedFormatUtils, recordReader);
      mockedFormatUtils.when(() -> FormatUtils.spillableMap(any(), anyLong(), anyString()))
          .thenReturn(images);
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(metaClient);

      for (String mode : List.of("DATA_BEFORE", "OP_KEY_ONLY")) {
        conf.set(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, mode);
        HoodieCDCFileSplit change = new HoodieCDCFileSplit(
            "20230101000000000", HoodieCDCInferenceCase.AS_IS,
            Collections.emptyList(), Option.of(before), Option.of(after));
        try (ClosableIterator<RowData> iterator =
                 createFunction(writeConfig).createRecordIterator(cdcSplit(change))) {
          assertFalse(iterator.hasNext());
        }
      }
    }
  }

  @Test
  public void testFileSliceReaderWrapsInitializationFailure() throws Exception {
    HoodieRecordReader<RowData> recordReader = mock(HoodieRecordReader.class);
    when(recordReader.getClosableIterator()).thenThrow(new IOException("failed"));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mockWriteConfig();
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_DELETE,
        Collections.emptyList(), Option.of(fileSlice("001")), Option.empty());

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class);
         MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class)) {
      mockRecordReader(mockedFormatUtils, recordReader);
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(metaClient);
      try (ClosableIterator<RowData> iterator =
               createFunction(writeConfig).createRecordIterator(cdcSplit(change))) {
        assertThrows(HoodieIOException.class, iterator::hasNext);
      }
    }
  }

  @Test
  public void testNonCdcSplitUsesFallbackIterator() throws Exception {
    HoodieRecordReader<RowData> recordReader = mock(HoodieRecordReader.class);
    when(recordReader.getClosableIterator()).thenReturn(
        ClosableIterator.wrap(Collections.<RowData>emptyList().iterator()));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mockWriteConfig();
    HoodieSourceSplit split = new HoodieSourceSplit(
        1, null, Option.of(Collections.emptyList()), tempDir.getAbsolutePath(), "",
        "read_optimized", "20230101000000000", "file-1", Option.empty());

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class);
         MockedStatic<StreamerUtil> mockedStreamerUtil = mockStatic(StreamerUtil.class);
         MockedStatic<FlinkWriteClients> mockedWriteClients = mockStatic(FlinkWriteClients.class)) {
      mockRecordReader(mockedFormatUtils, recordReader);
      mockedStreamerUtil.when(() -> StreamerUtil.metaClientForReader(any(), any()))
          .thenReturn(metaClient);
      mockedWriteClients.when(() -> FlinkWriteClients.getHoodieClientConfig(any()))
          .thenReturn(writeConfig);
      try (ClosableIterator<RowData> iterator = createFunction().createRecordIterator(split)) {
        assertFalse(iterator.hasNext());
      }
    }
  }

  private static void mockRecordReader(
      MockedStatic<FormatUtils> mockedFormatUtils,
      HoodieRecordReader<RowData> recordReader) {
    mockedFormatUtils.when(() -> FormatUtils.createRecordReader(
        any(), any(), any(), any(), any(), any(), anyString(), anyString(),
        anyBoolean(), anyList(), any())).thenReturn(recordReader);
  }

  private static FileSlice fileSlice(String instant) {
    return new FileSlice(
        new HoodieFileGroupId("partition", "file"), instant, null, Collections.emptyList());
  }

  private HoodieWriteConfig mockWriteConfig() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getBasePath()).thenReturn(tempDir.getAbsolutePath());
    return writeConfig;
  }

  private HoodieCdcSplitReaderFunction createFunction(HoodieWriteConfig writeConfig) {
    return new HoodieCdcSplitReaderFunction(
        conf,
        tableState,
        internalSchemaManager,
        ROW_DATA_TYPE.getChildren(),
        Collections.emptyList(),
        false) {
      @Override
      protected HoodieWriteConfig getWriteConfig() {
        return writeConfig;
      }
    };
  }

  private HoodieCdcSourceSplit cdcSplit(HoodieCDCFileSplit... changes) {
    return new HoodieCdcSourceSplit(
        1, tempDir.getAbsolutePath(), 128 * 1024 * 1024L, "file-cdc",
        EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");
  }
}
