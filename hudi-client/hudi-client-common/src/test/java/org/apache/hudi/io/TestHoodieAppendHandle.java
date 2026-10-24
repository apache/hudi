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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieAppendHandle extends HoodieCommonTestHarness {

  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_PARTITION_PATH = "partition1";
  private static final String TEST_FILE_ID = "file-001";

  @Mock
  private HoodieTable<Object, Object, Object, Object> mockHoodieTable;

  @Mock
  private RecordContext<?> recordContext;

  private HoodieWriteConfig writeConfig;

  private TaskContextSupplier taskContextSupplier;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initMetaClient();

    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMarkersType("DIRECT")
        .build();
    taskContextSupplier = new LocalTaskContextSupplier();

    mockMethodsNeededByConstructor();
  }

  @AfterEach
  public void cleanUp() {
    cleanMetaClient();
  }

  private void mockMethodsNeededByConstructor() {
    when(mockHoodieTable.getConfig()).thenReturn(writeConfig);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    doReturn(recordContext).when(mockHoodieTable).getRecordContextForWrite();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testFailedFlushClosesWriterAndPreventsAnotherFlush(boolean closeFails) throws IOException {
    writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(writeConfig.getProps())
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .build();
    when(mockHoodieTable.getStorage()).thenReturn(metaClient.getStorage());
    HoodieInlineLogAppendHandle<Object, Object, Object, Object> handle = spy(
        new HoodieInlineLogAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
            TEST_PARTITION_PATH, TEST_FILE_ID, null, taskContextSupplier));
    HoodieLogFormat.Writer writer = mock(HoodieLogFormat.Writer.class);
    handle.writer = writer;
    handle.recordItr = Collections.emptyIterator();
    RuntimeException failure = new IllegalStateException("flush failed");
    RuntimeException closeFailure = new IllegalStateException("close failed");
    doThrow(failure).when(handle).flushAppend();
    if (closeFails) {
      doThrow(closeFailure).when(writer).close();
    }

    assertSame(failure, assertThrows(IllegalStateException.class, handle::doAppend));
    assertTrue(handle.isClosed());
    assertNull(handle.writer);
    assertNull(handle.recordItr);
    assertArrayEquals(closeFails ? new Throwable[] {closeFailure} : new Throwable[0], failure.getSuppressed());
    assertDoesNotThrow(handle::close);
    verify(handle, times(1)).flushAppend();
    verify(writer, times(1)).close();
  }

  @ParameterizedTest
  @CsvSource({"false,EVENT_TIME_ORDERING", "true,EVENT_TIME_ORDERING", "false,COMMIT_TIME_ORDERING", "true,COMMIT_TIME_ORDERING"})
  void testInlineDeleteConvertsOrderingThroughRecordContext(boolean logCompaction, RecordMergeMode mergeMode) {
    metaClient.getTableConfig().setValue(HoodieTableConfig.RECORD_MERGE_MODE, mergeMode.name());
    writeConfig.setValue(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    List<String> converterFields = mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Collections.emptyList() : Collections.singletonList("ts");
    writeConfig = HoodieWriteConfig.newBuilder().withProps(writeConfig.getProps()).withWriteRecordPositionsEnabled(false).build();
    when(mockHoodieTable.getStorage()).thenReturn(metaClient.getStorage());
    RecordContext<?> context = recordContext;
    when(context.convertOrderingValueToJava("engine")).thenReturn("java");
    HoodieInlineLogAppendHandle<Object, Object, Object, Object> handle = logCompaction
        ? new HoodieInlineLogAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
            TEST_PARTITION_PATH, TEST_FILE_ID, null, taskContextSupplier, Collections.emptyMap())
        : new HoodieInlineLogAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
            TEST_PARTITION_PATH, TEST_FILE_ID, null, taskContextSupplier);
    HoodieRecord<Object> record = spy(new HoodieEmptyRecord<>(new HoodieKey("key", TEST_PARTITION_PATH),
        HoodieOperation.DELETE, "engine", HoodieRecord.HoodieRecordType.SPARK));
    HoodieSchema schema = HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA);

    verify(mockHoodieTable, times(1)).getRecordContextForWrite();
    verify(context, times(1)).initOrderingValueConverter(handle.getWriterSchemaWithMetaFields(), converterFields);

    handle.writeDelete(schema, record);
    handle.writeDelete(schema, record);

    verify(record, times(2)).getOrderingValue(schema, handle.recordProperties, converterFields.toArray(new String[0]));
    assertEquals("java", handle.recordsToDeleteWithPositions.get(0).getLeft().getOrderingValue());
    assertEquals("engine", record.getOrderingValue(schema, writeConfig.getProps(), new String[] {"ts"}));
    verify(mockHoodieTable, times(1)).getRecordContextForWrite();
    verify(context, times(1)).initOrderingValueConverter(handle.getWriterSchemaWithMetaFields(), converterFields);
    verify(context, times(2)).convertOrderingValueToJava("engine");

    RecordContext<?> readerContext = mock(RecordContext.class);
    new HoodieInlineLogAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, null, taskContextSupplier, false, readerContext);
    verify(readerContext, times(1)).initOrderingValueConverter(handle.getWriterSchemaWithMetaFields(), converterFields);
  }

  private static Stream<Arguments> versionsSixAndAbove() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX),
        Arguments.of(HoodieTableVersion.EIGHT),
        Arguments.of(HoodieTableVersion.NINE)
    );
  }

  @ParameterizedTest
  @MethodSource("versionsSixAndAbove")
  void testCreateLogFileWriterLogVersion(HoodieTableVersion tableVersion) throws IOException {
    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMarkersType("DIRECT")
        .withWriteTableVersion(tableVersion.versionCode())
        .build();

    storage = HoodieStorageUtils.getStorage(basePath, metaClient.getStorageConf());

    when(mockHoodieTable.getStorage()).thenReturn(storage);
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      SyncableFileSystemView mockedFSView = mock(SyncableFileSystemView.class);
      when(mockHoodieTable.getHoodieView()).thenReturn(mockedFSView);
      when(mockedFSView.getLatestBaseFile(TEST_PARTITION_PATH, TEST_FILE_ID)).thenReturn(Option.empty());
    }

    HoodieInlineLogAppendHandle<Object, Object, Object, Object> appendHandle =
        new HoodieInlineLogAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable, TEST_PARTITION_PATH, TEST_FILE_ID, null, taskContextSupplier);

    FileSlice mockFileSlice = mock(FileSlice.class);
    if (tableVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      when(mockFileSlice.getLatestLogFile()).thenReturn(Option.empty());
    }

    // verify writer log version is 1 when there are no log files present
    try (HoodieLogFormat.Writer writer = appendHandle.createLogWriter(TEST_INSTANT_TIME, Option.of(mockFileSlice))) {
      assertEquals(1, writer.getLogFile().getLogVersion());
    }

    HoodieLogFile mockLogFile = mock(HoodieLogFile.class);
    if (tableVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      when(mockLogFile.getLogVersion()).thenReturn(1);
      when(mockFileSlice.getLatestLogFile()).thenReturn(Option.of(mockLogFile));
    }

    // verify writer log version is incremented (latest + 1) for pre-v8 tables when a log file is
    // present; v8+ derives the log file name differently and is unaffected.
    try (HoodieLogFormat.Writer writer = appendHandle.createLogWriter(TEST_INSTANT_TIME, Option.of(mockFileSlice))) {
      assertEquals(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 1 : 2, writer.getLogFile().getLogVersion());
    }
  }
}
