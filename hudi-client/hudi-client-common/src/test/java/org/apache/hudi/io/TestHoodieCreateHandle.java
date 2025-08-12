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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieCreateHandle extends HoodieCommonTestHarness {

  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_PARTITION_PATH = "partition1";
  private static final String TEST_FILE_ID = "file-001";
  private static final String TEST_RECORD_KEY = "record-key-001";

  @Mock
  private HoodieTable<Object, Object, Object, Object> mockHoodieTable;
  
  private HoodieWriteConfig writeConfig;
  
  private TaskContextSupplier taskContextSupplier;
  
  @Mock
  private HoodieFileWriter mockFileWriter;
  
  @Mock
  private HoodieRecord<Object> mockRecord;
  
  @Mock
  private Schema mockSchema;
  
  @Mock
  private org.apache.hudi.common.engine.HoodieEngineContext mockEngineContext;
  
  @Mock
  private org.apache.hudi.common.table.timeline.HoodieActiveTimeline mockActiveTimeline;
  
  @Mock
  private org.apache.hudi.common.table.HoodieTableMetaClient mockMetaClient;
  
  @Mock
  private org.apache.hudi.common.table.HoodieTableConfig mockTableConfig;
  
  @Mock
  private HoodieStorageLayout mockStorageLayout;

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

  @Test
  public void testConstructorWithBasicParameters() {
    HoodieCreateHandle createHandle = new HoodieTestCreateHandle(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
      
    assertNotNull(createHandle);
    assertEquals(IOType.CREATE, createHandle.getIOType());
  }

  @Test
  public void testConstructorWithPreserveMetadata() {
    HoodieCreateHandle createHandle = new HoodieTestCreateHandle(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier, true);

    assertNotNull(createHandle);
  }

  @Test
  public void testGetIOType() {
    mockMetaClientTimelineMarker();
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    assertEquals(IOType.CREATE, createHandle.getIOType());
  }

  @Test
  public void testConstructorCreatesPartitionMetadataAndMarkerAndBaseFile() throws Exception {
    mockMetaClientTimelineMarker();
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);

    assertNotNull(createHandle);

    FileSystem fs = metaClient.getFs();

    // Verify partition metadata file exists under the target partition
    Path partitionPath = new Path(basePath, TEST_PARTITION_PATH);
    String metaExt = HoodieFileFormat.PARQUET.getFileExtension();
    // Alternative representation of ".hoodie_partition_metadata" + ext
    Path partitionMetaPath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + metaExt);
    assertTrue(fs.exists(partitionMetaPath));

    // Verify marker file exists with expected name
    String writeToken = FSUtils.makeWriteToken(0, 0, 0L);
    String expectedBaseFileName = FSUtils.makeBaseFileName(TEST_INSTANT_TIME, writeToken, TEST_FILE_ID, ".parquet");
    Path expectedMarkerPath = new Path(new Path(metaClient.getMarkerFolderPath(TEST_INSTANT_TIME), TEST_PARTITION_PATH),
        expectedBaseFileName + HoodieTableMetaClient.MARKER_EXTN + "." + IOType.CREATE.name());
    assertTrue(fs.exists(expectedMarkerPath));

    // Verify the base file path exists
    Path expectedBaseFilePath = new Path(partitionPath, expectedBaseFileName);
    assertTrue(fs.exists(expectedBaseFilePath));
  }

  @Test
  public void testCanWriteWhenFileWriterCanWrite() {
    mockMetaClientTimelineMarker();
    when(mockFileWriter.canWrite()).thenReturn(true);
    when(mockRecord.getPartitionPath()).thenReturn(TEST_PARTITION_PATH);

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);

    // Mock the file writer to return true for canWrite
    createHandle.fileWriter = mockFileWriter;
    when(mockFileWriter.canWrite()).thenReturn(true);

    boolean canWrite = createHandle.canWrite(mockRecord);
    assertTrue(canWrite);
  }

  @Test
  public void testCanWriteWhenFileWriterCannotWrite() {
    mockMetaClientTimelineMarker();
    when(mockStorageLayout.determinesNumFileGroups()).thenReturn(false);
    when(mockFileWriter.canWrite()).thenReturn(true);
    when(mockHoodieTable.getStorageLayout()).thenReturn(mockStorageLayout);

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    when(mockFileWriter.canWrite()).thenReturn(false);

    boolean canWrite = createHandle.canWrite(mockRecord);
    assertFalse(canWrite);
  }

  @Test
  public void testEmptyRecordMapNoWrite() {
    mockMetaClientTimelineMarker();
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.recordMap = new HashMap<>();
    
    assertDoesNotThrow(() -> createHandle.write());
  }

  @Test
  public void testWriteWithSingleRecord() {
    mockMetaClientTimelineMarker();
    mockGetOperationAndMetadata();

    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    recordMap.put(TEST_RECORD_KEY, mockRecord);
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, recordMap, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    assertDoesNotThrow(() -> createHandle.write());
    assertEquals(1, createHandle.recordsWritten);
  }

  @Test
  public void testWriteWithSortedRecords() {
    mockMetaClientTimelineMarker();
    when(mockRecord.getMetadata()).thenReturn(Option.of(Collections.emptyMap()));
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    recordMap.put("record-1", mockRecord);
    recordMap.put("record-2", mockRecord);

    when(mockHoodieTable.requireSortedRecords()).thenReturn(true);
    
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, recordMap, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    assertDoesNotThrow(() -> createHandle.write());
    assertEquals(2, createHandle.recordsWritten);
    assertEquals(2, createHandle.writeStatus.getTotalRecords());
  }

  @Test
  public void testDoWriteWithValidRecord() throws Exception {
    mockMetaClientTimelineMarker();
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    when(mockRecord.getMetadata()).thenReturn(Option.of(Collections.emptyMap()));
    recordMap.put("record-1", mockRecord);

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, recordMap, taskContextSupplier);
    
    createHandle.fileWriter = mockFileWriter;
    
    when(mockRecord.isDelete(any(), any())).thenReturn(false);
    when(mockRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(mockRecord.prependMetaFields(any(), any(), any(), any())).thenReturn(mockRecord);

    assertDoesNotThrow(() -> {
      createHandle.doWrite(mockRecord, mockSchema, new TypedProperties());
    });
    
    verify(mockFileWriter).write(eq(null), eq(mockRecord), any(Schema.class));
    verify(mockRecord).unseal();
    verify(mockRecord).setNewLocation(any());
    verify(mockRecord).seal();
    verify(mockRecord).deflate();
  }

  @Test
  public void testDoWriteWithDeletedRecord() throws Exception {
    mockMetaClientTimelineMarker();
    mockGetOperationAndMetadata();

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    when(mockRecord.getOperation()).thenReturn(HoodieOperation.DELETE);

    assertDoesNotThrow(() -> {
      createHandle.doWrite(mockRecord, mockSchema, new TypedProperties());
    });
    assertEquals(0, createHandle.recordsWritten);
    assertEquals(1, createHandle.recordsDeleted);
    verify(mockFileWriter, never()).writeWithMetadata(any(), any(), any());
  }

  @Test
  public void testDoWriteWithIgnoredRecord() throws Exception {
    mockMetaClientTimelineMarker();
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    when(mockRecord.isDelete(any(), any())).thenReturn(false);
    when(mockRecord.shouldIgnore(any(), any())).thenReturn(true);
    assertDoesNotThrow(() ->
        createHandle.doWrite(mockRecord, mockSchema, new TypedProperties()));
    verify(mockFileWriter, never()).writeWithMetadata(any(), any(), any());
  }

  @Test
  public void testDoWriteWithPreserveMetadata() throws Exception {
    mockMetaClientTimelineMarker();
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    when(mockRecord.getMetadata()).thenReturn(Option.of(Collections.emptyMap()));
    recordMap.put("record-1", mockRecord);
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier, true);
    
    createHandle.fileWriter = mockFileWriter;

    when(mockRecord.isDelete(any(), any())).thenReturn(false);
    when(mockRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(mockRecord.prependMetaFields(any(), any(), any(), any())).thenReturn(mockRecord);

    assertDoesNotThrow(() ->
        createHandle.doWrite(mockRecord, mockSchema, new TypedProperties()));

    verify(mockFileWriter).write(eq(null), eq(mockRecord), any(Schema.class));
    assertEquals(1, createHandle.recordsWritten);
  }

  @Test
  public void testDoWriteHandlesException() throws Exception {
    mockMetaClientTimelineMarker();
    mockGetOperationAndMetadata();

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    createHandle.fileWriter = mockFileWriter;
    
    when(mockRecord.isDelete(any(), any())).thenReturn(false);
    when(mockRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(mockRecord.prependMetaFields(any(), any(), any(), any())).thenThrow(new RuntimeException("Test exception"));

    assertDoesNotThrow(() ->
        createHandle.doWrite(mockRecord, mockSchema, new TypedProperties()));
    assertEquals(1, createHandle.writeStatus.getErrors().size());
  }

  @Test
  public void testCloseWhenAlreadyClosed() {
    mockMetaClientTimelineMarker();

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.markClosed();
    
    List<WriteStatus> result = createHandle.close();
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCloseSuccessful() throws IOException {
    mockMetaClientTimelineMarker();

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    List<WriteStatus> result = createHandle.close();
    assertNotNull(result);
    assertEquals(1, result.size());
  }

  @Test
  public void testClosePopulatesWriteStatusPathAndBytes() throws Exception {
    mockMetaClientTimelineMarker();

    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);

    List<WriteStatus> statuses = createHandle.close();
    assertEquals(1, statuses.size());
    WriteStatus ws = statuses.get(0);
    HoodieWriteStat stat = ws.getStat();

    // Validate relative path points to the expected base file
    String writeToken = FSUtils.makeWriteToken(0, 0, 0L);
    String expectedBaseFileName = FSUtils.makeBaseFileName(TEST_INSTANT_TIME, writeToken, TEST_FILE_ID, ".parquet");
    assertEquals(TEST_PARTITION_PATH + "/" + expectedBaseFileName, stat.getPath());

    // Validate file size recorded matches actual size and is > 0
    Path baseFilePath = new Path(basePath, stat.getPath());
    long actualSize = metaClient.getFs().getFileStatus(baseFilePath).getLen();
    assertTrue(actualSize > 0);
    assertEquals(actualSize, stat.getTotalWriteBytes());
  }

  @Test
  public void testCloseWithIOException() throws IOException {
    mockMetaClientTimelineMarker();

    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    createHandle.fileWriter = mockFileWriter;
    doThrow(new IOException("Test IO exception")).when(mockFileWriter).close();
    
    assertThrows(HoodieInsertException.class, () -> createHandle.close());
  }

  @Test
  public void testAbstractCreateHandleInitialization() {
    mockMetaClientTimelineMarker();
    
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    // Verify fields inherited from AbstractHoodieCreateHandle are properly initialized
    assertEquals(0, createHandle.recordsWritten);
    assertEquals(0, createHandle.recordsDeleted);
    assertEquals(0, createHandle.insertRecordsWritten);
    assertFalse(createHandle.useWriterSchema);
    assertNull(createHandle.recordMap);
    assertNotNull(createHandle.path);
    assertNotNull(createHandle.writeStatus);
  }

  @Test
  public void testAbstractCreateHandleWithRecordMap() {
    mockMetaClientTimelineMarker();
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    recordMap.put(TEST_RECORD_KEY, mockRecord);
    
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, recordMap, taskContextSupplier);
    
    // Verify recordMap and useWriterSchema are set correctly
    assertNotNull(createHandle.recordMap);
    assertEquals(1, createHandle.recordMap.size());
    assertTrue(createHandle.useWriterSchema);
  }

  @Test
  public void testCanWriteWithLayoutControlsNumFiles() {
    mockMetaClientTimelineMarker();
    when(mockStorageLayout.determinesNumFileGroups()).thenReturn(true);
    when(mockHoodieTable.getStorageLayout()).thenReturn(mockStorageLayout);
    when(mockFileWriter.canWrite()).thenReturn(false);

    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    // Even though fileWriter.canWrite() returns false, should return true due to layout controls
    boolean canWrite = createHandle.canWrite(mockRecord);
    assertTrue(canWrite);
  }

  @Test
  public void testSetupWriteStatus() {
    mockMetaClientTimelineMarker();

    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    createHandle.recordsWritten = 5;
    createHandle.recordsDeleted = 2;
    createHandle.insertRecordsWritten = 3;
    
    assertDoesNotThrow(() -> createHandle.setupWriteStatus());
    
    HoodieWriteStat stat = createHandle.writeStatus.getStat();
    assertEquals(TEST_PARTITION_PATH, stat.getPartitionPath());
    assertEquals(5, stat.getNumWrites());
    assertEquals(2, stat.getNumDeletes());
    assertEquals(3, stat.getNumInserts());
    assertEquals(TEST_FILE_ID, stat.getFileId());
  }

  @Test
  public void testDoWriteWithVariousOperationTypes() throws Exception {
    mockMetaClientTimelineMarker();
    
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    // Test INSERT operation
    HoodieRecord<Object> insertRecord = mock(HoodieRecord.class);
    when(insertRecord.getOperation()).thenReturn(HoodieOperation.INSERT);
    when(insertRecord.isDelete(any(), any())).thenReturn(false);
    when(insertRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(insertRecord.prependMetaFields(any(), any(), any(), any())).thenReturn(insertRecord);
    when(insertRecord.getMetadata()).thenReturn(Option.empty());

    createHandle.doWrite(insertRecord, mockSchema, new TypedProperties());
    assertEquals(1, createHandle.recordsWritten);
    assertEquals(1, createHandle.insertRecordsWritten);
    assertEquals(0, createHandle.recordsDeleted);

    // Test UPDATE operation
    HoodieRecord<Object> updateRecord = mock(HoodieRecord.class);
    when(updateRecord.getOperation()).thenReturn(HoodieOperation.UPDATE_AFTER);
    when(updateRecord.isDelete(any(), any())).thenReturn(false);
    when(updateRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(updateRecord.prependMetaFields(any(), any(), any(), any())).thenReturn(updateRecord);
    when(updateRecord.getMetadata()).thenReturn(Option.empty());
    
    createHandle.doWrite(updateRecord, mockSchema, new TypedProperties());
    assertEquals(2, createHandle.recordsWritten);
    assertEquals(2, createHandle.insertRecordsWritten);
    assertEquals(0, createHandle.recordsDeleted);

    // Test DELETE operation
    HoodieRecord<Object> deleteRecord = mock(HoodieRecord.class);
    when(deleteRecord.getOperation()).thenReturn(HoodieOperation.DELETE);
    when(deleteRecord.getMetadata()).thenReturn(Option.empty());

    createHandle.doWrite(deleteRecord, mockSchema, new TypedProperties());
    assertEquals(2, createHandle.recordsWritten);
    assertEquals(2, createHandle.insertRecordsWritten);
    assertEquals(1, createHandle.recordsDeleted);
  }

  @Test
  public void testMultipleCloseAttempts() {
    mockMetaClientTimelineMarker();
    
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    // First close should succeed
    List<WriteStatus> firstResult = createHandle.close();
    assertNotNull(firstResult);
    assertEquals(1, firstResult.size());
    
    // Subsequent closes should return empty list
    List<WriteStatus> secondResult = createHandle.close();
    assertTrue(secondResult.isEmpty());
    
    List<WriteStatus> thirdResult = createHandle.close();
    assertTrue(thirdResult.isEmpty());
  }

  @Test
  public void testWriteStatusErrorHandling() throws IOException {
    mockMetaClientTimelineMarker();
    
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    HoodieRecord<Object> faultyRecord = mock(HoodieRecord.class);
    when(faultyRecord.isDelete(any(), any())).thenReturn(false);
    when(faultyRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(faultyRecord.prependMetaFields(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Prepend failed"));
    when(faultyRecord.getMetadata()).thenReturn(Option.empty());
    
    // Should not throw, but should record error
    assertDoesNotThrow(() -> createHandle.doWrite(faultyRecord, mockSchema, new TypedProperties()));
    
    // Verify error was recorded
    assertEquals(1, createHandle.writeStatus.getErrors().size());
    assertEquals(0, createHandle.recordsWritten);
  }

  @Test
  public void testRecordLocationSetting() throws Exception {
    mockMetaClientTimelineMarker();
    
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    HoodieRecord<Object> recordSpy = spy(mockRecord);
    when(recordSpy.isDelete(any(), any())).thenReturn(false);
    when(recordSpy.shouldIgnore(any(), any())).thenReturn(false);
    when(recordSpy.prependMetaFields(any(), any(), any(), any())).thenReturn(recordSpy);
    when(recordSpy.getMetadata()).thenReturn(Option.empty());
    
    createHandle.doWrite(recordSpy, mockSchema, new TypedProperties());
    
    // Verify record location was set
    verify(recordSpy).unseal();
    verify(recordSpy).setNewLocation(any(HoodieRecordLocation.class));
    verify(recordSpy).seal();
    verify(recordSpy).deflate();
  }

  @Test
  public void testWithOverriddenSchema() {
    mockMetaClientTimelineMarker();
    
    Schema overridenSchema = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);
    
    HoodieCreateHandle handleWithOverridenSchema = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, Option.of(overridenSchema), taskContextSupplier);
    assertEquals(overridenSchema, handleWithOverridenSchema.writeSchema);
  }

  @Test
  public void testResourceCleanupOnException() throws IOException {
    mockMetaClientTimelineMarker();
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    HoodieFileWriter mockFailingWriter = mock(HoodieFileWriter.class);
    doThrow(new IOException("Close failed")).when(mockFailingWriter).close();
    createHandle.fileWriter = mockFailingWriter;
    
    HoodieInsertException exception = assertThrows(HoodieInsertException.class, () -> createHandle.close());
    assertTrue(exception.getMessage().contains("Failed to close the Insert Handle"));
  }

  @Test
  public void testWritePerformanceMetrics() throws IOException {
    mockMetaClientTimelineMarker();
    
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      recordMap.put("record-" + i, mockRecord);
    }
    
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, mockHoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, recordMap, taskContextSupplier);
    createHandle.fileWriter = mockFileWriter;
    
    when(mockRecord.getMetadata()).thenReturn(Option.empty());
    when(mockRecord.isDelete(any(), any())).thenReturn(false);
    when(mockRecord.shouldIgnore(any(), any())).thenReturn(false);
    when(mockRecord.prependMetaFields(any(), any(), any(), any())).thenReturn(mockRecord);
    
    // Write records
    createHandle.write();

    // Close and get stats
    List<WriteStatus> statuses = createHandle.close();
    assertEquals(1, statuses.size());
    
    HoodieWriteStat stat = statuses.get(0).getStat();
    assertTrue(stat.getRuntimeStats().getTotalCreateTime() > 0);
    assertEquals(100, stat.getNumWrites());
  }

  /**
   * Test implementation of HoodieCreateHandle that avoids file system operations.
   */
  private static class HoodieTestCreateHandle extends HoodieCreateHandle<Object, Object, Object, Object> {

    public HoodieTestCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<Object, Object, Object, Object> hoodieTable,
                                  String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
      super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
    }

    public HoodieTestCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<Object, Object, Object, Object> hoodieTable,
                                  String partitionPath, String fileId, TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
      super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, preserveMetadata);
    }

    @Override
    protected void createMarkerFile(String partitionPath, String dataFileName) {
      // No-op to avoid file system operations in tests
    }

    @Override
    public List<WriteStatus> close() {
      // Simplified close operation for tests
      if (isClosed()) {
        return Collections.emptyList();
      }
      markClosed();
      return Collections.singletonList(writeStatus);
    }
  }

  private void mockMethodsNeededByConstructor() {
    when(mockHoodieTable.getConfig()).thenReturn(writeConfig);
    when(mockHoodieTable.getBaseFileExtension()).thenReturn(".parquet");
    when(mockHoodieTable.getHadoopConf()).thenReturn(metaClient.getHadoopConf());
    when(mockHoodieTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockHoodieTable.getPartitionMetafileFormat()).thenReturn(Option.of(org.apache.hudi.common.model.HoodieFileFormat.PARQUET));
    when(mockMetaClient.getFs()).thenReturn(metaClient.getFs());
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.getBaseFileFormat()).thenReturn(org.apache.hudi.common.model.HoodieFileFormat.PARQUET);
  }

  private void mockMetaClientTimelineMarker() {
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    when(mockMetaClient.getBasePath()).thenReturn(metaClient.getBasePath());
    when(mockMetaClient.getMarkerFolderPath(TEST_INSTANT_TIME)).thenReturn(metaClient.getBasePath() + "/.hoodie/.temp/" + TEST_INSTANT_TIME);
  }

  private void mockGetOperationAndMetadata() {
    when(mockRecord.getOperation()).thenReturn(HoodieOperation.INSERT);
    when(mockRecord.getMetadata()).thenReturn(Option.empty());
  }
}