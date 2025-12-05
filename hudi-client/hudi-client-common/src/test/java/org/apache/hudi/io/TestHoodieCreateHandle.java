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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.TestFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.TestBaseHoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

@ExtendWith(MockitoExtension.class)
public class TestHoodieCreateHandle extends HoodieCommonTestHarness {
  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_FILE_ID = "file-001";
  private static final HoodieSchema TEST_SCHEMA = HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA);
  private static final String TEST_PARTITION_PATH = DEFAULT_FIRST_PARTITION_PATH;

  private HoodieWriteConfig writeConfig;
  private TaskContextSupplier taskContextSupplier;
  private HoodieTable hoodieTable;

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    initMetaClient(false);
    initTestDataGenerator();

    writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .withSchema(TRIP_EXAMPLE_SCHEMA)
      .withMarkersType("DIRECT")
      .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
      .build();
    hoodieTable = new TestBaseHoodieTable(writeConfig, getEngineContext(), metaClient);
    taskContextSupplier = new LocalTaskContextSupplier();
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanMetaClient();
    cleanupTestDataGenerator();
  }

  @Test
  void testConstructorWithBasicParameters() {
    HoodieCreateHandle createHandle = new HoodieCreateHandle(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);

    assertNotNull(createHandle);
    assertEquals(IOType.CREATE, createHandle.getIOType());
    assertFalse(createHandle.preserveMetadata);
  }

  @Test
  void testConstructorWithPreserveMetadata() {
    HoodieCreateHandle createHandle = new HoodieCreateHandle(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier, true);

    assertNotNull(createHandle);
    assertTrue(createHandle.preserveMetadata);
  }

  @Test
  public void testConstructorWithOverriddenSchema() {
    Schema overridenSchema = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);

    HoodieCreateHandle handleWithOverridenSchema = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, Option.of(overridenSchema), taskContextSupplier);
    assertEquals(overridenSchema, handleWithOverridenSchema.writeSchema);
  }

  @Test
  void testConstructorCreatesPartitionMetadataAndMarkerAndBaseFile() throws Exception {
    HoodieCreateHandle createHandle = new HoodieCreateHandle(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);
    assertNotNull(createHandle);

    HoodieStorage hoodieStorage = metaClient.getStorage();

    // Verify partition metadata file exists under the target partition
    StoragePath partitionPath = new StoragePath(basePath, TEST_PARTITION_PATH);
    validatePartitionMetaPathExistence(hoodieStorage, true);

    // Verify marker file exists with expected name
    String expectedBaseFileName = getExpectedBaseFileName();
    validateMarkerFileExistence(hoodieStorage, expectedBaseFileName, true);

    // Verify the base file path exists
    StoragePath expectedBaseFilePath = new StoragePath(partitionPath, expectedBaseFileName);
    assertTrue(hoodieStorage.exists(expectedBaseFilePath));
  }

  @ParameterizedTest
  @MethodSource("generateParametersForCanWrite")
  void testCanWriteWhenFileWriterCanWrite(String partitionPath, boolean expectedCanWrite) {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, partitionPath,
        TEST_FILE_ID, taskContextSupplier);

    boolean canWrite = createHandle.canWrite(dataGen.generateInserts(TEST_INSTANT_TIME, 1).get(0));
    assertEquals(expectedCanWrite, canWrite);
  }

  private static Stream<Arguments> generateParametersForCanWrite() {
    // partition path, expected canWrite value
    return Stream.of(
      Arguments.of(DEFAULT_FIRST_PARTITION_PATH, true),
      // datagen generates records for each partition in a round-robin fashion so first record is written to first partition path
      Arguments.of(DEFAULT_SECOND_PARTITION_PATH, false)
    );
  }

  @Test
  void testEmptyRecordMapNoWrite() {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);
    createHandle.recordMap = new HashMap<>();

    assertDoesNotThrow(() -> createHandle.write());
    assertEquals(0, createHandle.recordsWritten);
  }

  @Test
  void testWriteSingleRecordFromRecordMap() {
    Map<String, HoodieRecord<Object>> recordMap = new HashMap<>();
    HoodieRecord record = dataGen.generateInserts(TEST_INSTANT_TIME, 1).get(0);
    recordMap.put(record.getRecordKey(), record);
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, recordMap, taskContextSupplier);

    assertDoesNotThrow(() -> createHandle.write());
    assertEquals(1, createHandle.recordsWritten);
  }

  @Test
  void testDoWrite() throws Exception {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);

    // Generate and store records for comparison
    List<HoodieRecord> sourceRecords = dataGen.generateInserts(TEST_INSTANT_TIME, 4);
    for (HoodieRecord record : sourceRecords) {
      createHandle.doWrite(record, TEST_SCHEMA, new TypedProperties());
    }
    List<WriteStatus> writeStatuses = createHandle.close();

    // By default, preserveMetadata is false so written records should contain meta fields
    validateWrittenRecords(writeStatuses, sourceRecords, true);
  }

  @Test
  public void testDoWriteWithPreserveMetadata() throws Exception {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier, true);

    List<HoodieRecord> sourceRecords = dataGen.generateInserts(TEST_INSTANT_TIME, 4);
    for (HoodieRecord record : sourceRecords) {
      createHandle.doWrite(record, TEST_SCHEMA, new TypedProperties());
    }
    List<WriteStatus> writeStatuses = createHandle.close();

    // Since preserveMetadata is true, create handle doesn't populate meta fields
    validateWrittenRecords(writeStatuses, sourceRecords, false);
  }

  @Test
  public void testDoWriteWithDeleteRecord() throws Exception {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);

    List<HoodieRecord> deleteRecords = dataGen.generateDeletes(TEST_INSTANT_TIME, 4);
    for (HoodieRecord deleteRecord : deleteRecords) {
      createHandle.doWrite(deleteRecord, TEST_SCHEMA, new TypedProperties());
    }
    List<WriteStatus> writeStatuses = createHandle.close();
    assertEquals(0, createHandle.recordsWritten);
    assertEquals(4, createHandle.recordsDeleted);

    validateNoRecordWritten(writeStatuses);
  }

  @Test
  public void testDoWriteHandlesException() throws Exception {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);

    HoodieRecord realRecord = dataGen.generateInserts(TEST_INSTANT_TIME, 1).get(0);
    HoodieRecord spyRecord = spy(realRecord);

    // Stub the prependMetaFields method to throw exception
    doThrow(new RuntimeException("Test exception"))
      .when(spyRecord).prependMetaFields(any(), any(), any(), any());

    assertDoesNotThrow(() ->
        createHandle.doWrite(spyRecord, TEST_SCHEMA, new TypedProperties()));
    List<WriteStatus> writeStatuses = createHandle.close();

    assertEquals(1, createHandle.writeStatus.getErrors().size());
    validateNoRecordWritten(writeStatuses);
  }

  @Test
  public void testCloseWhenAlreadyClosed() {
    HoodieCreateHandle createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);
    List<WriteStatus> writeStatuses = createHandle.close();
    assertEquals(1, writeStatuses.size());

    List<WriteStatus> secondCloseOutput = createHandle.close();
    assertEquals(writeStatuses, secondCloseOutput);
  }

  @Test
  public void testCloseWithFileWriterIOException() throws IOException {
    HoodieCreateHandle<Object, Object, Object, Object> createHandle = new HoodieCreateHandle<>(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);

    // Spy on the real fileWriter object that was created
    HoodieFileWriter spyFileWriter = spy(createHandle.fileWriter);
    createHandle.fileWriter = spyFileWriter;
    
    doThrow(new IOException("Test IO exception")).when(spyFileWriter).close();
    assertThrows(HoodieInsertException.class, () -> createHandle.close());
  }

  @Test
  void testMarkerFileCreatedWhenFileWriterInitializationFails() throws Exception {
    // Create a custom HoodieCreateHandle that simulates file writer initialization failure
    class CreateHandleWithFileWriterInitFailure extends HoodieCreateHandle<Object, Object, Object, Object> {
      public CreateHandleWithFileWriterInitFailure(HoodieWriteConfig config, String instantTime,
                                           HoodieTable<Object, Object, Object, Object> hoodieTable,
                                           String partitionPath, String fileId,
                                           TaskContextSupplier taskContextSupplier) {
        super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
      }

      @Override
      protected HoodieFileWriter initializeFileWriter() throws IOException {
        return new TestFileWriter(path, hoodieTable.getStorage(), true);
      }
    }

    // Verify that HoodieInsertException is thrown when file writer initialization fails
    HoodieInsertException exception = assertThrows(HoodieInsertException.class, () ->
      new CreateHandleWithFileWriterInitFailure(writeConfig, TEST_INSTANT_TIME, hoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier));
    assertEquals("Failed to initialize HoodieStorageWriter for path "
        + String.format("%s/%s/%s", basePath, TEST_PARTITION_PATH, getExpectedBaseFileName()), exception.getMessage());

    HoodieStorage hoodieStorage = metaClient.getStorage();

    // Verify partition meta path
    validatePartitionMetaPathExistence(hoodieStorage, true);

    // Verify marker file has been created in the file system
    String expectedBaseFileName = getExpectedBaseFileName();
    validateMarkerFileExistence(hoodieStorage, expectedBaseFileName, true);

    // Verify the actual file was created by the TestFileWriter during initialization
    StoragePath partitionPath = new StoragePath(basePath, TEST_PARTITION_PATH);
    StoragePath expectedFilePath = new StoragePath(partitionPath, expectedBaseFileName);
    assertTrue(hoodieStorage.exists(expectedFilePath), "File should have been created during TestFileWriter initialization");
  }

  @Test
  void testMarkerFileCreatedWhenFileWriterWriteFails() throws Exception {
    // Create a custom HoodieCreateHandle that uses TestFileWriter that succeeds on initialization but fails on write
    class CreateHandleWithFileWriterWriteFailure extends HoodieCreateHandle<Object, Object, Object, Object> {
      public CreateHandleWithFileWriterWriteFailure(HoodieWriteConfig config, String instantTime,
                                                HoodieTable<Object, Object, Object, Object> hoodieTable,
                                                String partitionPath, String fileId,
                                                TaskContextSupplier taskContextSupplier) {
        super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
      }

      @Override
      protected HoodieFileWriter initializeFileWriter() throws IOException {
        return new TestFileWriter(path, hoodieTable.getStorage(), false, true);
      }
    }

    CreateHandleWithFileWriterWriteFailure createHandle = new CreateHandleWithFileWriterWriteFailure(
        writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    
    assertNotNull(createHandle);
    assertNotNull(createHandle.fileWriter);
    assertTrue(createHandle.fileWriter.canWrite());

    HoodieStorage hoodieStorage = metaClient.getStorage();

    // Verify partition metadata file exists
    validatePartitionMetaPathExistence(hoodieStorage, true);

    // Verify marker file exists
    String expectedBaseFileName = getExpectedBaseFileName();
    validateMarkerFileExistence(hoodieStorage, expectedBaseFileName, true);

    // Verify the actual parquet file was created by the TestFileWriter during initialization
    StoragePath partitionPath = new StoragePath(basePath, TEST_PARTITION_PATH);
    StoragePath expectedFilePath = new StoragePath(partitionPath, expectedBaseFileName);
    assertTrue(hoodieStorage.exists(expectedFilePath));

    // Verify that write operations fail as expected
    HoodieRecord testRecord = dataGen.generateInserts(TEST_INSTANT_TIME, 1).get(0);
    IOException writeException = assertThrows(IOException.class, () ->
        createHandle.fileWriter.write("key1", testRecord, TEST_SCHEMA, new Properties()));
    assertEquals("Simulated file writer write failure", writeException.getMessage());
    IOException writeWithMetadataException = assertThrows(IOException.class, () ->
        createHandle.fileWriter.writeWithMetadata(testRecord.getKey(), testRecord, TEST_SCHEMA, new Properties()));
    assertEquals("Simulated file writer write failure", writeWithMetadataException.getMessage());

    assertDoesNotThrow(() -> createHandle.fileWriter.close());
    assertFalse(createHandle.fileWriter.canWrite());
    assertDoesNotThrow(createHandle::close);
  }

  /**
   * Validates that the written records match the original records and that all write statistics are correct.
   */
  private void validateWrittenRecords(
      List<WriteStatus> writeStatuses,
      List<HoodieRecord> originalRecords,
      boolean populateMetaFields) throws IOException {
    assertEquals(1, writeStatuses.size());
    WriteStatus writeStatus = writeStatuses.get(0);

    // Validate WriteStatus properties and statistics
    assertEquals(TEST_FILE_ID, writeStatus.getFileId());
    assertEquals(TEST_PARTITION_PATH, writeStatus.getPartitionPath());
    assertEquals(originalRecords.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());
    assertFalse(writeStatus.hasErrors());
    assertNull(writeStatus.getGlobalError());

    HoodieWriteStat writeStat = writeStatus.getStat();
    assertEquals(originalRecords.size(), writeStat.getNumInserts());
    assertEquals(originalRecords.size(), writeStat.getNumWrites());
    assertEquals(0, writeStat.getNumDeletes());
    assertEquals(0, writeStat.getNumUpdateWrites());
    assertEquals(0, writeStat.getTotalWriteErrors());
    assertEquals(TEST_FILE_ID, writeStat.getFileId());
    assertEquals(TEST_PARTITION_PATH, writeStat.getPartitionPath());

    // Validate file properties
    assertTrue(writeStat.getTotalWriteBytes() > 0);
    assertTrue(writeStat.getFileSizeInBytes() > 0);
    assertEquals(String.format("%s/%s", TEST_PARTITION_PATH, getExpectedBaseFileName()), writeStat.getPath());

    // Read back written records using ParquetUtils and validate content
    StoragePath writtenFilePath = new StoragePath(basePath, writeStat.getPath());
    assertTrue(metaClient.getStorage().exists(writtenFilePath));

    List<GenericRecord> writtenRecords = new ParquetUtils().readAvroRecords(metaClient.getStorage(), writtenFilePath);
    assertEquals(originalRecords.size(), writtenRecords.size());

    // Validate record content matches original data
    for (int i = 0; i < originalRecords.size(); i++) {
      HoodieRecord originalRecord = originalRecords.get(i);
      GenericRecord writtenRecord = writtenRecords.get(i);

      assertNotNull(writtenRecord, "Written record not found for key: " + originalRecord.getRecordKey());

      if (populateMetaFields) {
        // Validate commit time is set
        assertEquals(TEST_INSTANT_TIME,
            writtenRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());

        // Validate commit seq no
        String expectedCommitSeqId = HoodieRecord.generateSequenceId(TEST_INSTANT_TIME,
            taskContextSupplier.getPartitionIdSupplier().get(), i);
        assertEquals(expectedCommitSeqId,
            writtenRecord.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).toString());

        // Validate record key
        assertEquals(originalRecords.get(i).getRecordKey(),
            writtenRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString());

        // Validate the partition path is correctly set
        assertEquals(DEFAULT_PARTITION_PATHS[i % 3],
            writtenRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString());
      } else {
        assertNull(writtenRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
        assertNull(writtenRecord.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
        assertNull(writtenRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        assertNull(writtenRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
      }
      // Filename is always set
      assertEquals(getExpectedBaseFileName(), writtenRecord.get(HoodieRecord.FILENAME_METADATA_FIELD).toString());
    }
  }

  /**
   * Validates that no record was written
   */
  private void validateNoRecordWritten(List<WriteStatus> writeStatuses) throws IOException {
    WriteStatus writeStatus = writeStatuses.get(0);
    StoragePath writtenFilePath = new StoragePath(basePath, writeStatus.getStat().getPath());
    assertTrue(metaClient.getStorage().exists(writtenFilePath));

    List<GenericRecord> writtenRecords = new ParquetUtils().readAvroRecords(metaClient.getStorage(), writtenFilePath);
    assertEquals(0, writtenRecords.size());
  }

  private String getExpectedBaseFileName() {
    String writeToken = FSUtils.makeWriteToken(0, 0, 0L);
    return FSUtils.makeBaseFileName(TEST_INSTANT_TIME, writeToken, TEST_FILE_ID, ".parquet");
  }

  private void validateMarkerFileExistence(HoodieStorage hoodieStorage, String expectedBaseFileName, boolean shouldExist) throws IOException {
    StoragePath expectedMarkerFolder = new StoragePath(metaClient.getMarkerFolderPath(TEST_INSTANT_TIME), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath expectedMarkerPath = new StoragePath(expectedMarkerFolder,
        expectedBaseFileName + HoodieTableMetaClient.MARKER_EXTN + "." + IOType.CREATE.name());

    if (shouldExist) {
      assertTrue(hoodieStorage.exists(expectedMarkerPath));
    } else {
      assertFalse(hoodieStorage.exists(expectedMarkerPath));
      // Validate that marker folder of TEST_INSTANT_TIME is empty
      StoragePath markerFolderPath = new StoragePath(metaClient.getMarkerFolderPath(TEST_INSTANT_TIME));
      if (hoodieStorage.exists(markerFolderPath)) {
        assertTrue(hoodieStorage.listDirectEntries(markerFolderPath).isEmpty(),
            "Marker folder should be empty when marker file should not exist");
      }
    }
  }

  private void validatePartitionMetaPathExistence(HoodieStorage hoodieStorage, boolean shouldExist) throws IOException {
    StoragePath partitionPath = new StoragePath(basePath, DEFAULT_FIRST_PARTITION_PATH);
    StoragePath partitionMetaPath = new StoragePath(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
    assertEquals(shouldExist, hoodieStorage.exists(partitionMetaPath));
  }
}
