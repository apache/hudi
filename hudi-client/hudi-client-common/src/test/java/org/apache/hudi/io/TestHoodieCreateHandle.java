package org.apache.hudi.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.TestBaseHoodieTable;
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
  private static final Schema TEST_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
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

    FileSystem fs = (FileSystem) metaClient.getStorage().getFileSystem();

    // Verify partition metadata file exists under the target partition
    Path partitionPath = new Path(basePath, TEST_PARTITION_PATH);
    validatePartitionMetaPathExistence(fs, true);

    // Verify marker file exists with expected name
    String expectedBaseFileName = getExpectedBaseFileName();
    validateMarkerFileExistence(fs, expectedBaseFileName, true);

    // Verify the base file path exists
    Path expectedBaseFilePath = new Path(partitionPath, expectedBaseFileName);
    assertTrue(fs.exists(expectedBaseFilePath));
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
  void testNoMarkerFileCreatedWhenFileWriterFails() throws Exception {
    // Create a custom HoodieCreateHandle that simulates file writer initialization failure
    class FailingFileWriterCreateHandle extends HoodieCreateHandle<Object, Object, Object, Object> {
      public FailingFileWriterCreateHandle(HoodieWriteConfig config, String instantTime,
                                           HoodieTable<Object, Object, Object, Object> hoodieTable,
                                           String partitionPath, String fileId,
                                           TaskContextSupplier taskContextSupplier) {
        super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
      }

      @Override
      protected HoodieFileWriter initializeFileWriter() throws IOException {
        throw new IOException("Simulated file writer initialization failure");
      }

      @Override
      protected void createMarkerFile(String partitionPath, String dataFileName) {
        // Track if marker file creation was attempted
        throw new AssertionError("Marker file should not be created when file writer fails");
      }
    }

    // Verify that HoodieInsertException is thrown when file writer initialization fails
    HoodieInsertException exception = assertThrows(HoodieInsertException.class, () ->
      new FailingFileWriterCreateHandle(writeConfig, TEST_INSTANT_TIME, hoodieTable,
        TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier));
    assertEquals("Failed to initialize HoodieStorageWriter for path "
        + String.format("%s/%s/%s", basePath, TEST_PARTITION_PATH, getExpectedBaseFileName()), exception.getMessage());

    FileSystem fs = (FileSystem) metaClient.getStorage().getFileSystem();

    // Verify no partition meta path
    validatePartitionMetaPathExistence(fs, false);

    // Verify that no marker file exists in the file system
    String expectedBaseFileName = getExpectedBaseFileName();
    validateMarkerFileExistence(fs, expectedBaseFileName, false);
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

  private void validateMarkerFileExistence(FileSystem fs, String expectedBaseFileName, boolean shouldExist) throws IOException {
    Path expectedMarkerPath = new Path(new Path(metaClient.getMarkerFolderPath(TEST_INSTANT_TIME),DEFAULT_FIRST_PARTITION_PATH),
        expectedBaseFileName + HoodieTableMetaClient.MARKER_EXTN + "." + IOType.CREATE.name());

    if (shouldExist) {
      assertTrue(fs.exists(expectedMarkerPath));
    } else {
      assertFalse(fs.exists(expectedMarkerPath));
    }
  }

  private void validatePartitionMetaPathExistence(FileSystem fs, boolean shouldExist) throws IOException {
    Path partitionPath = new Path(basePath, DEFAULT_FIRST_PARTITION_PATH);
    Path partitionMetaPath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
    assertEquals(shouldExist, fs.exists(partitionMetaPath));
  }
}
