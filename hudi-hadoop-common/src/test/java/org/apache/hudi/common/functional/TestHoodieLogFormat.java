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

package org.apache.hudi.common.functional;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.SerializableIndexedRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFileReader;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.TestLogReaderUtils;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HadoopMapRedUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.CorruptedLogFileException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordLocation.INVALID_POSITION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.NULL_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getJavaVersion;
import static org.apache.hudi.common.testutils.HoodieTestUtils.shouldUseExternalHdfs;
import static org.apache.hudi.common.testutils.HoodieTestUtils.useExternalHdfs;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

/**
 * Tests hoodie log format {@link HoodieLogFormat}.
 */
@SuppressWarnings("Duplicates")
public class TestHoodieLogFormat extends HoodieCommonTestHarness {

  private static final HoodieLogBlockType DEFAULT_DATA_BLOCK_TYPE = HoodieLogBlockType.AVRO_DATA_BLOCK;
  private static final int BUFFER_SIZE = 4096;

  private static HdfsTestService hdfsTestService;
  private static HoodieStorage storage;
  private StoragePath partitionPath;
  private String spillableBasePath;

  @BeforeAll
  public static void setUpClass() throws IOException {
    if (shouldUseExternalHdfs()) {
      storage = new HoodieHadoopStorage(useExternalHdfs());
    } else {
      // Append is not supported in LocalFileSystem. HDFS needs to be setup.
      hdfsTestService = new HdfsTestService();
      storage = new HoodieHadoopStorage(hdfsTestService.start(true).getFileSystem());
    }
  }

  @AfterAll
  public static void tearDownClass() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws IOException, InterruptedException {
    Path workDir = ((FileSystem) storage.getFileSystem()).getWorkingDirectory();
    basePath =
        new StoragePath(workDir.toString(),
            testInfo.getDisplayName() + System.currentTimeMillis()).toString();
    partitionPath = new StoragePath(basePath, "partition_path");
    spillableBasePath = new StoragePath(workDir.toString(), ".spillable_path").toString();
    assertTrue(storage.createDirectory(partitionPath));
    HoodieTestUtils.init(storage.getConf().newInstance(), basePath, HoodieTableType.MERGE_ON_READ);
  }

  @AfterEach
  public void tearDown() throws IOException {
    storage.deleteDirectory(new StoragePath(basePath));
    storage.deleteDirectory(partitionPath);
    storage.deleteDirectory(new StoragePath(spillableBasePath));
  }

  @Test
  public void testHoodieLogBlockTypeIsDataOrDeleteBlock() {
    List<HoodieLogBlock.HoodieLogBlockType> dataOrDeleteBlocks = new ArrayList<>();
    dataOrDeleteBlocks.add(HoodieLogBlockType.DELETE_BLOCK);
    dataOrDeleteBlocks.add(HoodieLogBlockType.AVRO_DATA_BLOCK);
    dataOrDeleteBlocks.add(HoodieLogBlockType.PARQUET_DATA_BLOCK);
    dataOrDeleteBlocks.add(HoodieLogBlockType.HFILE_DATA_BLOCK);
    dataOrDeleteBlocks.add(HoodieLogBlockType.CDC_DATA_BLOCK);

    Arrays.stream(HoodieLogBlockType.values()).forEach(logBlockType -> {
      assertEquals(dataOrDeleteBlocks.contains(logBlockType), logBlockType.isDataOrDeleteBlock());
    });
  }

  @Test
  public void testEmptyLog() throws IOException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    assertEquals(0, writer.getCurrentSize(), "Just created this log, size should be 0");
    assertTrue(writer.getLogFile().getFileName().startsWith("."), "Check all log files should start with a .");
    assertEquals(1, writer.getLogFile().getLogVersion(), "Version should be 1 for new log created");
    writer.close();
  }

  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "HFILE_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  public void testBasicAppend(HoodieLogBlockType dataBlockType) throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    long pos = writer.getCurrentSize();
    HoodieDataBlock dataBlock = getDataBlock(dataBlockType, records, header);
    AppendResult result = writer.appendBlock(dataBlock);

    long size = writer.getCurrentSize();
    assertTrue(size > 0, "We just wrote a block - size should be > 0");
    assertEquals(size, storage.getPathInfo(writer.getLogFile().getPath()).getLength(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    assertEquals(size, result.size());
    assertEquals(writer.getLogFile(), result.logFile());
    assertEquals(0, result.offset());
    writer.close();
  }

  @Test
  public void testRollover() throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    // Write out a block
    AppendResult firstAppend = writer.appendBlock(dataBlock);
    // Get the size of the block
    long size = writer.getCurrentSize();
    writer.close();

    assertEquals(0, firstAppend.offset());
    assertEquals(size, firstAppend.size());

    // Create a writer with the size threshold as the size we just wrote - so this has to roll
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage)
            .withSizeThreshold(size - 1).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    AppendResult secondAppend = writer.appendBlock(dataBlock);

    assertNotEquals(firstAppend.logFile(), secondAppend.logFile(), "A log file should include at most one data block");
    assertEquals(0, secondAppend.offset());
    assertEquals(0, writer.getCurrentSize(), "This should be a new log file and hence size should be 0");
    assertEquals(3, writer.getLogFile().getLogVersion(), "Version should be rolled to 3");
    StoragePath logFilePath = writer.getLogFile().getPath();
    assertFalse(storage.exists(logFilePath), "Path (" + logFilePath + ") must not exist");

    // Write one more block, which should not go to the new log file.
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    AppendResult rolloverAppend = writer.appendBlock(dataBlock);

    assertNotEquals(secondAppend.logFile(), rolloverAppend.logFile());
    assertEquals(0, rolloverAppend.offset());
    writer.close();
  }

  @Test
  public void testConcurrentAppendOnExistingLogFileWithoutWriteToken() throws Exception {
    testConcurrentAppend(true, false);
  }

  @Test
  public void testConcurrentAppendOnExistingLogFileWithWriteToken() throws Exception {
    testConcurrentAppend(true, true);
  }

  @Test
  public void testConcurrentAppendOnFirstLogFileVersion() throws Exception {
    testConcurrentAppend(false, true);
  }

  private void testConcurrentAppend(boolean logFileExists, boolean newLogFileFormat) throws Exception {
    HoodieLogFormat.WriterBuilder builder1 =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .withInstantTime("100").withStorage(storage);
    HoodieLogFormat.WriterBuilder builder2 =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .withInstantTime("100").withStorage(storage);

    if (newLogFileFormat && logFileExists) {
      // Assume there is an existing log-file with write token
      builder1 =
          builder1.withLogVersion(1).withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
      builder2 =
          builder2.withLogVersion(1).withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    } else if (newLogFileFormat) {
      // First log file of the file-slice
      builder1 = builder1.withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
          .withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
      builder2 = builder2.withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
          .withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    } else {
      builder1 = builder1.withLogVersion(1).withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    }
    Writer writer = builder1.build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    Writer writer2 = builder2.build();
    writer2.appendBlock(dataBlock);
    HoodieLogFile logFile1 = writer.getLogFile();
    HoodieLogFile logFile2 = writer2.getLogFile();
    writer.close();
    writer2.close();
    assertNotNull(logFile1.getLogWriteToken());
    assertEquals(logFile1.getLogVersion(), logFile2.getLogVersion() - 1, "Log Files must have different versions");
  }

  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "HFILE_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  public void testMultipleAppend(HoodieLogBlockType dataBlockType) throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withLogVersion(1).withInstantTime("100")
            .withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(dataBlockType, records, header);
    writer.appendBlock(dataBlock);
    long size1 = writer.getCurrentSize();
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withLogVersion(1).withInstantTime("100")
            .withStorage(storage).build();
    ((HoodieLogFormatWriter) writer).withOutputStream((FSDataOutputStream)
        storage.append(writer.getLogFile().getPath()));
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(dataBlockType, records, header);
    writer.appendBlock(dataBlock);
    long size2 = writer.getCurrentSize();
    assertTrue(size2 > size1, "We just wrote a new block - size2 should be > size1");
    assertEquals(size2, storage.getPathInfo(writer.getLogFile().getPath()).getLength(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withLogVersion(1).withInstantTime("100")
            .withStorage(storage).build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(dataBlockType, records, header);
    writer.appendBlock(dataBlock);
    long size3 = writer.getCurrentSize();
    assertTrue(size3 > size2, "We just wrote a new block - size3 should be > size2");
    assertEquals(size3, storage.getPathInfo(writer.getLogFile().getPath()).getLength(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    writer.close();

    // Cannot get the current size after closing the log
    final Writer closedWriter = writer;
    assertThrows(IllegalStateException.class, closedWriter::getCurrentSize, "getCurrentSize should fail after the logAppender is closed");
  }

  @Test
  public void testAppendNotSupported(@TempDir java.nio.file.Path tempDir) throws IOException, URISyntaxException, InterruptedException {
    // Use some fs like LocalFileSystem, that does not support appends
    StoragePath localTempDir = new StoragePath(tempDir.toUri().toString());
    HoodieStorage localStorage = HoodieStorageUtils.getStorage(
        localTempDir.toString(), HoodieTestUtils.getDefaultStorageConf());
    assertTrue(localStorage.getFileSystem() instanceof LocalFileSystem);
    StoragePath testPath = new StoragePath(localTempDir, "append_test");
    localStorage.createDirectory(testPath);

    // Some data & append two times.
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 5);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);

    for (int i = 0; i < 2; i++) {
      Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(testPath)
          .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION).withFileId("commits")
          .withInstantTime("")
          .withStorage(localStorage).build();
      writer.appendBlock(dataBlock);
      writer.close();
    }

    // ensure there are two log file versions, with same data.
    List<StoragePathInfo> logFileList = localStorage.listDirectEntries(testPath);
    assertEquals(2, logFileList.size());
  }

  @ParameterizedTest
  @ValueSource(ints = {6, 8})
  public void testBasicWriteAndScan(int tableVersion) throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withTableVersion(HoodieTableVersion.fromVersionCode(tableVersion))
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords = records.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "We wrote a block, we should be able to read it");
    HoodieLogBlock nextBlock = reader.next();
    assertEquals(DEFAULT_DATA_BLOCK_TYPE, nextBlock.getBlockType(), "The next block should be a data block");
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;
    List<IndexedRecord> recordsRead = getRecords(dataBlockRead);
    assertEquals(copyOfRecords.size(), recordsRead.size(),
        "Read records size should be equal to the written records size");
    assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords), recordsRead,
        "Both records lists should be the same. (ordering guaranteed)");
    reader.close();
  }

  private List<IndexedRecord> convertAvroToSerializableIndexedRecords(List<IndexedRecord> indexedRecords) {
    return indexedRecords.stream().map(record -> SerializableIndexedRecord.createInstance(record)).collect(Collectors.toList());
  }

  @Test
  public void testHugeLogFileWrite() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage)
            .withSizeThreshold(3L * 1024 * 1024 * 1024)
            .build();
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 1000);
    List<IndexedRecord> copyOfRecords = records.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    byte[] dataBlockContentBytes = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header).getContentBytes(storage).toByteArray();
    HoodieLogBlock.HoodieLogBlockContentLocation logBlockContentLoc = new HoodieLogBlock.HoodieLogBlockContentLocation(
        HoodieTestUtils.getStorage(basePath), null, 0, dataBlockContentBytes.length, 0);
    HoodieDataBlock reusableDataBlock = new HoodieAvroDataBlock(null, Option.ofNullable(dataBlockContentBytes), false,
        logBlockContentLoc, Option.ofNullable(getSimpleSchema()), header, new HashMap<>(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
    long writtenSize = 0;
    int logBlockWrittenNum = 0;
    while (writtenSize < Integer.MAX_VALUE) {
      AppendResult appendResult = writer.appendBlock(reusableDataBlock);
      assertTrue(appendResult.size() > 0);
      writtenSize += appendResult.size();
      logBlockWrittenNum++;
    }
    writer.close();

    Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), SchemaTestUtil.getSimpleSchema(), true);
    assertTrue(reader.hasNext(), "We wrote a block, we should be able to read it");
    HoodieLogBlock nextBlock = reader.next();
    assertEquals(DEFAULT_DATA_BLOCK_TYPE, nextBlock.getBlockType(), "The next block should be a data block");
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;
    List<IndexedRecord> recordsRead = getRecords(dataBlockRead);
    assertEquals(copyOfRecords.size(), recordsRead.size(),
        "Read records size should be equal to the written records size");
    assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords), recordsRead,
        "Both records lists should be the same. (ordering guaranteed)");
    int logBlockReadNum = 1;
    while (reader.hasNext()) {
      reader.next();
      logBlockReadNum++;
    }
    assertEquals(logBlockWrittenNum, logBlockReadNum, "All written log should be correctly found");
    reader.close();
  }

  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "HFILE_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  public void testBasicAppendAndRead(HoodieLogBlockType dataBlockType) throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(dataBlockType, records1, header);
    writer.appendBlock(dataBlock);
    writer.close();

    writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema()))
        .collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(dataBlockType, records2, header);
    writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema()))
        .collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(dataBlockType, records3, header);
    writer.appendBlock(dataBlock);
    writer.close();

    Reader reader =
        HoodieLogFormat.newReader(storage, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    HoodieLogBlock nextBlock = reader.next();
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;
    List<IndexedRecord> recordsRead1 = getRecords(dataBlockRead);
    assertEquals(copyOfRecords1.size(), recordsRead1.size(),
        "Read records size should be equal to the written records size");
    assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords1), recordsRead1,
        "Both records lists should be the same. (ordering guaranteed)");
    assertEquals(dataBlockRead.getSchema(), getSimpleSchema());

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieDataBlock) nextBlock;
    List<IndexedRecord> recordsRead2 = getRecords(dataBlockRead);
    assertEquals(copyOfRecords2.size(), recordsRead2.size(),
        "Read records size should be equal to the written records size");
    assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords2), recordsRead2,
        "Both records lists should be the same. (ordering guaranteed)");

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieDataBlock) nextBlock;
    List<IndexedRecord> recordsRead3 = getRecords(dataBlockRead);
    assertEquals(copyOfRecords3.size(), recordsRead3.size(),
        "Read records size should be equal to the written records size");
    assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords3), recordsRead3,
        "Both records lists should be the same. (ordering guaranteed)");
    reader.close();
  }

  @Test
  public void testCDCBlock() throws IOException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build();

    String dataSchemaString = "{\"type\":\"record\",\"name\":\"Record\","
        + "\"fields\":["
        + "{\"name\":\"uuid\",\"type\":[\"int\",\"null\"]},"
        + "{\"name\":\"name\",\"type\":[\"string\",\"null\"]},"
        + "{\"name\":\"ts\",\"type\":[\"long\",\"null\"]}"
        + "]}";
    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaString);
    HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
        HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER, dataSchema);
    GenericRecord insertedRecord = new GenericData.Record(dataSchema.toAvroSchema());
    insertedRecord.put("uuid", 1);
    insertedRecord.put("name", "apple");
    insertedRecord.put("ts", 1100L);

    GenericRecord updateBeforeImageRecord = new GenericData.Record(dataSchema.toAvroSchema());
    updateBeforeImageRecord.put("uuid", 2);
    updateBeforeImageRecord.put("name", "banana");
    updateBeforeImageRecord.put("ts", 1000L);
    GenericRecord updateAfterImageRecord = new GenericData.Record(dataSchema.toAvroSchema());
    updateAfterImageRecord.put("uuid", 2);
    updateAfterImageRecord.put("name", "blueberry");
    updateAfterImageRecord.put("ts", 1100L);

    GenericRecord deletedRecord = new GenericData.Record(dataSchema.toAvroSchema());
    deletedRecord.put("uuid", 3);
    deletedRecord.put("name", "cherry");
    deletedRecord.put("ts", 1000L);

    GenericRecord record1 = HoodieCDCUtils.cdcRecord(cdcSchema, "i", "100",
        null, insertedRecord);
    GenericRecord record2 = HoodieCDCUtils.cdcRecord(cdcSchema, "u", "100",
        updateBeforeImageRecord, updateAfterImageRecord);
    GenericRecord record3 = HoodieCDCUtils.cdcRecord(cdcSchema, "d", "100",
        deletedRecord, null);
    List<IndexedRecord> records = new ArrayList<>(Arrays.asList(record1, record2, record3));
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, cdcSchema.toString());
    HoodieDataBlock dataBlock = getDataBlock(HoodieLogBlockType.CDC_DATA_BLOCK, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), cdcSchema);
    assertTrue(reader.hasNext());
    HoodieLogBlock block = reader.next();
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) block;
    List<IndexedRecord> recordsRead = getRecords(dataBlockRead);
    assertEquals(3, recordsRead.size(),
        "Read records size should be equal to the written records size");
    assertEquals(dataBlockRead.getSchema(), cdcSchema);

    GenericRecord insert = (GenericRecord) recordsRead.stream()
        .filter(record -> record.get(0).toString().equals("i")).findFirst().get();
    assertNull(insert.get("before"));
    assertNotNull(insert.get("after"));
    assertEquals(((GenericRecord) insert.get("after")).get("name").toString(), "apple");

    GenericRecord update = (GenericRecord) recordsRead.stream()
        .filter(record -> record.get(0).toString().equals("u")).findFirst().get();
    assertNotNull(update.get("before"));
    assertNotNull(update.get("after"));
    GenericRecord uBefore = (GenericRecord) update.get("before");
    GenericRecord uAfter = (GenericRecord) update.get("after");
    assertEquals(String.valueOf(uBefore.get("name")), "banana");
    assertEquals(Long.valueOf(uBefore.get("ts").toString()), 1000L);
    assertEquals(String.valueOf(uAfter.get("name")), "blueberry");
    assertEquals(Long.valueOf(uAfter.get("ts").toString()), 1100L);

    GenericRecord delete = (GenericRecord) recordsRead.stream()
        .filter(record -> record.get(0).toString().equals("d")).findFirst().get();
    assertNotNull(delete.get("before"));
    assertNull(delete.get("after"));
    assertEquals(((GenericRecord) delete.get("before")).get("name").toString(), "cherry");

    reader.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFilteringUncommittedLogBlocksPreTableVersion8(boolean enableOptimizedLogBlocksScan) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(basePath);
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.SIX);
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
    testBasicAppendAndScanMultipleFiles(ExternalSpillableMap.DiskMapType.ROCKS_DB, true, enableOptimizedLogBlocksScan, true, true);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testBasicAppendAndScanMultipleFiles(ExternalSpillableMap.DiskMapType diskMapType,
                                                  boolean isCompressionEnabled,
                                                  boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    testBasicAppendAndScanMultipleFiles(diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan, false, false);
  }

  private void testBasicAppendAndScanMultipleFiles(ExternalSpillableMap.DiskMapType diskMapType,
                                                   boolean isCompressionEnabled,
                                                   boolean enableOptimizedLogBlocksScan,
                                                   boolean produceUncommittedLogBlocks,
                                                   boolean preTableVersion8)
      throws IOException, URISyntaxException, InterruptedException {
    // Generate 4 delta-log files w/ random records
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> genRecords = testUtil.generateHoodieTestRecords(0, 400);

    List<HoodieLogFile> logFiles = writeLogFiles(partitionPath, schema, genRecords, 4, storage, "test-fileid1", "100", "100");

    if (produceUncommittedLogBlocks) {
      List<IndexedRecord> toBeSkippedRecords = testUtil.generateHoodieTestRecords(0, 200);
      logFiles.addAll(writeLogFiles(partitionPath, schema, toBeSkippedRecords, 2, storage, "test-fileid1", "100", "150"));
      FileCreateUtilsLegacy.createInflightDeltaCommit(basePath, "150", storage);
    }

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage, preTableVersion8);

    // scan all log blocks (across multiple log files)
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(
            logFiles.stream()
                .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList()))
        .withReaderSchema(schema)
        .withLatestInstantTime("200")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .build();

    List<IndexedRecord> scannedRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      Object data = record.toIndexedRecord(schema.toAvroSchema(), CollectionUtils.emptyProps()).get().getData();
      if (data instanceof SerializableIndexedRecord) {
        scannedRecords.add(((SerializableIndexedRecord) data).getData());
      } else {
        scannedRecords.add((GenericRecord) data);
      }
    }

    assertEquals(genRecords.size(), scannedRecords.size(),
        "Scanner records count should be the same as appended records");
    assertEquals(sort(convertAvroToSerializableIndexedRecords(genRecords)), sort(scannedRecords),
        "Scanner records content should be the same as appended records");
    scanner.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testBasicAppendAndPartialScanning(ExternalSpillableMap.DiskMapType diskMapType,
                                                boolean isCompressionEnabled,
                                                boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    // Generate 3 delta-log files w/ random records
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> genRecords = testUtil.generateHoodieTestRecords(0, 300);

    List<HoodieLogFile> logFiles = writeLogFiles(partitionPath, schema, genRecords, 3, storage);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // scan all log blocks (across multiple log files)
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(
            logFiles.stream()
                .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList()))
        .withReaderSchema(schema)
        .withLatestInstantTime("100")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .withForceFullScan(false)
        .build();

    List<String> sampledRecordKeys = Arrays.asList(
        "b190b1fb-392b-4ceb-932d-a72c906127c2",
        "409e9ad3-5def-45e7-9180-ef579c1c220b",
        "e6b31f1c-60a8-4577-acf5-7e8ea318b08b",
        "0c477a9e-e602-4642-8e96-1cfd357b4ba0",
        "ea076c17-32ae-4659-8caf-6ad538b4dd8d",
        "7a943e09-3856-4874-83a1-8ee93e158f94",
        "9cbff584-d8a4-4b05-868b-dc917d6cf841",
        "bda0b0d8-0c56-43b0-89f9-e090d924586b",
        "ee118fb3-69cb-4705-a8c4-88a18e8aa1b7",
        "cb1fbe4d-06c3-4c9c-aea7-2665ffa8b205"
    );

    List<IndexedRecord> sampledRecords = genRecords.stream()
        .filter(r -> sampledRecordKeys.contains(((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()))
        .collect(Collectors.toList());

    //
    // Step 1: Scan by a list of keys
    //

    scanner.scanByFullKeys(sampledRecordKeys);

    List<HoodieRecord> scannedHoodieRecords = new ArrayList<>();
    List<IndexedRecord> scannedAvroRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      scannedHoodieRecords.add(record);
      scannedAvroRecords.add((IndexedRecord) ((SerializableIndexedRecord)
          ((HoodieAvroRecord) record).getData().getInsertValue(schema.toAvroSchema()).get()).getData());
    }

    assertEquals(sort(sampledRecords), sort(scannedAvroRecords));

    //
    // Step 2: Scan by the same list of keys (no new scanning should be performed,
    //         in this case, and same _objects_ have to be returned)
    //

    scanner.scanByFullKeys(sampledRecordKeys);

    List<HoodieRecord> newScannedHoodieRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      newScannedHoodieRecords.add(record);
    }

    assertEquals(scannedHoodieRecords.size(), newScannedHoodieRecords.size());

    for (int i = 0; i < scannedHoodieRecords.size(); ++i) {
      assertSame(scannedHoodieRecords.get(i), newScannedHoodieRecords.get(i), "Objects have to be identical");
    }

    scanner.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testBasicAppendAndPartialScanningByKeyPrefixes(ExternalSpillableMap.DiskMapType diskMapType,
                                                             boolean isCompressionEnabled,
                                                             boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    // Generate 3 delta-log files w/ random records
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> genRecords = testUtil.generateHoodieTestRecords(0, 300);

    List<HoodieLogFile> logFiles = writeLogFiles(partitionPath, schema, genRecords, 3, storage);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // scan all log blocks (across multiple log files)
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(
            logFiles.stream()
                .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList()))
        .withReaderSchema(schema)
        .withLatestInstantTime("100")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .withForceFullScan(false)
        .build();


    List<String> sampledRecordKeys = Arrays.asList(
        "00509b14-3d1a-4283-9a8c-c72b971a9d06",
        "006b2f57-9bf7-4634-910c-c91542ea61e5",
        "007fc45d-7ce2-45be-8765-0b9082412518",
        "00826e50-73b4-4cb0-9d5a-375554d5e0f7"
    );

    List<IndexedRecord> sampledRecords = genRecords.stream()
        .filter(r -> sampledRecordKeys.contains(((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()))
        .collect(Collectors.toList());

    List<String> sampledKeyPrefixes = Collections.singletonList("00");

    //
    // Step 1: Scan by a list of keys
    //

    scanner.scanByKeyPrefixes(sampledKeyPrefixes);

    List<HoodieRecord> scannedHoodieRecords = new ArrayList<>();
    List<IndexedRecord> scannedAvroRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      scannedHoodieRecords.add(record);
      scannedAvroRecords.add((IndexedRecord) ((SerializableIndexedRecord)
          ((HoodieAvroRecord) record).getData().getInsertValue(schema.toAvroSchema()).get()).getData());
    }

    assertEquals(sort(sampledRecords), sort(scannedAvroRecords));

    //
    // Step 2: Scan by the same list of keys (no new scanning should be performed,
    //         in this case, and same _objects_ have to be returned)
    //

    scanner.scanByKeyPrefixes(sampledKeyPrefixes);

    List<HoodieRecord> newScannedHoodieRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      newScannedHoodieRecords.add(record);
    }

    assertEquals(scannedHoodieRecords.size(), newScannedHoodieRecords.size());

    for (int i = 0; i < scannedHoodieRecords.size(); ++i) {
      assertSame(scannedHoodieRecords.get(i), newScannedHoodieRecords.get(i), "Objects have to be identical");
    }

    scanner.close();
  }

  @Test
  public void testAppendAndReadOnCorruptedLog() throws IOException, URISyntaxException, InterruptedException {
    HoodieLogFile logFile = addValidBlock("test-fileId1", "100", 100);

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream = (FSDataOutputStream) storage.append(logFile.getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(474);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(400);
    // Write out incomplete content
    outputStream.write(getUTF8Bytes("something-random"));
    outputStream.flush();
    outputStream.close();

    // Append a proper block that is of the missing length of the corrupted block
    logFile = appendValidBlock(logFile.getPath(), "test-fileId1", "100", 10);

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(storage, logFile, SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    reader.next();
    assertTrue(reader.hasNext(), "We should have corrupted block next");
    HoodieLogBlock block = reader.next();
    assertEquals(HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType(), "The read block should be a corrupt block");
    assertTrue(reader.hasNext(), "Third block should be available");
    reader.next();
    assertFalse(reader.hasNext(), "There should be no more block left");

    reader.close();

    // Simulate another failure back to back
    outputStream = (FSDataOutputStream) storage.append(logFile.getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(500);
    // Write out some bytes
    outputStream.write(getUTF8Bytes("something-else-random"));
    outputStream.flush();
    outputStream.close();

    // Should be able to append a new block
    logFile = appendValidBlock(logFile.getPath(), "test-fileId1", "100", 100);

    // Second round of reads - we should be able to read the first and last block
    reader = HoodieLogFormat.newReader(storage, logFile, SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    reader.next();
    assertTrue(reader.hasNext(), "We should get the 1st corrupted block next");
    reader.next();
    assertTrue(reader.hasNext(), "Third block should be available");
    reader.next();
    assertTrue(reader.hasNext(), "We should get the 2nd corrupted block next");
    block = reader.next();
    assertEquals(HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType(), "The read block should be a corrupt block");
    assertTrue(reader.hasNext(), "We should get the last block next");
    reader.next();
    assertFalse(reader.hasNext(), "We should have no more blocks left");
    reader.close();
  }

  @Test
  public void testSkipCorruptedCheck() throws Exception {
    // normal case: if the block is corrupted, we should be able to read back a corrupted block
    Reader reader1 = createCorruptedFile("test-fileid1");
    HoodieLogBlock block = reader1.next();
    assertEquals(HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType(), "The read block should be a corrupt block");
    reader1.close();

    // as we can't mock a private method or directly test it, we are going this route.
    // So adding a corrupted block which ideally should have been skipped for write transactional system. and hence when we call next() on log block reader, it will fail.
    Reader reader2 = createCorruptedFile("test-fileid2");
    assertTrue(reader2.hasNext(), "We should have corrupted block next");

    // mock the fs to be GCS to skip isBlockCorrupted() check
    Field f1 = reader2.getClass().getDeclaredField("storage");
    f1.setAccessible(true);
    HoodieStorage mockStorage = Mockito.mock(HoodieStorage.class);
    when(mockStorage.getScheme()).thenReturn("gs");
    f1.set(reader2, mockStorage);

    // except an exception for block type since the block is corrupted
    Exception exception = assertThrows(IllegalArgumentException.class, reader2::next);
    assertTrue(exception.getMessage().contains("Invalid block byte type found"));
    reader2.close();
  }

  @Test
  public void testMissingBlockExceptMagicBytes() throws IOException, URISyntaxException, InterruptedException {
    HoodieLogFile logFile = addValidBlock("test-fileId1", "100", 100);

    // Append just magic bytes and move onto next block
    FSDataOutputStream outputStream = (FSDataOutputStream) storage.append(logFile.getPath());
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.flush();
    outputStream.close();

    // Append a proper block
    logFile = appendValidBlock(logFile.getPath(), "test-fileId1", "100", 10);

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(storage, logFile, SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    reader.next();
    assertTrue(reader.hasNext(), "We should have corrupted block next");
    HoodieLogBlock block = reader.next();
    assertEquals(HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType(), "The read block should be a corrupt block");
    assertTrue(reader.hasNext(), "Third block should be available");
    reader.next();
    assertFalse(reader.hasNext(), "There should be no more block left");

    reader.close();
  }

  private HoodieLogFile addValidBlock(String fileId, String commitTime, int numRecords) throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId(fileId).withInstantTime(commitTime).withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, numRecords);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();
    return writer.getLogFile();
  }

  private HoodieLogFile appendValidBlock(StoragePath path, String fileId, String commitTime,
                                         int numRecords)
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId(fileId).withInstantTime(commitTime).withStorage(storage).build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(path));
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, numRecords);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();
    return writer.getLogFile();
  }

  @Test
  public void testValidateCorruptBlockEndPosition() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(474);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(400);
    // Write out incomplete content
    outputStream.write(getUTF8Bytes("something-random"));
    // get corrupt block end position
    long corruptBlockEndPos = outputStream.getPos();
    outputStream.flush();
    outputStream.close();

    // Append a proper block again
    appendValidBlock(writer.getLogFile().getPath(), "test-fileid1", "100", 10);

    // Read data and corrupt block
    Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    reader.next();
    assertTrue(reader.hasNext(), "We should have corrupted block next");
    HoodieLogBlock block = reader.next();
    assertEquals(HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType(), "The read block should be a corrupt block");
    // validate the corrupt block end position correctly.
    assertEquals(corruptBlockEndPos, block.getBlockContentLocation().get().getBlockEndPos());
    assertTrue(reader.hasNext(), "Third block should be available");
    reader.next();
    assertFalse(reader.hasNext(), "There should be no more block left");

    reader.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderBasic(ExternalSpillableMap.DiskMapType diskMapType,
                                           boolean isCompressionEnabled,
                                           boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage)
            .withSizeThreshold(500).build();
    SchemaTestUtil testUtil = new SchemaTestUtil();

    // Write 1
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    copyOfRecords1.addAll(copyOfRecords2);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    checkLogBlocksAndKeys("100", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        200, 200, Option.of(originalKeys));
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithRollbackTombstone(ExternalSpillableMap.DiskMapType diskMapType,
                                                           boolean isCompressionEnabled,
                                                           boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    // Rollback the last write
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    // Write 3
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    List<IndexedRecord> records3 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records3, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);

    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    checkLogBlocksAndKeys("102", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        200, 200, Option.of(originalKeys));
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithFailedPartialBlock(ExternalSpillableMap.DiskMapType diskMapType,
                                                            boolean isCompressionEnabled,
                                                            boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);
    writer.close();

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(1000);

    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());

    // Write out some header
    outputStream.write(HoodieLogBlock.getHeaderMetadataBytes(header));
    outputStream.writeLong(getUTF8Bytes("something-random").length);
    outputStream.write(getUTF8Bytes("something-random"));
    outputStream.flush();
    outputStream.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    // Write 3
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    List<IndexedRecord> records3 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records3, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "103", storage);

    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    checkLogBlocksAndKeys("103", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        200, 200, Option.of(originalKeys));
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithDeleteAndRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                           boolean isCompressionEnabled,
                                                           boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    // Delete 50 keys
    List<DeleteRecord> deletedRecords = copyOfRecords1.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    copyOfRecords2.addAll(copyOfRecords1);
    List<String> originalKeys =
        copyOfRecords2.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);

    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("102")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .build();

    assertEquals(200, scanner.getTotalLogRecords(), "We still would read 200 records");
    final List<String> readKeys = new ArrayList<>(200);
    final List<Boolean> emptyPayloads = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    scanner.forEach(s -> {
      try {
        if (!((HoodieRecordPayload) s.getData()).getInsertValue(schema.toAvroSchema()).isPresent()) {
          emptyPayloads.add(true);
        }
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    assertEquals(50, emptyPayloads.size(), "Stream collect should return all 50 records with empty payloads");
    originalKeys.removeAll(deletedRecords);
    Collections.sort(originalKeys);
    Collections.sort(readKeys);
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 150 records from 2 versions");

    // Rollback the 1st block i.e. a data block.
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    FileCreateUtilsLegacy.deleteDeltaCommit(basePath, "101", storage);

    readKeys.clear();
    scanner.close();
    scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("103")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .build();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    final List<Boolean> newEmptyPayloads = new ArrayList<>();
    scanner.forEach(s -> {
      try {
        if (!((HoodieRecordPayload) s.getData()).getInsertValue(schema.toAvroSchema()).isPresent()) {
          newEmptyPayloads.add(true);
        }
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    assertEquals(100, readKeys.size(), "Stream collect should return 100 records, since 2nd block is rolled back");
    assertEquals(50, newEmptyPayloads.size(), "Stream collect should return all 50 records with empty payloads");
    List<String> firstBlockRecords =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());
    Collections.sort(firstBlockRecords);
    Collections.sort(readKeys);
    assertEquals(firstBlockRecords, readKeys, "CompositeAvroLogReader should return 150 records from 2 versions");
    writer.close();
    scanner.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithCommitBeforeAndAfterRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                                      boolean isCompressionEnabled,
                                                                      boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    String fileId = "test-fileid111";
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId(fileId).withInstantTime("100").withStorage(storage).build();

    // Write 1 -> 100 records are written
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(HoodieLogBlockType.AVRO_DATA_BLOCK, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2 -> 100 records are written
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> allRecordsInserted = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    dataBlock = getDataBlock(HoodieLogBlockType.AVRO_DATA_BLOCK, records2, header);
    writer.appendBlock(dataBlock);
    allRecordsInserted.addAll(copyOfRecords1);

    // Delete 50 keys from write 1 batch
    List<HoodieKey> deletedKeys = copyOfRecords1.stream()
        .map(s -> (new HoodieKey(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (HoodieKey deletedKey : deletedKeys) {
      deleteRecordList.add(Pair.of(
          DeleteRecord.create(deletedKey.getRecordKey(), deletedKey.getPartitionPath()), -1L));
    }
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, fileId, HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    // Rollback the last block i.e. a data block.
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    // Recreate the delete block which should have been removed from consideration because of rollback block next to it.
    Map<HoodieLogBlock.HeaderMetadataType, String> deleteBlockHeader = new HashMap<>();
    deleteBlockHeader.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    deleteRecordList = new ArrayList<>();
    for (HoodieKey deletedKey : deletedKeys) {
      deleteRecordList.add(Pair.of(
          DeleteRecord.create(deletedKey.getRecordKey(), deletedKey.getPartitionPath()), -1L));
    }
    deleteBlock = new HoodieDeleteBlock(deleteRecordList, deleteBlockHeader);
    writer.appendBlock(deleteBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);

    final List<String> readKeys = new ArrayList<>();
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("103")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .build();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    final List<Boolean> newEmptyPayloads = new ArrayList<>();
    scanner.forEach(s -> {
      try {
        if (!((HoodieRecordPayload) s.getData()).getInsertValue(schema.toAvroSchema()).isPresent()) {
          newEmptyPayloads.add(true);
        }
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    assertEquals(50, newEmptyPayloads.size(), "Stream collect should return 50 records with empty payloads.");
    List<String> recordKeysInserted = allRecordsInserted.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .collect(Collectors.toList());
    Collections.sort(recordKeysInserted);
    Collections.sort(readKeys);
    assertEquals(recordKeysInserted, readKeys, "CompositeAvroLogReader should return 150 records from 2 versions");
    writer.close();
    scanner.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithDisorderDelete(ExternalSpillableMap.DiskMapType diskMapType,
                                                        boolean isCompressionEnabled)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    copyOfRecords1.addAll(copyOfRecords2);
    List<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

    // Delete 10 keys
    // Default orderingVal is 0, which means natural order, the DELETE records
    // should overwrite the data records.
    List<DeleteRecord> deleteRecords1 = copyOfRecords1.subList(0, 10).stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList());

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");

    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deleteRecords1) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock1 = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock1);

    // Delete another 10 keys with -1 as orderingVal.
    // The deletion should not work

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    deleteRecordList = copyOfRecords1.subList(10, 20).stream()
        .map(s -> Pair.of(DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString(), -1), -1L))
        .collect(Collectors.toList());

    HoodieDeleteBlock deleteBlock2 = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock2);

    // Delete another 10 keys with +1 as orderingVal.
    // The deletion should work because the keys has greater ordering value.
    List<DeleteRecord> deletedRecords3 = copyOfRecords1.subList(20, 30).stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString(), 1)))
        .collect(Collectors.toList());

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "104");
    deleteRecordList.clear();
    for (DeleteRecord dr : deletedRecords3) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock3 = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock3);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "103", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "104", storage);

    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("104")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .build();

    assertEquals(200, scanner.getTotalLogRecords(), "We still would read 200 records");
    final List<String> readKeys = new ArrayList<>(200);
    final List<String> emptyPayloadKeys = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getRecordKey()));
    scanner.forEach(s -> {
      try {
        if (!((HoodieRecordPayload) s.getData()).getInsertValue(schema.toAvroSchema()).isPresent()) {
          emptyPayloadKeys.add(s.getRecordKey());
        }
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    assertEquals(20, emptyPayloadKeys.size(), "Stream collect should return all 20 records with empty payloads");

    originalKeys.removeAll(deleteRecords1.stream().map(DeleteRecord::getRecordKey).collect(Collectors.toSet()));
    originalKeys.removeAll(deletedRecords3.stream().map(DeleteRecord::getRecordKey).collect(Collectors.toSet()));
    readKeys.removeAll(emptyPayloadKeys);

    Collections.sort(originalKeys);
    Collections.sort(readKeys);
    assertEquals(originalKeys, readKeys, "HoodieMergedLogRecordScanner should return 180 records from 4 versions");
    writer.close();
    scanner.close();
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithFailedRollbacks(ExternalSpillableMap.DiskMapType diskMapType,
                                                         boolean isCompressionEnabled,
                                                         boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a Data block and Delete block with same InstantTime (written in same batch)
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    // Delete 50 keys
    // Delete 50 keys
    List<DeleteRecord> deleteRecords = copyOfRecords1.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deleteRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Attempt 1 : Write rollback block for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    try {
      writer.appendBlock(commandBlock);
      // Say job failed, retry writing 2 rollback in the next rollback(..) attempt
      throw new Exception("simulating failure");
    } catch (Exception e) {
      // it's okay
    }
    // Attempt 2 : Write another rollback blocks for a failed write
    writer.appendBlock(commandBlock);
    writer.close();

    checkLogBlocksAndKeys("100", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        0, 0, Option.empty());
    FileCreateUtilsLegacy.deleteDeltaCommit(basePath, "100", storage);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithInsertDeleteAndRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                                 boolean isCompressionEnabled,
                                                                 boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a Data block and Delete block with same InstantTime (written in same batch)
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Delete 50 keys
    List<DeleteRecord> deleteRecords = copyOfRecords1.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deleteRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Write 2 rollback blocks (1 data block + 1 delete bloc) for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);
    writer.appendBlock(commandBlock);
    writer.close();

    checkLogBlocksAndKeys("100", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        0, 0, Option.empty());
    FileCreateUtilsLegacy.deleteDeltaCommit(basePath, "100", storage);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithInvalidRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                         boolean isCompressionEnabled,
                                                         boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Write invalid rollback for a failed write (possible for in-flight commits)
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);
    writer.close();

    checkLogBlocksAndKeys("100", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        100, 100, Option.empty());
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithInsertsDeleteAndRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                                  boolean isCompressionEnabled,
                                                                  boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a 3 Data blocs with same InstantTime (written in same batch)
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);
    writer.appendBlock(dataBlock);
    writer.appendBlock(dataBlock);

    // Delete 50 keys
    // Delete 50 keys
    List<DeleteRecord> deleteRecords = copyOfRecords1.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deleteRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    writer.appendBlock(deleteBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Write 1 rollback block for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);
    writer.close();

    checkLogBlocksAndKeys("101", schema, diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan,
        0, 0, Option.empty());
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  void testLogReaderWithDifferentVersionsOfDeleteBlocks(ExternalSpillableMap.DiskMapType diskMapType,
                                                        boolean isCompressionEnabled,
                                                        boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    List<String> deleteKeyListInV2Block = Arrays.asList(
        "d448e1b8-a0d4-45c0-bf2d-a9e16ff3c8ce",
        "df3f71cd-5b68-406c-bb70-861179444adb",
        "cf64885c-af32-463b-8f1b-2f31a39b1afa",
        "9884e134-0d60-46e8-8a1e-36db0e455c4a",
        "698544b8-defa-4fa7-ac15-8963f7d0784d",
        "081c279e-fc6a-4e05-89b7-3136e4cad488",
        "1041fac7-8a54-47e6-8a2d-d1a650301699",
        "69c003f8-386d-40a0-9c61-5a903d1d6ac2",
        "e574d164-f8c4-47cf-b150-264c2364f10e",
        "d76007d2-9dc8-46ff-bf6f-0789c6ffffc0");

    // Write 1: add 100 records
    SchemaTestUtil testUtil = new SchemaTestUtil();
    // Generate 100 records with 10 records to be deleted in V2 delete block in commit 102
    List<String> recordKeyList = testUtil.genRandomUUID(100, deleteKeyListInV2Block);
    List<IndexedRecord> records1 =
        testUtil.generateHoodieTestRecords(0, recordKeyList, "0000/00/00", "100");
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // Write 2: add another 100 records
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    // Delete 10 keys in V2 delete block
    byte[] contentBytes = new byte[605];
    InputStream inputStream = TestHoodieLogFormat.class
        .getResourceAsStream("/format/delete-block-v2-content-10-records.data");
    inputStream.read(contentBytes);

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    writer.appendBlock(new HoodieDeleteBlock(
        Option.of(contentBytes), null, true, Option.empty(), header, Collections.EMPTY_MAP));

    // Delete 60 keys in V3 delete block
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    List<DeleteRecord> deletedRecords = copyOfRecords2.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 60);
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();
    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    writer.appendBlock(new HoodieDeleteBlock(deleteRecordList, header));

    copyOfRecords2.addAll(copyOfRecords1);
    List<String> originalKeys =
        copyOfRecords2.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "103", storage);

    try (HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("103")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .build()) {
      assertEquals(200, scanner.getTotalLogRecords(), "We still would read 200 records");
      final List<String> readKeys = new ArrayList<>(200);
      final List<String> recordKeys = new ArrayList<>(200);
      final List<Boolean> emptyPayloads = new ArrayList<>();
      scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
      scanner.forEach(s -> {
        try {
          if (!((HoodieRecordPayload) s.getData()).getInsertValue(schema.toAvroSchema(), new Properties()).isPresent()) {
            emptyPayloads.add(true);
          } else {
            recordKeys.add(s.getKey().getRecordKey());
          }
        } catch (IOException io) {
          throw new UncheckedIOException(io);
        }
      });
      assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
      assertEquals(70, emptyPayloads.size(), "Stream collect should return all 70 records with empty payloads");
      Collections.sort(originalKeys);
      Collections.sort(readKeys);
      assertEquals(originalKeys, readKeys, "200 records should be scanned regardless of deletes or not");

      originalKeys.removeAll(deleteKeyListInV2Block);
      originalKeys.removeAll(
          deletedRecords.stream().map(DeleteRecord::getRecordKey).collect(Collectors.toList()));
      Collections.sort(originalKeys);
      Collections.sort(recordKeys);
      assertEquals(originalKeys, recordKeys, "Only 130 records should exist after deletion");
    }
  }

  @Test
  public void testAvroLogRecordReaderWithRollbackOlderBlocks()
      throws IOException, URISyntaxException, InterruptedException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Write 2
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(100, 10);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);

    // Should be able to read all 110 records
    checkLogBlocksAndKeys("101", schema, ExternalSpillableMap.DiskMapType.BITCASK, false,
        false, 110, 110, Option.empty());

    // Write a rollback for commit 100 which is not the latest commit
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    // Should only be able to read 10 records from commit 101
    checkLogBlocksAndKeys("101", schema, ExternalSpillableMap.DiskMapType.BITCASK, false,
        false, 10, 10, Option.empty());

    // Write a rollback for commit 101 which is the latest commit
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);
    writer.close();

    // Should not read any records as both commits are rolled back
    checkLogBlocksAndKeys("101", schema, ExternalSpillableMap.DiskMapType.BITCASK, false,
        false, 0, 0, Option.empty());
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback(ExternalSpillableMap.DiskMapType diskMapType,
                                                                         boolean isCompressionEnabled,
                                                                         boolean enableOptimizedLogBlocksScan)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a 3 Data blocs with same InstantTime (written in same batch)
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);
    writer.appendBlock(dataBlock);
    writer.appendBlock(dataBlock);

    writer.close();
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    outputStream = (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    outputStream = (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    // Write 1 rollback block for the last commit instant
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);
    writer.close();

    checkLogBlocksAndKeys("101", schema, ExternalSpillableMap.DiskMapType.BITCASK, false,
        false, 0, 0, Option.empty());
    FileCreateUtilsLegacy.deleteDeltaCommit(basePath, "100", storage);
  }

  @ParameterizedTest
  @MethodSource("testArgumentsWithoutOptimizedScanArg")
  public void testAvroLogRecordReaderWithMixedInsertsCorruptsRollbackAndMergedLogBlock(ExternalSpillableMap.DiskMapType diskMapType,
                                                                                       boolean isCompressionEnabled)
      throws IOException, URISyntaxException, InterruptedException {

    // Write blocks in this manner.
    // Ex: B1(i1), B2(i2), B3(i3), CRPB, CRPB, CB4(i4, [i1,i2]), CB5(i5, [CB4, B3]), B6(i6), B7(i7), B8(i8), CB9(i9, [B7, B8])
    // CRPB implies a corrupt block and CB implies a compacted block.

    // Write a 3 Data blocks with same InstantTime (written in same batch)
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();

    // Write 1st data blocks multiple times.
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records1 = testUtil.generateHoodieTestRecords(0, 100);
    Set<String> recordKeysOfFirstTwoBatches = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())
            .get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()).collect(Collectors.toSet());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records1), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Write 2nd data block
    List<IndexedRecord> records2 = testUtil.generateHoodieTestRecords(0, 100);
    recordKeysOfFirstTwoBatches.addAll(records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())
            .get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()).collect(Collectors.toList()));
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records2), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);

    // Write 3rd data block
    List<IndexedRecord> records3 = testUtil.generateHoodieTestRecords(0, 100);
    Set<String> recordKeysOfFirstThreeBatches = new HashSet<>(recordKeysOfFirstTwoBatches);
    recordKeysOfFirstThreeBatches.addAll(records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())
            .get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()).collect(Collectors.toList()));
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records3), header);
    writer.appendBlock(dataBlock);

    writer.close();
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "102", storage);

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    outputStream = (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));

    // Create compacted block CB4
    List<IndexedRecord> compactedRecords = Stream.of(records1, records2).flatMap(Collection::stream)
        .collect(Collectors.toList());
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    header.put(HeaderMetadataType.COMPACTED_BLOCK_TIMES, "100,101");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(compactedRecords), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "103", storage);

    // Create compacted block CB5
    List<IndexedRecord> secondCompactedRecords = Stream.of(compactedRecords, records3).flatMap(Collection::stream)
        .collect(Collectors.toList());
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "104");
    header.put(HeaderMetadataType.COMPACTED_BLOCK_TIMES, "103,102");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(secondCompactedRecords), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "104", storage);

    // Write 6th data block
    List<IndexedRecord> records6 = testUtil.generateHoodieTestRecords(0, 100);
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "105");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records6), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "105", storage);

    // Write 7th data block
    List<IndexedRecord> records7 = testUtil.generateHoodieTestRecords(0, 100);
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "106");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records7), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "106", storage);

    // Write 8th data block
    List<IndexedRecord> records8 = testUtil.generateHoodieTestRecords(0, 100);
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "107");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(records8), header);
    writer.appendBlock(dataBlock);

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "107", storage);

    // Create compacted block CB9
    List<IndexedRecord> thirdCompactedBlockRecords = Stream.of(records7, records8).flatMap(Collection::stream)
        .collect(Collectors.toList());
    header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "108");
    header.put(HeaderMetadataType.COMPACTED_BLOCK_TIMES, "106,107");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, new ArrayList<>(thirdCompactedBlockRecords), header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "108", storage);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("108")
        .withMaxMemorySizeInBytes(1024000L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(true)
        .build();
    assertEquals(600, scanner.getTotalLogRecords(), "We would read 600 records from scanner");
    final List<String> readKeys = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    List<String> expectedRecords = Stream.of(secondCompactedRecords, records6, thirdCompactedBlockRecords)
        .flatMap(Collection::stream)
        .map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .sorted()
        .collect(Collectors.toList());
    List<String> validBlockInstants = scanner.getValidBlockInstants();
    List<String> expectedBlockInstants = Arrays.asList("108", "105", "104");
    assertEquals(expectedBlockInstants, validBlockInstants);
    Collections.sort(readKeys);
    assertEquals(expectedRecords, readKeys, "Record keys read should be exactly same.");
    scanner.close();
  }

  /*
   * During a spark stage failure, when the stage is retried, tasks that are part of the previous attempt
   * of the stage would continue to run.  As a result two different tasks could be performing the same operation.
   * When trying to update the log file, only one of the tasks would succeed (one holding lease on the log file).
   *
   * In order to make progress in this scenario, second task attempting to update the log file would rollover to
   * a new version of the log file.  As a result, we might end up with two log files with same set of data records
   * present in both of them.
   *
   * Following uint tests mimic this scenario to ensure that the reader can handle merging multiple log files with
   * duplicate data.
   *
   */
  private void testAvroLogRecordReaderMergingMultipleLogFiles(int numRecordsInLog1, int numRecordsInLog2,
                                                              ExternalSpillableMap.DiskMapType diskMapType,
                                                              boolean isCompressionEnabled,
                                                              boolean enableOptimizedLogBlocksScan) {
    try {
      // Write one Data block with same InstantTime (written in same batch)
      HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> records = testUtil.generateHoodieTestRecords(0, 101);
      List<IndexedRecord> records2 = new ArrayList<>(records);

      // Write1 with numRecordsInLog1 records written to log.1
      Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
          .withInstantTime("100").withStorage(storage).build();

      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records.subList(0, numRecordsInLog1), header);
      writer.appendBlock(dataBlock);
      // Get the size of the block
      long size = writer.getCurrentSize();
      writer.close();

      // write2 with numRecordsInLog2 records written to log.2
      Writer writer2 = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
          .withInstantTime("100").withStorage(storage).withSizeThreshold(size - 1).build();

      Map<HoodieLogBlock.HeaderMetadataType, String> header2 = new HashMap<>();
      header2.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
      header2.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieDataBlock dataBlock2 = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2.subList(0, numRecordsInLog2), header2);
      writer2.appendBlock(dataBlock2);
      // Get the size of the block
      writer2.close();

      FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

      // From the two log files generated, read the records
      List<String> allLogFiles = FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1",
          HoodieLogFile.DELTA_EXTENSION, "100").map(s -> s.getPath().toString()).collect(Collectors.toList());

      HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
          .withStorage(storage)
          .withBasePath(basePath)
          .withLogFilePaths(allLogFiles)
          .withReaderSchema(schema)
          .withLatestInstantTime("100")
          .withMaxMemorySizeInBytes(10240L)
          .withReverseReader(false)
          .withBufferSize(BUFFER_SIZE)
          .withSpillableMapBasePath(spillableBasePath)
          .withDiskMapType(diskMapType)
          .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
          .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
          .build();

      assertEquals(Math.max(numRecordsInLog1, numRecordsInLog2), scanner.getNumMergedRecordsInLog(),
          "We would read 100 records");
      scanner.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithFailedTaskInFirstStageAttempt(ExternalSpillableMap.DiskMapType diskMapType,
                                                                       boolean isCompressionEnabled,
                                                                       boolean enableOptimizedLogBlocksScan) {
    /*
     * FIRST_ATTEMPT_FAILED:
     * Original task from the stage attempt failed, but subsequent stage retry succeeded.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(77, 100,
        diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderWithFailedTaskInSecondStageAttempt(ExternalSpillableMap.DiskMapType diskMapType,
                                                                        boolean isCompressionEnabled,
                                                                        boolean enableOptimizedLogBlocksScan) {
    /*
     * SECOND_ATTEMPT_FAILED:
     * Original task from stage attempt succeeded, but subsequent retry attempt failed.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(100, 66,
        diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAvroLogRecordReaderTasksSucceededInBothStageAttempts(ExternalSpillableMap.DiskMapType diskMapType,
                                                                       boolean isCompressionEnabled,
                                                                       boolean enableOptimizedLogBlocksScan) {
    /*
     * BOTH_ATTEMPTS_SUCCEEDED:
     * Original task from the stage attempt and duplicate task from the stage retry succeeded.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(100, 100,
        diskMapType, isCompressionEnabled, enableOptimizedLogBlocksScan);
  }

  @Test
  public void testBasicAppendAndReadInReverse()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    // append 100 more records
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    // append 100 more records
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema()))
        .collect(Collectors.toList());
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records3, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    HoodieLogFile logFile = new HoodieLogFile(writer.getLogFile().getPath(), storage.getPathInfo(writer.getLogFile().getPath()).getLength());
    try (HoodieLogFileReader reader = new HoodieLogFileReader(storage, logFile, SchemaTestUtil.getSimpleSchema(), BUFFER_SIZE, true)) {

      assertTrue(reader.hasPrev(), "Last block should be available");
      HoodieLogBlock prevBlock = reader.prev();
      HoodieDataBlock dataBlockRead = (HoodieDataBlock) prevBlock;

      List<IndexedRecord> recordsRead1 = getRecords(dataBlockRead);
      assertEquals(copyOfRecords3.size(), recordsRead1.size(),
          "Third records size should be equal to the written records size");
      assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords3), recordsRead1,
          "Both records lists should be the same. (ordering guaranteed)");

      assertTrue(reader.hasPrev(), "Second block should be available");
      prevBlock = reader.prev();
      dataBlockRead = (HoodieDataBlock) prevBlock;
      List<IndexedRecord> recordsRead2 = getRecords(dataBlockRead);
      assertEquals(copyOfRecords2.size(), recordsRead2.size(),
          "Read records size should be equal to the written records size");
      assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords2), recordsRead2,
          "Both records lists should be the same. (ordering guaranteed)");

      assertTrue(reader.hasPrev(), "First block should be available");
      prevBlock = reader.prev();
      dataBlockRead = (HoodieDataBlock) prevBlock;
      List<IndexedRecord> recordsRead3 = getRecords(dataBlockRead);
      assertEquals(copyOfRecords1.size(), recordsRead3.size(),
          "Read records size should be equal to the written records size");
      assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords1), recordsRead3,
          "Both records lists should be the same. (ordering guaranteed)");

      assertFalse(reader.hasPrev());
    }
  }

  @Test
  public void testAppendAndReadOnCorruptedLogInReverse()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    // Write out a length that does not confirm with the content
    outputStream.writeInt(1000);
    // Write out footer length
    outputStream.writeInt(1);
    // Write out some metadata
    // TODO : test for failure to write metadata - NA ?
    outputStream.write(HoodieLogBlock.getHeaderMetadataBytes(header));
    outputStream.write(getUTF8Bytes("something-random"));
    outputStream.flush();
    outputStream.close();

    // Should be able to append a new block
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    ((HoodieLogFormatWriter) writer).withOutputStream(
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    // First round of reads - we should be able to read the first block and then EOF
    HoodieLogFile logFile = new HoodieLogFile(writer.getLogFile().getPath(),
        storage.getPathInfo(writer.getLogFile().getPath()).getLength());

    try (HoodieLogFileReader reader = new HoodieLogFileReader(storage, logFile, schema, BUFFER_SIZE, true)) {

      assertTrue(reader.hasPrev(), "Last block should be available");
      HoodieLogBlock block = reader.prev();
      assertTrue(block instanceof HoodieDataBlock, "Last block should be datablock");

      assertTrue(reader.hasPrev(), "Last block should be available");
      assertThrows(CorruptedLogFileException.class, reader::prev);
    }
  }

  @Test
  public void testBasicAppendAndTraverseInReverse()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema.toAvroSchema())).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records1, header);
    writer.appendBlock(dataBlock);

    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records2, header);
    writer.appendBlock(dataBlock);

    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records3, header);
    writer.appendBlock(dataBlock);
    writer.close();

    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);

    HoodieLogFile logFile = new HoodieLogFile(writer.getLogFile().getPath(),
        storage.getPathInfo(writer.getLogFile().getPath()).getLength());
    try (HoodieLogFileReader reader =
             new HoodieLogFileReader(storage, logFile, SchemaTestUtil.getSimpleSchema(), BUFFER_SIZE, true)) {

      assertTrue(reader.hasPrev(), "Third block should be available");
      reader.moveToPrev();

      assertTrue(reader.hasPrev(), "Second block should be available");
      reader.moveToPrev();

      // After moving twice, this last reader.prev() should read the First block written
      assertTrue(reader.hasPrev(), "First block should be available");
      HoodieLogBlock prevBlock = reader.prev();
      HoodieDataBlock dataBlockRead = (HoodieDataBlock) prevBlock;
      List<IndexedRecord> recordsRead = getRecords(dataBlockRead);
      assertEquals(copyOfRecords1.size(), recordsRead.size(),
          "Read records size should be equal to the written records size");
      assertEquals(convertAvroToSerializableIndexedRecords(copyOfRecords1), recordsRead,
          "Both records lists should be the same. (ordering guaranteed)");

      assertFalse(reader.hasPrev());
    }
  }

  @Test
  public void testV0Format() throws IOException, URISyntaxException {
    // HoodieLogFormatVersion.DEFAULT_VERSION has been deprecated so we cannot
    // create a writer for it. So these tests are only for the HoodieAvroDataBlock
    // of older version.
    HoodieSchema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> recordsCopy = convertAvroToSerializableIndexedRecords(new ArrayList<>(records));
    assertEquals(100, records.size());
    assertEquals(100, recordsCopy.size());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList()), schema);
    byte[] content = dataBlock.getBytes(schema.toAvroSchema());
    assertTrue(content.length > 0);

    HoodieLogBlock logBlock = HoodieAvroDataBlock.getBlock(content, schema);
    assertEquals(HoodieLogBlockType.AVRO_DATA_BLOCK, logBlock.getBlockType());
    List<IndexedRecord> readRecords = getRecords((HoodieAvroDataBlock) logBlock);
    assertEquals(readRecords.size(), recordsCopy.size());
    for (int i = 0; i < recordsCopy.size(); ++i) {
      assertEquals(recordsCopy.get(i), readRecords.get(i));
    }

    // Reader schema is optional if it is same as write schema
    logBlock = HoodieAvroDataBlock.getBlock(content, null);
    assertEquals(HoodieLogBlockType.AVRO_DATA_BLOCK, logBlock.getBlockType());
    readRecords = getRecords((HoodieAvroDataBlock) logBlock);
    assertEquals(readRecords.size(), recordsCopy.size());
    for (int i = 0; i < recordsCopy.size(); ++i) {
      assertEquals(recordsCopy.get(i), readRecords.get(i));
    }
  }

  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "HFILE_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  public void testDataBlockFormatAppendAndReadWithProjectedSchema(
      HoodieLogBlockType dataBlockType
  ) throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .withStorage(storage)
        .build();

    List<GenericRecord> records = SchemaTestUtil.generateTestGenericRecords(0, 1000);

    HoodieSchema schema = getSimpleSchema();

    Map<HoodieLogBlock.HeaderMetadataType, String> header =
        new HashMap<HoodieLogBlock.HeaderMetadataType, String>() {
          {
            put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
            put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
          }
        };

    // Init Benchmark to report number of bytes actually read from the Block
    BenchmarkCounter.initCounterFromReporter(HadoopMapRedUtils.createTestReporter(),
        ((FileSystem) storage.getFileSystem()).getConf());

    // NOTE: Have to use this ugly hack since List generic is not covariant in its type param
    HoodieDataBlock dataBlock = getDataBlock(dataBlockType, (List<IndexedRecord>) (List) records, header);

    writer.appendBlock(dataBlock);
    writer.close();

    HoodieSchema projectedSchema = HoodieSchemaUtils.generateProjectionSchema(schema, Collections.singletonList("name"));

    List<GenericRecord> projectedRecords = HoodieAvroUtils.rewriteRecords(records, projectedSchema.toAvroSchema());

    try (Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), projectedSchema, false)) {
      assertTrue(reader.hasNext(), "First block should be available");

      HoodieLogBlock nextBlock = reader.next();

      HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;

      Map<HoodieLogBlockType, Integer> expectedReadBytes =
          new HashMap<HoodieLogBlockType, Integer>() {
            {
              put(HoodieLogBlockType.AVRO_DATA_BLOCK, 0); // not supported
              put(HoodieLogBlockType.HFILE_DATA_BLOCK, 0); // not supported
              put(HoodieLogBlockType.PARQUET_DATA_BLOCK,
                  HoodieAvroUtils.gteqAvro1_9()
                      ? getJavaVersion() == 17 || getJavaVersion() == 11 ? 1803 : 1802
                      : 1809);
            }
          };

      List<IndexedRecord> recordsRead = getRecords(dataBlockRead);
      List<IndexedRecord> expectedRecords = convertAvroToSerializableIndexedRecords(projectedRecords.stream()
                    .map(record -> (IndexedRecord)record).collect(Collectors.toList()));
      assertEquals(expectedRecords.size(), recordsRead.size(),
          "Read records size should be equal to the written records size");
      assertEquals(expectedRecords, recordsRead,
          "Both records lists should be the same. (ordering guaranteed)");
      assertEquals(dataBlockRead.getSchema(), projectedSchema);

      int bytesRead = (int) BenchmarkCounter.getBytesRead();

      assertEquals(expectedReadBytes.get(dataBlockType), bytesRead, "Read bytes have to match");
    }
  }

  @ParameterizedTest
  @CsvSource(value = {"false,false,false", "false,false,true",
      "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
  public void testGetRecordPositions(boolean recordWithPositions,
                                     boolean allValidPositions,
                                     boolean addBaseFileInstantTimeOfPositions) throws IOException {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    List<Long> positions = new ArrayList<>();
    if (recordWithPositions) {
      positions.addAll(TestLogReaderUtils.generatePositions());
      if (!allValidPositions) {
        positions.add(INVALID_POSITION);
        positions.set(positions.size() / 2, INVALID_POSITION);
      }
    }

    List<Pair<DeleteRecord, Long>> deleteRecordList = positions.isEmpty()
        ? IntStream.range(0, 10).boxed()
        .map(i -> Pair.of(DeleteRecord.create("key" + i, "partition"), INVALID_POSITION))
        .collect(Collectors.toList())
        : IntStream.range(0, positions.size()).boxed()
        .map(i -> Pair.of(DeleteRecord.create("key" + i, "partition"), positions.get(i)))
        .collect(Collectors.toList());
    List<HoodieRecord> recordList = positions.isEmpty()
        ? IntStream.range(0, 10).boxed()
        .map(i -> new HoodieAvroRecord(new HoodieKey("key" + i, "partition"), null, HoodieOperation.INSERT,
            new HoodieRecordLocation("001", "file1", INVALID_POSITION), null))
        .collect(Collectors.toList())
        : IntStream.range(0, positions.size()).boxed()
        .map(i -> new HoodieAvroRecord(new HoodieKey("key" + i, "partition"), null, HoodieOperation.INSERT,
            new HoodieRecordLocation("001", "file1", positions.get(i)), null))
        .collect(Collectors.toList());

    header.put(HeaderMetadataType.SCHEMA, NULL_SCHEMA);
    if (addBaseFileInstantTimeOfPositions) {
      header.put(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, "001");
    }
    HoodieLogBlock deleteBlock = new HoodieDeleteBlock(deleteRecordList, header);
    HoodieLogBlock dataBlock = new HoodieAvroDataBlock(recordList, header, "key");
    boolean hasPositions = (recordWithPositions && allValidPositions && addBaseFileInstantTimeOfPositions);
    String expectedInstantTime = hasPositions ? "001" : null;
    Set<Long> expectedPositions = hasPositions ? new HashSet<>(positions) : Collections.emptySet();
    assertEquals(expectedInstantTime, deleteBlock.getBaseFileInstantTimeOfPositions());
    TestLogReaderUtils.assertPositionEquals(expectedPositions, deleteBlock.getRecordPositions());
    assertEquals(expectedInstantTime, dataBlock.getBaseFileInstantTimeOfPositions());
    TestLogReaderUtils.assertPositionEquals(expectedPositions, dataBlock.getRecordPositions());
  }

  private static Stream<Arguments> testArguments() {
    // Arg1: ExternalSpillableMap Type, Arg2: isDiskMapCompressionEnabled, Arg3: enableOptimizedLogBlocksScan
    return Stream.of(
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false, true),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true, true)
    );
  }

  private static Stream<Arguments> testArgumentsWithoutOptimizedScanArg() {
    // Arg1: ExternalSpillableMap Type, Arg2: isDiskMapCompressionEnabled
    return Stream.of(
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, true)
    );
  }

  /**
   * Utility to convert the given iterator to a List.
   */
  private static List<IndexedRecord> getRecords(HoodieDataBlock dataBlock) {
    ClosableIterator<HoodieRecord<IndexedRecord>> itr = dataBlock.getRecordIterator(HoodieRecordType.AVRO);

    List<IndexedRecord> elements = new ArrayList<>();
    itr.forEachRemaining(r -> elements.add(r.getData()));
    return elements;
  }

  private static List<IndexedRecord> sort(List<IndexedRecord> records) {
    List<IndexedRecord> copy = new ArrayList<>(records);
    copy.sort(Comparator.comparing(r -> ((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()));
    return copy;
  }

  private HoodieLogFormat.Reader createCorruptedFile(String fileId) throws Exception {
    // block is corrupted, but check is skipped.
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId(fileId).withInstantTime("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(DEFAULT_DATA_BLOCK_TYPE, records, header);
    writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbitrary byte[] to the end of the log (mimics a partially written commit)
    FSDataOutputStream outputStream =
        (FSDataOutputStream) storage.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(473);
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    outputStream.writeInt(10000); // an invalid block type

    // Write out a length that does not confirm with the content
    outputStream.writeLong(400);
    // Write out incomplete content
    outputStream.write(getUTF8Bytes("something-random"));
    outputStream.flush();
    outputStream.close();

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(storage, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());

    assertTrue(reader.hasNext(), "First block should be available");
    reader.next();

    return reader;
  }

  private void checkLogBlocksAndKeys(String latestInstantTime, HoodieSchema schema, ExternalSpillableMap.DiskMapType diskMapType,
                                     boolean isCompressionEnabled, boolean enableOptimizedLogBlocksScan, int expectedTotalRecords,
                                     int expectedTotalKeys, Option<Set<String>> expectedKeys) throws IOException {
    List<String> allLogFiles =
        FSUtils.getAllLogFiles(storage, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner.Builder builder = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime(latestInstantTime)
        .withMaxMemorySizeInBytes(10240L)
        .withReverseReader(false)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableBasePath)
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(isCompressionEnabled)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan);
    try (HoodieMergedLogRecordScanner scanner = builder.build()) {
      assertEquals(expectedTotalRecords, scanner.getTotalLogRecords(), "There should be " + expectedTotalRecords + " records");
      final Set<String> readKeys = new HashSet<>();
      scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
      assertEquals(expectedTotalKeys, readKeys.size(), "Read should return return all " + expectedTotalKeys + " keys");

      if (expectedKeys.isPresent()) {
        assertEquals(expectedKeys.get(), readKeys, "Keys read from log file should match written keys");
      }
    }
  }
}
