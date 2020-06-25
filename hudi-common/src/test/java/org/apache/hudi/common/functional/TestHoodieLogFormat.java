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
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFileReader;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.MiniClusterUtil;
import org.apache.hudi.exception.CorruptedLogFileException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie log format {@link HoodieLogFormat}.
 */
@SuppressWarnings("Duplicates")
public abstract class TestHoodieLogFormat extends HoodieCommonTestHarness {

  private static String BASE_OUTPUT_PATH = "/tmp/";
  private FileSystem fs;
  private Path partitionPath;
  private int bufferSize = 4096;
  private HoodieLogBlockType dataBlockType;

  public TestHoodieLogFormat(HoodieLogBlockType dataBlockType) {
    this.dataBlockType = dataBlockType;
  }

  private TestHoodieLogFormat() {
  }

  @BeforeAll
  public static void setUpClass() throws IOException, InterruptedException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    MiniClusterUtil.setUp();
  }

  @AfterAll
  public static void tearDownClass() {
    MiniClusterUtil.shutdown();
  }

  @BeforeEach
  public void setUp() throws IOException, InterruptedException {
    this.fs = MiniClusterUtil.fileSystem;

    assertTrue(fs.mkdirs(new Path(tempDir.toAbsolutePath().toString())));
    this.partitionPath = new Path(tempDir.toAbsolutePath().toString());
    this.basePath = tempDir.getParent().toString();
    HoodieTestUtils.init(MiniClusterUtil.configuration, basePath, HoodieTableType.MERGE_ON_READ);
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(partitionPath, true);
  }

  @Test
  public void testEmptyLog() throws IOException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    assertEquals(0, writer.getCurrentSize(), "Just created this log, size should be 0");
    assertTrue(writer.getLogFile().getFileName().startsWith("."), "Check all log files should start with a .");
    assertEquals(1, writer.getLogFile().getLogVersion(), "Version should be 1 for new log created");
  }

  @Test
  public void testBasicAppend() throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size = writer.getCurrentSize();
    assertTrue(size > 0, "We just wrote a block - size should be > 0");
    assertEquals(size, fs.getFileStatus(writer.getLogFile().getPath()).getLen(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    writer.close();
  }

  @Test
  public void testRollover() throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    // Write out a block
    writer = writer.appendBlock(dataBlock);
    // Get the size of the block
    long size = writer.getCurrentSize();
    writer.close();

    // Create a writer with the size threshold as the size we just wrote - so this has to roll
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).withSizeThreshold(size - 1).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    assertEquals(0, writer.getCurrentSize(), "This should be a new log file and hence size should be 0");
    assertEquals(2, writer.getLogFile().getLogVersion(), "Version should be rolled to 2");
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
    HoodieLogFormat.WriterBuilder builder1 = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1").overBaseCommit("100").withFs(fs);
    HoodieLogFormat.WriterBuilder builder2 = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1").overBaseCommit("100").withFs(fs);

    if (newLogFileFormat && logFileExists) {
      // Assume there is an existing log-file with write token
      builder1 = builder1.withLogVersion(1).withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN)
          .withRolloverLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
      builder2 = builder2.withLogVersion(1).withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN)
          .withRolloverLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    } else if (newLogFileFormat) {
      // First log file of the file-slice
      builder1 = builder1.withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
          .withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN)
          .withRolloverLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
      builder2 = builder2.withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
          .withLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN)
          .withRolloverLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    } else {
      builder1 = builder1.withLogVersion(1).withRolloverLogWriteToken(HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    }
    Writer writer = builder1.build();
    Writer writer2 = builder2.build();
    HoodieLogFile logFile1 = writer.getLogFile();
    HoodieLogFile logFile2 = writer2.getLogFile();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer2 = writer2.appendBlock(dataBlock);
    writer.close();
    writer2.close();
    assertNotNull(logFile1.getLogWriteToken());
    assertEquals(logFile1.getLogVersion(), logFile2.getLogVersion() - 1, "Log Files must have different versions");
  }

  @Test
  public void testMultipleAppend() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size1 = writer.getCurrentSize();
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size2 = writer.getCurrentSize();
    assertTrue(size2 > size1, "We just wrote a new block - size2 should be > size1");
    assertEquals(size2, fs.getFileStatus(writer.getLogFile().getPath()).getLen(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size3 = writer.getCurrentSize();
    assertTrue(size3 > size2, "We just wrote a new block - size3 should be > size2");
    assertEquals(size3, fs.getFileStatus(writer.getLogFile().getPath()).getLen(),
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match");
    writer.close();

    // Cannot get the current size after closing the log
    final Writer closedWriter = writer;
    assertThrows(IllegalStateException.class, () -> {
      closedWriter.getCurrentSize();
    }, "getCurrentSize should fail after the logAppender is closed");
  }

  /*
   * This is actually a test on concurrent append and not recovery lease. Commenting this out.
   * https://issues.apache.org/jira/browse/HUDI-117
   */
  /**
   * @Test public void testLeaseRecovery() throws IOException, URISyntaxException, InterruptedException { Writer writer
   *       = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
   *       .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
   *       .overBaseCommit("100").withFs(fs).build(); List<IndexedRecord> records =
   *       SchemaTestUtil.generateTestRecords(0, 100); Map<HoodieLogBlock.HeaderMetadataType, String> header =
   *       Maps.newHashMap(); header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
   *       header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString()); HoodieAvroDataBlock
   *       dataBlock = new HoodieAvroDataBlock(records, header); writer = writer.appendBlock(dataBlock); long size1 =
   *       writer.getCurrentSize(); // do not close this writer - this simulates a data note appending to a log dying
   *       without closing the file // writer.close();
   *
   *       writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
   *       .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1").overBaseCommit("100")
   *       .withFs(fs).build(); records = SchemaTestUtil.generateTestRecords(0, 100);
   *       header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString()); dataBlock = new
   *       HoodieAvroDataBlock(records, header); writer = writer.appendBlock(dataBlock); long size2 =
   *       writer.getCurrentSize(); assertTrue("We just wrote a new block - size2 should be > size1", size2 > size1);
   *       assertEquals("Write should be auto-flushed. The size reported by FileStatus and the writer should match",
   *       size2, fs.getFileStatus(writer.getLogFile().getPath()).getLen()); writer.close(); }
   */

  @Test
  public void testAppendNotSupported() throws IOException, URISyntaxException, InterruptedException {
    // Use some fs like LocalFileSystem, that does not support appends
    Path localPartitionPath = new Path("file://" + partitionPath);
    FileSystem localFs = FSUtils.getFs(localPartitionPath.toString(), HoodieTestUtils.getDefaultHadoopConf());
    Path testPath = new Path(localPartitionPath, "append_test");
    localFs.mkdirs(testPath);

    // Some data & append two times.
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);

    for (int i = 0; i < 2; i++) {
      HoodieLogFormat.newWriterBuilder().onParentPath(testPath)
          .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION).withFileId("commits.archive").overBaseCommit("")
          .withFs(localFs).build().appendBlock(dataBlock).close();
    }

    // ensure there are two log file versions, with same data.
    FileStatus[] statuses = localFs.listStatus(testPath);
    assertEquals(2, statuses.length);
  }

  @Test
  public void testBasicWriteAndScan() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords = records.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "We wrote a block, we should be able to read it");
    HoodieLogBlock nextBlock = reader.next();
    assertEquals(dataBlockType, nextBlock.getBlockType(), "The next block should be a data block");
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;
    assertEquals(copyOfRecords.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");
    reader.close();
  }

  @Test
  public void testBasicAppendAndRead() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    Schema schema = getSimpleSchema();
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue(reader.hasNext(), "First block should be available");
    HoodieLogBlock nextBlock = reader.next();
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) nextBlock;
    assertEquals(copyOfRecords1.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords1, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");
    assertEquals(dataBlockRead.getSchema(), getSimpleSchema());

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieDataBlock) nextBlock;
    assertEquals(copyOfRecords2.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords2, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieDataBlock) nextBlock;
    assertEquals(copyOfRecords3.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords3, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");
    reader.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBasicAppendAndScanMultipleFiles(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withSizeThreshold(1024).withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());

    Set<HoodieLogFile> logFiles = new HashSet<>();
    List<List<IndexedRecord>> allRecords = new ArrayList<>();
    // create 4 log files
    while (writer.getLogFile().getLogVersion() != 4) {
      logFiles.add(writer.getLogFile());
      List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
      List<IndexedRecord> copyOfRecords1 = records1.stream()
          .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
      allRecords.add(copyOfRecords1);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieDataBlock dataBlock = getDataBlock(records1, header);
      writer = writer.appendBlock(dataBlock);
    }
    writer.close();

    // scan all log blocks (across multiple log files)
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath,
        logFiles.stream().map(logFile -> logFile.getPath().toString()).collect(Collectors.toList()), schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);

    List<IndexedRecord> scannedRecords = new ArrayList<>();
    for (HoodieRecord record : scanner) {
      scannedRecords.add((IndexedRecord) record.getData().getInsertValue(schema).get());
    }

    assertEquals(scannedRecords.size(), allRecords.stream().mapToLong(Collection::size).sum(),
        "Scanner records count should be the same as appended records");

  }

  @Test
  public void testAppendAndReadOnCorruptedLog() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(474);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(400);
    // Write out incomplete content
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Append a proper block that is of the missing length of the corrupted block
    writer =
            HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
                    .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 10);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
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
    outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(500);
    // Write out some bytes
    outputStream.write("something-else-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Should be able to append a new block
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Second round of reads - we should be able to read the first and last block
    reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderBasic(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).withSizeThreshold(500).build();
    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    copyOfRecords1.addAll(copyOfRecords2);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 200 records from 2 versions");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithRollbackTombstone(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);

    // Rollback the last write
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(commandBlock);

    // Write 3
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    List<IndexedRecord> records3 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "102",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(200, scanner.getTotalLogRecords(), "We read 200 records from 2 write batches");
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 200 records from 2 versions");
  }

  @Test
  public void testAvroLogRecordReaderWithRollbackPartialBlock()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(1000);

    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());

    // Write out some header
    outputStream.write(HoodieLogBlock.getLogMetadataBytes(header));
    outputStream.writeLong("something-random".getBytes().length);
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Rollback the last write
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    writer = writer.appendBlock(commandBlock);

    // Write 3
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    List<IndexedRecord> records3 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "103",
        10240L, true, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(200, scanner.getTotalLogRecords(), "We would read 200 records");
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 200 records from 2 versions");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithDeleteAndRollback(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);

    copyOfRecords1.addAll(copyOfRecords2);
    List<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

    // Delete 50 keys
    List<HoodieKey> deletedKeys = copyOfRecords1.stream()
        .map(s -> (new HoodieKey(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "102");
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deletedKeys.toArray(new HoodieKey[50]), header);
    writer = writer.appendBlock(deleteBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "102",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(200, scanner.getTotalLogRecords(), "We still would read 200 records");
    final List<String> readKeys = new ArrayList<>(200);
    final List<Boolean> emptyPayloads = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    scanner.forEach(s -> {
      try {
        if (!s.getData().getInsertValue(schema).isPresent()) {
          emptyPayloads.add(true);
        }
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    assertEquals(50, emptyPayloads.size(), "Stream collect should return all 50 records with empty payloads");
    originalKeys.removeAll(deletedKeys);
    Collections.sort(originalKeys);
    Collections.sort(readKeys);
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 150 records from 2 versions");

    // Rollback the last block
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    readKeys.clear();
    scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "101", 10240L, readBlocksLazily,
        false, bufferSize, BASE_OUTPUT_PATH);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records after rollback of delete");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithFailedRollbacks(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a Data block and Delete block with same InstantTime (written in same batch)
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);

    // Delete 50 keys
    // Delete 50 keys
    List<HoodieKey> deletedKeys = copyOfRecords1.stream()
        .map(s -> (new HoodieKey(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);

    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deletedKeys.toArray(new HoodieKey[50]), header);
    writer = writer.appendBlock(deleteBlock);

    // Attempt 1 : Write rollback block for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    try {
      writer = writer.appendBlock(commandBlock);
      // Say job failed, retry writing 2 rollback in the next rollback(..) attempt
      throw new Exception("simulating failure");
    } catch (Exception e) {
      // it's okay
    }
    // Attempt 2 : Write another rollback blocks for a failed write
    writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    // all data must be rolled back before merge
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(0, scanner.getTotalLogRecords(), "We would have scanned 0 records because of rollback");

    final List<String> readKeys = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(0, readKeys.size(), "Stream collect should return all 0 records");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithInsertDeleteAndRollback(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a Data block and Delete block with same InstantTime (written in same batch)
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Delete 50 keys
    List<HoodieKey> deletedKeys = copyOfRecords1.stream()
        .map(s -> (new HoodieKey(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deletedKeys.toArray(new HoodieKey[50]), header);
    writer = writer.appendBlock(deleteBlock);

    // Write 2 rollback blocks (1 data block + 1 delete bloc) for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(commandBlock);
    writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(0, scanner.getTotalLogRecords(), "We would read 0 records");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithInvalidRollback(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write invalid rollback for a failed write (possible for in-flight commits)
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(100, scanner.getTotalLogRecords(), "We still would read 100 records");
    final List<String> readKeys = new ArrayList<>(100);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals(100, readKeys.size(), "Stream collect should return all 150 records");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithInsertsDeleteAndRollback(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a 3 Data blocs with same InstantTime (written in same batch)
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);

    // Delete 50 keys
    // Delete 50 keys
    List<HoodieKey> deletedKeys = copyOfRecords1.stream()
        .map(s -> (new HoodieKey(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deletedKeys.toArray(new HoodieKey[50]), header);
    writer = writer.appendBlock(deleteBlock);

    // Write 1 rollback block for a failed write
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "101",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(0, scanner.getTotalLogRecords(), "We would read 0 records");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {

    // Write a 3 Data blocs with same InstantTime (written in same batch)
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);

    writer.close();
    // Append some arbit byte[] to the end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(100);
    outputStream.flush();
    outputStream.close();

    // Append some arbit byte[] to the end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    outputStream = fs.append(writer.getLogFile().getPath());
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
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to the end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    outputStream = fs.append(writer.getLogFile().getPath());
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
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    // Write 1 rollback block for the last commit instant
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(commandBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "101",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals(0, scanner.getTotalLogRecords(), "We would read 0 records");
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
      boolean readBlocksLazily) {
    try {
      // Write one Data block with same InstantTime (written in same batch)
      Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
      List<IndexedRecord> records = SchemaTestUtil.generateHoodieTestRecords(0, 101);
      List<IndexedRecord> records2 = new ArrayList<>(records);

      // Write1 with numRecordsInLog1 records written to log.1
      Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
          .overBaseCommit("100").withFs(fs).build();

      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieDataBlock dataBlock = getDataBlock(records.subList(0, numRecordsInLog1), header);
      writer = writer.appendBlock(dataBlock);
      // Get the size of the block
      long size = writer.getCurrentSize();
      writer.close();

      // write2 with numRecordsInLog2 records written to log.2
      Writer writer2 = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
          .overBaseCommit("100").withFs(fs).withSizeThreshold(size - 1).build();

      Map<HoodieLogBlock.HeaderMetadataType, String> header2 = new HashMap<>();
      header2.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
      header2.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieDataBlock dataBlock2 = getDataBlock(records2.subList(0, numRecordsInLog2), header2);
      writer2 = writer2.appendBlock(dataBlock2);
      // Get the size of the block
      writer2.close();

      // From the two log files generated, read the records
      List<String> allLogFiles = FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1",
          HoodieLogFile.DELTA_EXTENSION, "100").map(s -> s.getPath().toString()).collect(Collectors.toList());

      HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema,
          "100", 10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);

      assertEquals(Math.max(numRecordsInLog1, numRecordsInLog2), scanner.getNumMergedRecordsInLog(),
          "We would read 100 records");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithFailedTaskInFirstStageAttempt(boolean readBlocksLazily) {
    /*
     * FIRST_ATTEMPT_FAILED:
     * Original task from the stage attempt failed, but subsequent stage retry succeeded.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(77, 100, readBlocksLazily);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderWithFailedTaskInSecondStageAttempt(boolean readBlocksLazily) {
    /*
     * SECOND_ATTEMPT_FAILED:
     * Original task from stage attempt succeeded, but subsequent retry attempt failed.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(100, 66, readBlocksLazily);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAvroLogRecordReaderTasksSucceededInBothStageAttempts(boolean readBlocksLazily) {
    /*
     * BOTH_ATTEMPTS_SUCCEEDED:
     * Original task from the stage attempt and duplicate task from the stage retry succeeded.
     */
    testAvroLogRecordReaderMergingMultipleLogFiles(100, 100, readBlocksLazily);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBasicAppendAndReadInReverse(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = getDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    HoodieLogFileReader reader = new HoodieLogFileReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema(),
        bufferSize, readBlocksLazily, true);

    assertTrue(reader.hasPrev(), "Last block should be available");
    HoodieLogBlock prevBlock = reader.prev();
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) prevBlock;

    assertEquals(copyOfRecords3.size(), dataBlockRead.getRecords().size(),
        "Third records size should be equal to the written records size");
    assertEquals(copyOfRecords3, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");

    assertTrue(reader.hasPrev(), "Second block should be available");
    prevBlock = reader.prev();
    dataBlockRead = (HoodieDataBlock) prevBlock;
    assertEquals(copyOfRecords2.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords2, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");

    assertTrue(reader.hasPrev(), "First block should be available");
    prevBlock = reader.prev();
    dataBlockRead = (HoodieDataBlock) prevBlock;
    assertEquals(copyOfRecords1.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords1, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");

    assertFalse(reader.hasPrev());
    reader.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAppendAndReadOnCorruptedLogInReverse(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    // Write out a length that does not confirm with the content
    outputStream.writeInt(1000);
    // Write out footer length
    outputStream.writeInt(1);
    // Write out some metadata
    // TODO : test for failure to write metadata - NA ?
    outputStream.write(HoodieLogBlock.getLogMetadataBytes(header));
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Should be able to append a new block
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // First round of reads - we should be able to read the first block and then EOF
    HoodieLogFileReader reader =
        new HoodieLogFileReader(fs, writer.getLogFile(), schema, bufferSize, readBlocksLazily, true);

    assertTrue(reader.hasPrev(), "Last block should be available");
    HoodieLogBlock block = reader.prev();
    assertTrue(block instanceof HoodieDataBlock, "Last block should be datablock");

    assertTrue(reader.hasPrev(), "Last block should be available");
    assertThrows(CorruptedLogFileException.class, () -> {
      reader.prev();
    });
    reader.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBasicAppendAndTraverseInReverse(boolean readBlocksLazily)
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = getDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    HoodieLogFileReader reader = new HoodieLogFileReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema(),
        bufferSize, readBlocksLazily, true);

    assertTrue(reader.hasPrev(), "Third block should be available");
    reader.moveToPrev();

    assertTrue(reader.hasPrev(), "Second block should be available");
    reader.moveToPrev();

    // After moving twice, this last reader.prev() should read the First block written
    assertTrue(reader.hasPrev(), "First block should be available");
    HoodieLogBlock prevBlock = reader.prev();
    HoodieDataBlock dataBlockRead = (HoodieDataBlock) prevBlock;
    assertEquals(copyOfRecords1.size(), dataBlockRead.getRecords().size(),
        "Read records size should be equal to the written records size");
    assertEquals(copyOfRecords1, dataBlockRead.getRecords(),
        "Both records lists should be the same. (ordering guaranteed)");

    assertFalse(reader.hasPrev());
    reader.close();
  }

  @Test
  public void testV0Format() throws IOException, URISyntaxException {
    // HoodieLogFormatVersion.DEFAULT_VERSION has been deprecated so we cannot
    // create a writer for it. So these tests are only for the HoodieAvroDataBlock
    // of older version.
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> recordsCopy = new ArrayList<>(records);
    assertEquals(100, records.size());
    assertEquals(100, recordsCopy.size());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, schema);
    byte[] content = dataBlock.getBytes(schema);
    assertTrue(content.length > 0);

    HoodieLogBlock logBlock = HoodieAvroDataBlock.getBlock(content, schema);
    assertEquals(HoodieLogBlockType.AVRO_DATA_BLOCK, logBlock.getBlockType());
    List<IndexedRecord> readRecords = ((HoodieAvroDataBlock) logBlock).getRecords();
    assertEquals(readRecords.size(), recordsCopy.size());
    for (int i = 0; i < recordsCopy.size(); ++i) {
      assertEquals(recordsCopy.get(i), readRecords.get(i));
    }

    // Reader schema is optional if it is same as write schema
    logBlock = HoodieAvroDataBlock.getBlock(content, null);
    assertEquals(HoodieLogBlockType.AVRO_DATA_BLOCK, logBlock.getBlockType());
    readRecords = ((HoodieAvroDataBlock) logBlock).getRecords();
    assertEquals(readRecords.size(), recordsCopy.size());
    for (int i = 0; i < recordsCopy.size(); ++i) {
      assertEquals(recordsCopy.get(i), readRecords.get(i));
    }
  }

  private HoodieDataBlock getDataBlock(List<IndexedRecord> records, Map<HeaderMetadataType, String> header) {
    switch (dataBlockType) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(records, header);
      default:
        throw new RuntimeException("Unknown data block type " + dataBlockType);
    }
  }
}
