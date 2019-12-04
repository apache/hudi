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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.minicluster.MiniClusterUtil;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.exception.CorruptedLogFileException;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.SchemaTestUtil.getSimpleSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests hoodie log format {@link HoodieLogFormat}.
 */
@SuppressWarnings("Duplicates")
@RunWith(Parameterized.class)
public class TestHoodieLogFormat extends HoodieCommonTestHarness {

  private static String BASE_OUTPUT_PATH = "/tmp/";
  private FileSystem fs;
  private Path partitionPath;
  private int bufferSize = 4096;
  private Boolean readBlocksLazily = true;

  public TestHoodieLogFormat(Boolean readBlocksLazily) {
    this.readBlocksLazily = readBlocksLazily;
  }

  @Parameterized.Parameters(name = "LogBlockReadMode")
  public static Collection<Boolean[]> data() {
    return Arrays.asList(new Boolean[][] {{true}, {false}});
  }

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    MiniClusterUtil.setUp();
  }

  @AfterClass
  public static void tearDownClass() {
    MiniClusterUtil.shutdown();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    this.fs = MiniClusterUtil.fileSystem;

    assertTrue(fs.mkdirs(new Path(folder.getRoot().getPath())));
    this.partitionPath = new Path(folder.getRoot().getPath());
    this.basePath = folder.getRoot().getParent();
    HoodieTestUtils.init(MiniClusterUtil.configuration, basePath, HoodieTableType.MERGE_ON_READ);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(partitionPath, true);
  }

  @Test
  public void testEmptyLog() throws IOException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    assertEquals("Just created this log, size should be 0", 0, writer.getCurrentSize());
    assertTrue("Check all log files should start with a .", writer.getLogFile().getFileName().startsWith("."));
    assertEquals("Version should be 1 for new log created", 1, writer.getLogFile().getLogVersion());
  }

  @Test
  public void testBasicAppend() throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size = writer.getCurrentSize();
    assertTrue("We just wrote a block - size should be > 0", size > 0);
    assertEquals("Write should be auto-flushed. The size reported by FileStatus and the writer should match", size,
        fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();
  }

  @Test
  public void testRollover() throws IOException, InterruptedException, URISyntaxException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
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
    dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    assertEquals("This should be a new log file and hence size should be 0", 0, writer.getCurrentSize());
    assertEquals("Version should be rolled to 2", 2, writer.getLogFile().getLogVersion());
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer2 = writer2.appendBlock(dataBlock);
    writer.close();
    writer2.close();
    assertNotNull(logFile1.getLogWriteToken());
    assertEquals("Log Files must have different versions", logFile1.getLogVersion(), logFile2.getLogVersion() - 1);
  }

  @Test
  public void testMultipleAppend() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size1 = writer.getCurrentSize();
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size2 = writer.getCurrentSize();
    assertTrue("We just wrote a new block - size2 should be > size1", size2 > size1);
    assertEquals("Write should be auto-flushed. The size reported by FileStatus and the writer should match", size2,
        fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    long size3 = writer.getCurrentSize();
    assertTrue("We just wrote a new block - size3 should be > size2", size3 > size2);
    assertEquals("Write should be auto-flushed. The size reported by FileStatus and the writer should match", size3,
        fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();

    // Cannot get the current size after closing the log
    try {
      writer.getCurrentSize();
      fail("getCurrentSize should fail after the logAppender is closed");
    } catch (IllegalStateException e) {
      // pass
    }
  }

  /**
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);

    for (int i = 0; i < 2; i++) {
      HoodieLogFormat.newWriterBuilder().onParentPath(testPath)
          .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION).withFileId("commits.archive").overBaseCommit("")
          .withFs(localFs).build().appendBlock(dataBlock).close();
    }

    // ensure there are two log file versions, with same data.
    FileStatus[] statuses = localFs.listStatus(testPath);
    assertEquals(2, statuses.length);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicWriteAndScan() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords = records.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("We wrote a block, we should be able to read it", reader.hasNext());
    HoodieLogBlock nextBlock = reader.next();
    assertEquals("The next block should be a data block", HoodieLogBlockType.AVRO_DATA_BLOCK, nextBlock.getBlockType());
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords,
        dataBlockRead.getRecords());
    reader.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicAppendAndRead() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    Schema schema = getSimpleSchema();
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    dataBlock = new HoodieAvroDataBlock(records2, header);
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
    dataBlock = new HoodieAvroDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("First block should be available", reader.hasNext());
    HoodieLogBlock nextBlock = reader.next();
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords1.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords1,
        dataBlockRead.getRecords());

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords2.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords2,
        dataBlockRead.getRecords());

    reader.hasNext();
    nextBlock = reader.next();
    dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords3.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords3,
        dataBlockRead.getRecords());
    reader.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicAppendAndScanMultipleFiles() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withSizeThreshold(1024).withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
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
      HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
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

    assertEquals("Scanner records count should be the same as appended records", scannedRecords.size(),
        allRecords.stream().flatMap(records -> records.stream()).collect(Collectors.toList()).size());

  }

  @Test
  public void testAppendAndReadOnCorruptedLog() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FSUtils.getFs(fs.getUri().toString(), fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(1000);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    outputStream.writeInt(HoodieLogFormat.CURRENT_VERSION);
    // Write out a length that does not confirm with the content
    outputStream.writeLong(500);
    // Write out some bytes
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("First block should be available", reader.hasNext());
    reader.next();
    assertTrue("We should have corrupted block next", reader.hasNext());
    HoodieLogBlock block = reader.next();
    assertEquals("The read block should be a corrupt block", HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType());
    HoodieCorruptBlock corruptBlock = (HoodieCorruptBlock) block;
    // assertEquals("", "something-random", new String(corruptBlock.getCorruptedBytes()));
    assertFalse("There should be no more block left", reader.hasNext());

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
    dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Second round of reads - we should be able to read the first and last block
    reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("First block should be available", reader.hasNext());
    reader.next();
    assertTrue("We should get the 1st corrupted block next", reader.hasNext());
    reader.next();
    assertTrue("We should get the 2nd corrupted block next", reader.hasNext());
    block = reader.next();
    assertEquals("The read block should be a corrupt block", HoodieLogBlockType.CORRUPT_BLOCK, block.getBlockType());
    corruptBlock = (HoodieCorruptBlock) block;
    // assertEquals("", "something-else-random", new String(corruptBlock.getCorruptedBytes()));
    assertTrue("We should get the last block next", reader.hasNext());
    reader.next();
    assertFalse("We should have no more blocks left", reader.hasNext());
    reader.close();
  }

  @Test
  public void testAvroLogRecordReaderBasic() throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).withSizeThreshold(500).build();
    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());

    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = new HoodieAvroDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("", 200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    copyOfRecords1.addAll(copyOfRecords2);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys, readKeys);
  }

  @Test
  public void testAvroLogRecordReaderWithRollbackTombstone()
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = new HoodieAvroDataBlock(records2, header);
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
    dataBlock = new HoodieAvroDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "102",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We read 200 records from 2 write batches", 200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys, readKeys);
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
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
    dataBlock = new HoodieAvroDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "103",
        10240L, true, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We would read 200 records", 200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    copyOfRecords1.addAll(copyOfRecords3);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys, readKeys);
  }

  @Test
  public void testAvroLogRecordReaderWithDeleteAndRollback()
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = new HoodieAvroDataBlock(records2, header);
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
    assertEquals("We still would read 200 records", 200, scanner.getTotalLogRecords());
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
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    assertEquals("Stream collect should return all 50 records with empty payloads", 50, emptyPayloads.size());
    originalKeys.removeAll(deletedKeys);
    Collections.sort(originalKeys);
    Collections.sort(readKeys);
    assertEquals("CompositeAvroLogReader should return 150 records from 2 versions", originalKeys, readKeys);

    // Rollback the last block
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "102");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(commandBlock);

    readKeys.clear();
    scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "101", 10240L, readBlocksLazily,
        false, bufferSize, BASE_OUTPUT_PATH);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records after rollback of delete", 200, readKeys.size());
  }

  @Test
  public void testAvroLogRecordReaderWithFailedRollbacks()
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");

    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = new HoodieAvroDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);

    List<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

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
    writer = writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    // all data must be rolled back before merge
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We would have scanned 0 records because of rollback", 0, scanner.getTotalLogRecords());

    final List<String> readKeys = new ArrayList<>();
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 0 records", 0, readKeys.size());
  }

  @Test
  public void testAvroLogRecordReaderWithInsertDeleteAndRollback()
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    List<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

    // Delete 50 keys
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
    writer = writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We would read 0 records", 0, scanner.getTotalLogRecords());
  }

  @Test
  public void testAvroLogRecordReaderWithInvalidRollback()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);

    // Write invalid rollback for a failed write (possible for in-flight commits)
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "101");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "100",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We still would read 100 records", 100, scanner.getTotalLogRecords());
    final List<String> readKeys = new ArrayList<>(100);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 150 records", 100, readKeys.size());
  }

  @Test
  public void testAvroLogRecordReaderWithInsertsDeleteAndRollback()
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
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);
    writer = writer.appendBlock(dataBlock);

    List<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toList());

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
    writer = writer.appendBlock(commandBlock);

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, basePath, allLogFiles, schema, "101",
        10240L, readBlocksLazily, false, bufferSize, BASE_OUTPUT_PATH);
    assertEquals("We would read 0 records", 0, scanner.getTotalLogRecords());
  }

  @Test
  public void testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback()
      throws IOException, URISyntaxException, InterruptedException {

    // Write a 3 Data blocs with same InstantTime (written in same batch)
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
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
    assertEquals("We would read 0 records", 0, scanner.getTotalLogRecords());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicAppendAndReadInReverse() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = new HoodieAvroDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = new HoodieAvroDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    HoodieLogFileReader reader = new HoodieLogFileReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema(),
        bufferSize, readBlocksLazily, true);

    assertTrue("Last block should be available", reader.hasPrev());
    HoodieLogBlock prevBlock = reader.prev();
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) prevBlock;

    assertEquals("Third records size should be equal to the written records size", copyOfRecords3.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords3,
        dataBlockRead.getRecords());

    assertTrue("Second block should be available", reader.hasPrev());
    prevBlock = reader.prev();
    dataBlockRead = (HoodieAvroDataBlock) prevBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords2.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords2,
        dataBlockRead.getRecords());

    assertTrue("First block should be available", reader.hasPrev());
    prevBlock = reader.prev();
    dataBlockRead = (HoodieAvroDataBlock) prevBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords1.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords1,
        dataBlockRead.getRecords());

    assertFalse(reader.hasPrev());
    reader.close();
  }

  @Test
  public void testAppendAndReadOnCorruptedLogInReverse() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
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
    dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // First round of reads - we should be able to read the first block and then EOF
    HoodieLogFileReader reader =
        new HoodieLogFileReader(fs, writer.getLogFile(), schema, bufferSize, readBlocksLazily, true);

    assertTrue("Last block should be available", reader.hasPrev());
    HoodieLogBlock block = reader.prev();
    assertTrue("Last block should be datablock", block instanceof HoodieAvroDataBlock);

    assertTrue("Last block should be available", reader.hasPrev());
    try {
      reader.prev();
    } catch (CorruptedLogFileException e) {
      e.printStackTrace();
      // We should have corrupted block
    }
    reader.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicAppendAndTraverseInReverse() throws IOException, URISyntaxException, InterruptedException {
    Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    Schema schema = getSimpleSchema();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = new HoodieAvroDataBlock(records2, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords3 = records3.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    dataBlock = new HoodieAvroDataBlock(records3, header);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    HoodieLogFileReader reader = new HoodieLogFileReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema(),
        bufferSize, readBlocksLazily, true);

    assertTrue("Third block should be available", reader.hasPrev());
    reader.moveToPrev();

    assertTrue("Second block should be available", reader.hasPrev());
    reader.moveToPrev();

    // After moving twice, this last reader.prev() should read the First block written
    assertTrue("First block should be available", reader.hasPrev());
    HoodieLogBlock prevBlock = reader.prev();
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) prevBlock;
    assertEquals("Read records size should be equal to the written records size", copyOfRecords1.size(),
        dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", copyOfRecords1,
        dataBlockRead.getRecords());

    assertFalse(reader.hasPrev());
    reader.close();
  }
}
