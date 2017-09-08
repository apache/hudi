/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.log;

import com.uber.hoodie.common.minicluster.MiniClusterUtil;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Reader;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.uber.hoodie.common.util.SchemaTestUtil.getSimpleSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("Duplicates")
public class HoodieLogFormatTest {

  private FileSystem fs;
  private Path partitionPath;

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
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    assertTrue(fs.mkdirs(new Path(folder.getRoot().getPath())));
    this.partitionPath = new Path(folder.getRoot().getPath());
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(partitionPath, true);
  }

  @Test
  public void testEmptyLog() throws IOException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    assertEquals("Just created this log, size should be 0", 0, writer.getCurrentSize());
    assertTrue("Check all log files should start with a .",
        writer.getLogFile().getFileName().startsWith("."));
    assertEquals("Version should be 1 for new log created", 1,
        writer.getLogFile().getLogVersion());
  }

  @Test
  public void testBasicAppend() throws IOException, InterruptedException, URISyntaxException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size = writer.getCurrentSize();
    assertTrue("We just wrote a block - size should be > 0", size > 0);
    assertEquals(
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match",
        size, fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();
  }

  @Test
  public void testRollover() throws IOException, InterruptedException, URISyntaxException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    // Write out a block
    writer = writer.appendBlock(dataBlock);
    // Get the size of the block
    long size = writer.getCurrentSize();
    writer.close();

    // Create a writer with the size threshold as the size we just wrote - so this has to roll
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).withSizeThreshold(size - 1).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    assertEquals("This should be a new log file and hence size should be 0", 0,
        writer.getCurrentSize());
    assertEquals("Version should be rolled to 2", 2, writer.getLogFile().getLogVersion());
    writer.close();
  }

  @Test
  public void testRolloverAndAppendToPreviousLog() throws IOException, InterruptedException, URISyntaxException {
    Writer oldWriter = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
            getSimpleSchema());
    // Write out a block
    oldWriter = oldWriter.appendBlock(dataBlock);
    // Get the size of the block
    long size = oldWriter.getCurrentSize();
    oldWriter.close();

    // Create a writer with the size threshold as the size we just wrote - so this has to roll
    oldWriter = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .overBaseCommit("100").withFs(fs).withSizeThreshold(size - 1).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records,
            getSimpleSchema());
    Writer newWriter = oldWriter.appendBlock(dataBlock);
    assertEquals("This should be a new log file and hence size should be 0", 0,
            newWriter.getCurrentSize());
    assertEquals("Version should be rolled to 2", 2, newWriter.getLogFile().getLogVersion());
    // Open a writer to the old log version and try to append
    try {
      oldWriter = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
              .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1").withLogVersion(1)
              .overBaseCommit("100").withFs(fs).build();
      fail("should not be able to append to a sealed log file");
    } catch(HoodieIOException he) {
      // pass
    }
    newWriter.close();
  }

  @Test
  public void testMultipleAppend() throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size1 = writer.getCurrentSize();
    writer.close();

    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size2 = writer.getCurrentSize();
    assertTrue("We just wrote a new block - size2 should be > size1", size2 > size1);
    assertEquals(
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match",
        size2, fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();

    // Close and Open again and append 100 more records
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size3 = writer.getCurrentSize();
    assertTrue("We just wrote a new block - size3 should be > size2", size3 > size2);
    assertEquals(
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match",
        size3, fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();

    // Cannot get the current size after closing the log
    try {
      writer.getCurrentSize();
      fail("getCurrentSize should fail after the logAppender is closed");
    } catch (IllegalStateException e) {
      // pass
    }
  }

  @Test
  public void testLeaseRecovery() throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size1 = writer.getCurrentSize();
    // do not close this writer - this simulates a data note appending to a log dying without closing the file
    // writer.close();

    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    long size2 = writer.getCurrentSize();
    assertTrue("We just wrote a new block - size2 should be > size1", size2 > size1);
    assertEquals(
        "Write should be auto-flushed. The size reported by FileStatus and the writer should match",
        size2, fs.getFileStatus(writer.getLogFile().getPath()).getLen());
    writer.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicWriteAndScan()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("We wrote a block, we should be able to read it", reader.hasNext());
    HoodieLogBlock nextBlock = reader.next();
    assertEquals("The next block should be a data block", HoodieLogBlockType.AVRO_DATA_BLOCK,
        nextBlock.getBlockType());
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size",
        records.size(), dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", records,
        dataBlockRead.getRecords());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasicAppendAndRead()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records1 = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    writer.close();

    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records2 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records2,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Close and Open again and append 100 more records
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records3 = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records3,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    writer.close();

    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("First block should be available", reader.hasNext());
    HoodieLogBlock nextBlock = reader.next();
    HoodieAvroDataBlock dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size",
        records1.size(), dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", records1,
        dataBlockRead.getRecords());

    nextBlock = reader.next();
    dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size",
        records2.size(), dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", records2,
        dataBlockRead.getRecords());

    nextBlock = reader.next();
    dataBlockRead = (HoodieAvroDataBlock) nextBlock;
    assertEquals("Read records size should be equal to the written records size",
        records3.size(), dataBlockRead.getRecords().size());
    assertEquals("Both records lists should be the same. (ordering guaranteed)", records3,
        dataBlockRead.getRecords());
  }

  @Test
  public void testAppendAndReadOnCorruptedLog()
      throws IOException, URISyntaxException, InterruptedException {
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records,
        getSimpleSchema());
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FileSystem.get(fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    // Write out a length that does not confirm with the content
    outputStream.writeInt(100);
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // First round of reads - we should be able to read the first block and then EOF
    Reader reader = HoodieLogFormat.newReader(fs, writer.getLogFile(), SchemaTestUtil.getSimpleSchema());
    assertTrue("First block should be available", reader.hasNext());
    reader.next();
    assertTrue("We should have corrupted block next", reader.hasNext());
    HoodieLogBlock block = reader.next();
    assertEquals("The read block should be a corrupt block", HoodieLogBlockType.CORRUPT_BLOCK,
        block.getBlockType());
    assertEquals("", "something-random", new String(block.getBytes()));
    assertFalse("There should be no more block left", reader.hasNext());

    // Simulate another failure back to back
    outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    // Write out a length that does not confirm with the content
    outputStream.writeInt(100);
    outputStream.write("something-else-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Should be able to append a new block
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    records = SchemaTestUtil.generateTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records, getSimpleSchema());
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
    assertEquals("The read block should be a corrupt block", HoodieLogBlockType.CORRUPT_BLOCK,
        block.getBlockType());
    assertEquals("", "something-else-random", new String(block.getBytes()));
    assertTrue("We should get the last block next", reader.hasNext());
    reader.next();
    assertFalse("We should have no more blocks left", reader.hasNext());
  }


  @Test
  public void testAvroLogRecordReaderBasic()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).withSizeThreshold(500).build();
    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);

    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, schema);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records2, schema);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles = FSUtils
        .getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
        .map(s -> s.getPath().toString())
        .collect(Collectors.toList());

    HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs, allLogFiles,
        schema);
    assertEquals("", 200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    records1.addAll(records2);
    Set<String> originalKeys = records1.stream()
        .map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .collect(
            Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys,
        readKeys);
  }

  @Test
  public void testAvroLogRecordReaderWithRollbackTombstone()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, schema);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records2, schema);
    writer = writer.appendBlock(dataBlock);

    // Rollback the last write
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(
        HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK);
    writer = writer.appendBlock(commandBlock);

    // Write 3
    List<IndexedRecord> records3 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records3, schema);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles = FSUtils
        .getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
        .map(s -> s.getPath().toString())
        .collect(Collectors.toList());

    HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs, allLogFiles,
        schema);
    assertEquals("We still would read 300 records, but only 200 of them are valid", 300,
        scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    records1.addAll(records3);
    Set<String> originalKeys = records1.stream()
        .map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .collect(
            Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys,
        readKeys);
  }

  @Test
  public void testAvroLogRecordReaderWithRollbackPartialBlock()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, schema);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
    fs = FileSystem.get(fs.getConf());
    FSDataOutputStream outputStream = fs.append(writer.getLogFile().getPath());
    // create a block with
    outputStream.write(HoodieLogFormat.MAGIC);
    outputStream.writeInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());
    // Write out a length that does not confirm with the content
    outputStream.writeInt(100);
    outputStream.write("something-random".getBytes());
    outputStream.flush();
    outputStream.close();

    // Rollback the last write
    HoodieCommandBlock commandBlock = new HoodieCommandBlock(
        HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK);
    writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();
    writer = writer.appendBlock(commandBlock);

    // Write 3
    List<IndexedRecord> records3 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records3, schema);
    writer = writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles = FSUtils
        .getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
        .map(s -> s.getPath().toString())
        .collect(Collectors.toList());

    HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs, allLogFiles,
        schema);
    assertEquals("We would read 200 records", 200,
        scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 200, readKeys.size());
    records1.addAll(records3);
    Set<String> originalKeys = records1.stream()
        .map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .collect(
            Collectors.toSet());
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys,
        readKeys);
  }

  @Test
  public void testAvroLogRecordReaderWithDelete()
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    // Set a small threshold so that every block is a new version
    Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
        .overBaseCommit("100").withFs(fs).build();

    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, schema);
    writer = writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    dataBlock = new HoodieAvroDataBlock(records2, schema);
    writer = writer.appendBlock(dataBlock);

    records1.addAll(records2);
    List<String> originalKeys = records1.stream()
        .map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
        .collect(
            Collectors.toList());

    // Delete 50 keys
    List<String> deletedKeys = originalKeys.subList(0, 50);
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deletedKeys.toArray(new String[50]));
    writer = writer.appendBlock(deleteBlock);

    List<String> allLogFiles = FSUtils
        .getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
        .map(s -> s.getPath().toString())
        .collect(Collectors.toList());

    HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs, allLogFiles,
        schema);
    assertEquals("We still would read 200 records", 200,
        scanner.getTotalLogRecords());
    List<String> readKeys = new ArrayList<>(200);
    scanner.forEach(s -> readKeys.add(s.getKey().getRecordKey()));
    assertEquals("Stream collect should return all 200 records", 150, readKeys.size());
    originalKeys.removeAll(deletedKeys);
    Collections.sort(originalKeys);
    Collections.sort(readKeys);
    assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", originalKeys,
        readKeys);
  }
}
