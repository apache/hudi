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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link HoodieLogFileCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestHoodieLogFileCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String partitionPath;
  private HoodieAvroDataBlock dataBlock;
  private HoodieCommandBlock commandBlock;
  private String tablePath;
  private HoodieStorage storage;

  private static final String INSTANT_TIME = "100";

  @BeforeEach
  public void init() throws IOException, InterruptedException, URISyntaxException {
    HoodieCLI.conf = storageConf();

    // Create table and connect
    String tableName = tableName();
    tablePath = tablePath(tableName);
    partitionPath = Paths.get(tablePath, DEFAULT_FIRST_PARTITION_PATH).toString();
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    Files.createDirectories(Paths.get(partitionPath));
    storage = HoodieStorageUtils.getStorage(tablePath, storageConf());

    try (HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(new StoragePath(partitionPath))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId("test-log-fileid1").withInstantTime("100").withStorage(storage)
        .withSizeThreshold(1).build()) {

      // write data to file
      List<HoodieRecord> records = SchemaTestUtil.generateTestRecords(0, 100).stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, INSTANT_TIME);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
      dataBlock = new HoodieAvroDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      writer.appendBlock(dataBlock);

      Map<HoodieLogBlock.HeaderMetadataType, String> rollbackHeader = new HashMap<>();
      rollbackHeader.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "103");
      rollbackHeader.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "102");
      rollbackHeader.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
          String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
      commandBlock = new HoodieCommandBlock(rollbackHeader);
      writer.appendBlock(commandBlock);
    }
  }

  @AfterEach
  public void cleanUp() throws IOException {
    storage.close();
  }

  /**
   * Test case for 'show logfile metadata'.
   */
  @Test
  public void testShowLogFileCommits() throws JsonProcessingException {
    Object result = shell.evaluate(() -> "show logfile metadata --logFilePathPattern " + partitionPath + "/*");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_RECORD_COUNT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BLOCK_TYPE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HEADER_METADATA)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FOOTER_METADATA);

    // construct expect result, there is only 1 line.
    List<Comparable[]> rows = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    String logFileNamePrefix = DEFAULT_FIRST_PARTITION_PATH + "/test-log-fileid1_" + INSTANT_TIME + ".log";
    rows.add(new Comparable[] {
        logFileNamePrefix + ".1_1-0-1", INSTANT_TIME, 100, dataBlock.getBlockType(),
        objectMapper.writeValueAsString(dataBlock.getLogBlockHeader()),
        objectMapper.writeValueAsString(dataBlock.getLogBlockFooter())});
    rows.add(new Comparable[] {
        logFileNamePrefix + ".2_1-0-1", "103", 0, commandBlock.getBlockType(),
        objectMapper.writeValueAsString(commandBlock.getLogBlockHeader()),
        objectMapper.writeValueAsString(commandBlock.getLogBlockFooter())});

    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for 'show logfile records'.
   */
  @Test
  public void testShowLogFileRecords() throws IOException, URISyntaxException {
    Object result = shell.evaluate(() -> "show logfile records --logFilePathPattern " + partitionPath + "/*");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // construct expect result, get 10 records.
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 10);
    String[][] rows = records.stream().map(r -> new String[] {r.toString()}).toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_RECORDS}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for 'show logfile records' with merge.
   */
  @Test
  public void testShowLogFileRecordsWithMerge() throws IOException, InterruptedException, URISyntaxException {
    // create commit instant
    HoodieTestCommitMetadataGenerator.createCommitFile(tablePath, INSTANT_TIME, HoodieCLI.conf);

    // write to path '2015/03/16'.
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    partitionPath = tablePath + StoragePath.SEPARATOR + HoodieTestCommitMetadataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    Files.createDirectories(Paths.get(partitionPath));

    HoodieLogFormat.Writer writer = null;
    try {
      // set little threshold to split file.
      writer =
          HoodieLogFormat.newWriterBuilder().onParentPath(new StoragePath(partitionPath))
              .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
              .withFileId("test-log-fileid1").withInstantTime(INSTANT_TIME).withStorage(
                  storage)
              .withSizeThreshold(500).build();

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<HoodieRecord> records1 = testUtil.generateHoodieTestRecords(0, 100).stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, INSTANT_TIME);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records1, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      writer.appendBlock(dataBlock);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    Object result = shell.evaluate(() -> "show logfile records --logFilePathPattern "
        + partitionPath + "/* --mergeRecords true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get expected result of 10 records.
    List<String> logFilePaths = storage.globEntries(new StoragePath(partitionPath + "/*"))
        .stream()
        .map(status -> status.getPath().toString()).collect(Collectors.toList());
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(tablePath)
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(schema)
        .withLatestInstantTime(INSTANT_TIME)
        .withMaxMemorySizeInBytes(
            HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
        .withReverseReader(
            Boolean.parseBoolean(
                HoodieReaderConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue()))
        .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue())
        .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath())
        .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue())
        .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue())
        .withOptimizedLogBlocksScan(Boolean.parseBoolean(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.defaultValue()))
        .build();

    Iterator<HoodieRecord> records = scanner.iterator();
    int num = 0;
    int maxSize = 10;
    List<IndexedRecord> indexRecords = new ArrayList<>();
    while (records.hasNext() && num < maxSize) {
      Option<IndexedRecord> hoodieRecord = ((HoodieAvroRecord)records.next()).getData().getInsertValue(schema);
      indexRecords.add(hoodieRecord.get());
      num++;
    }
    String[][] rows = indexRecords.stream().map(r -> new String[] {r.toString()}).toArray(String[][]::new);
    assertNotNull(rows);

    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_RECORDS}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }
}
