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

package org.apache.hudi.client.functional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieAvroHFileReader;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataMergedLogRecordReader;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieBackedTableMetadata extends TestHoodieMetadataBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieBackedTableMetadata.class);

  @Test
  public void testTableOperations() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    doWriteInsertAndUpsert(testTable);

    // trigger an upsert
    doWriteOperation(testTable, "0000003");
    verifyBaseMetadataTable();
  }

  private void doWriteInsertAndUpsert(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000001", "0000002", false);
  }

  private void verifyBaseMetadataTable() throws IOException {
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);
    assertTrue(tableMetadata.enabled());
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, true);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths = fsPartitions.stream().map(partition -> basePath + "/" + partition).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap, partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
  }

  /**
   * Verify if the Metadata table is constructed with table properties including
   * the right key generator class name.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataTableKeyGenerator(final HoodieTableType tableType) throws Exception {
    init(tableType);

    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);

    assertEquals(HoodieTableMetadataKeyGenerator.class.getCanonicalName(),
        tableMetadata.getMetadataMetaClient().getTableConfig().getKeyGeneratorClassName());
  }

  /**
   * [HUDI-2852] Table metadata returns empty for non-exist partition.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testNotExistPartition(final HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        writeConfig.getMetadataConfig(), writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(), false);
    FileStatus[] allFilesInPartition =
        tableMetadata.getAllFilesInPartition(new Path(writeConfig.getBasePath() + "dummy"));
    assertEquals(allFilesInPartition.length, 0);
  }

  /**
   * 1. Verify metadata table records key deduplication feature. When record key
   * deduplication is enabled, verify the metadata record payload on disk has empty key.
   * Otherwise, verify the valid key.
   * 2. Verify populate meta fields work irrespective of record key deduplication config.
   * 3. Verify table services like compaction benefit from record key deduplication feature.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataRecordKeyExcludeFromPayload(final HoodieTableType tableType) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withPopulateMetaFields(false)
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .build();
    init(tableType, writeConfig);

    // 2nd commit
    doWriteOperation(testTable, "0000001", INSERT);

    final HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf)
        .setBasePath(metadataTableBasePath)
        .build();
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();
    final HoodieTable table = HoodieSparkTable.create(metadataTableWriteConfig, context, metadataMetaClient);

    // Compaction has not yet kicked in. Verify all the log files
    // for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000001");
    }, "Metadata table should have valid log files!");

    // Verify no base file created yet.
    assertThrows(IllegalStateException.class, () -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should not have a base file yet!");

    // 2 more commits
    doWriteOperation(testTable, "0000002", UPSERT);
    doWriteOperation(testTable, "0000004", UPSERT);

    // Compaction should be triggered by now. Let's verify the log files
    // if any for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000002");
    }, "Metadata table should have valid log files!");

    // Verify the base file created by the just completed compaction.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should have a valid base file!");

    // 2 more commits to trigger one more compaction, along with a clean
    doWriteOperation(testTable, "0000005", UPSERT);
    doClean(testTable, "0000006", Arrays.asList("0000004"));
    doWriteOperation(testTable, "0000007", UPSERT);

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "7");
    }, "Metadata table should have valid log files!");

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should have a valid base file!");

    validateMetadata(testTable);
  }

  /**
   * Verify the metadata table log files for the record field correctness. On disk format
   * should be based on meta fields and key deduplication config. And the in-memory merged
   * records should all be materialized fully irrespective of the config.
   *
   * @param table                 - Hoodie metadata test table
   * @param metadataMetaClient    - Metadata meta client
   * @param latestCommitTimestamp - Latest commit timestamp
   * @throws IOException
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadLogFiles(HoodieTable table, HoodieTableMetaClient metadataMetaClient,
                                                                 String latestCommitTimestamp) throws IOException {
    table.getHoodieView().sync();

    // Compaction should not be triggered yet. Let's verify no base file
    // and few log files available.
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(MetadataPartitionType.FILES.getPartitionPath()).collect(Collectors.toList());
    if (fileSlices.isEmpty()) {
      throw new IllegalStateException("LogFile slices are not available!");
    }

    // Verify the log files honor the key deduplication and virtual keys config
    List<HoodieLogFile> logFiles = fileSlices.get(0).getLogFiles().map(logFile -> {
      return logFile;
    }).collect(Collectors.toList());

    List<String> logFilePaths = logFiles.stream().map(logFile -> {
      return logFile.getPath().toString();
    }).collect(Collectors.toList());

    // Verify the on-disk raw records before they get materialized
    verifyMetadataRawRecords(table, logFiles);

    // Verify the in-memory materialized and merged records
    verifyMetadataMergedRecords(metadataMetaClient, logFilePaths, latestCommitTimestamp);
  }

  /**
   * Verify the metadata table on-disk raw records. When populate meta fields is enabled,
   * these records should have additional meta fields in the payload. When key deduplication
   * is enabled, these records on the disk should have key in the payload as empty string.
   *
   * @param table
   * @param logFiles - Metadata table log files to be verified
   * @throws IOException
   */
  private void verifyMetadataRawRecords(HoodieTable table, List<HoodieLogFile> logFiles) throws IOException {
    for (HoodieLogFile logFile : logFiles) {
      FileStatus[] fsStatus = fs.listStatus(logFile.getPath());
      MessageType writerSchemaMsg = TableSchemaResolver.readSchemaFromLogFile(fs, logFile.getPath());
      if (writerSchemaMsg == null) {
        // not a data block
        continue;
      }

      Schema writerSchema = new AvroSchemaConverter().convert(writerSchemaMsg);
      try (HoodieLogFormat.Reader logFileReader = HoodieLogFormat.newReader(fs, new HoodieLogFile(fsStatus[0].getPath()), writerSchema)) {
        while (logFileReader.hasNext()) {
          HoodieLogBlock logBlock = logFileReader.next();
          if (logBlock instanceof HoodieDataBlock) {
            try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = ((HoodieDataBlock) logBlock).getRecordIterator(HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(indexRecord -> {
                final GenericRecord record = (GenericRecord) indexRecord.getData();
                assertNull(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
                assertNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
                final String key = String.valueOf(record.get(HoodieMetadataPayload.KEY_FIELD_NAME));
                assertFalse(key.isEmpty());
              });
            }
          }
        }
      }
    }
  }

  /**
   * Verify the metadata table in-memory merged records. Irrespective of key deduplication
   * config, the in-memory merged records should always have the key field in the record
   * payload fully materialized.
   *
   * @param metadataMetaClient    - Metadata table meta client
   * @param logFilePaths          - Metadata table log file paths
   * @param latestCommitTimestamp - Latest commit timestamp
   */
  private void verifyMetadataMergedRecords(HoodieTableMetaClient metadataMetaClient, List<String> logFilePaths, String latestCommitTimestamp) {
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieMetadataMergedLogRecordReader logRecordReader = HoodieMetadataMergedLogRecordReader.newBuilder()
        .withFileSystem(metadataMetaClient.getFs())
        .withBasePath(metadataMetaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withLatestInstantTime(latestCommitTimestamp)
        .withPartition(MetadataPartitionType.FILES.getPartitionPath())
        .withReaderSchema(schema)
        .withMaxMemorySizeInBytes(100000L)
        .withBufferSize(4096)
        .withSpillableMapBasePath(tempDir.toString())
        .withDiskMapType(ExternalSpillableMap.DiskMapType.BITCASK)
        .build();

    assertDoesNotThrow(() -> {
      logRecordReader.scan();
    }, "Metadata log records materialization failed");

    for (Map.Entry<String, HoodieRecord> entry : logRecordReader.getRecords().entrySet()) {
      assertFalse(entry.getKey().isEmpty());
      assertFalse(entry.getValue().getRecordKey().isEmpty());
      assertEquals(entry.getKey(), entry.getValue().getRecordKey());
    }
  }

  /**
   * Verify metadata table base files for the records persisted based on the config. When
   * the key deduplication is enabled, the records persisted on the disk in the base file
   * should have key field in the payload as empty string.
   *
   * @param table - Metadata table
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(HoodieTable table) throws IOException {
    table.getHoodieView().sync();
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(MetadataPartitionType.FILES.getPartitionPath()).collect(Collectors.toList());
    if (!fileSlices.get(0).getBaseFile().isPresent()) {
      throw new IllegalStateException("Base file not available!");
    }
    final HoodieBaseFile baseFile = fileSlices.get(0).getBaseFile().get();

    HoodieAvroHFileReader hoodieHFileReader = new HoodieAvroHFileReader(context.getHadoopConf().get(),
        new Path(baseFile.getPath()),
        new CacheConfig(context.getHadoopConf().get()));
    List<IndexedRecord> records = HoodieAvroHFileReader.readAllRecords(hoodieHFileReader);
    records.forEach(entry -> {
      assertNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      final String keyInPayload = (String) ((GenericRecord) entry)
          .get(HoodieMetadataPayload.KEY_FIELD_NAME);
      assertFalse(keyInPayload.isEmpty());
    });
  }
}
