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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;

/**
 * The common hoodie test harness to provide the basic infrastructure.
 */
public class HoodieCommonTestHarness {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCommonTestHarness.class);
  protected static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
  protected static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = null;
  protected static final HoodieLogBlock.HoodieLogBlockType DEFAULT_DATA_BLOCK_TYPE = HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;

  protected String tableName;
  protected String basePath;
  protected URI baseUri;
  protected HoodieTestDataGenerator dataGen;
  protected HoodieTableMetaClient metaClient;
  @TempDir
  public java.nio.file.Path tempDir;

  protected StorageConfiguration<Configuration> storageConf;
  protected HoodieStorage storage;

  protected void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Initializes basePath.
   */
  protected void initPath() {
    initPath("dataset");
  }

  /**
   * Initializes basePath with folder name.
   *
   * @param folderName Folder name.
   */
  protected void initPath(String folderName) {
    try {
      java.nio.file.Path basePath = tempDir.resolve(folderName);
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toAbsolutePath().toString();
      this.baseUri = basePath.toUri();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Initializes a test data generator which used to generate test data.
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  protected void initTestDataGenerator(String[] partitionPaths) {
    dataGen = new HoodieTestDataGenerator(partitionPaths);
  }

  /**
   * Cleanups test data generator.
   */
  protected void cleanupTestDataGenerator() {
    if (dataGen != null) {
      dataGen = null;
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      initPath();
    }
    metaClient = HoodieTestUtils.init(basePath, getTableType());
  }

  protected void initMetaClient(boolean preTableVersion8) throws IOException {
    if (basePath == null) {
      initPath();
    }
    metaClient = HoodieTestUtils.init(basePath, getTableType(), "", false, null, "datestr",
        preTableVersion8 ? Option.of(HoodieTableVersion.SIX) : Option.of(HoodieTableVersion.current()));
  }

  protected void cleanMetaClient() {
    if (metaClient != null) {
      metaClient = null;
    }
  }

  protected void refreshFsView() throws IOException {
    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    return getFileSystemView(timeline, false);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline, boolean enableIncrementalTimelineSync) {
    return new HoodieTableFileSystemView(metaClient, timeline, enableIncrementalTimelineSync);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) throws IOException {
    return getFileSystemView(metaClient, metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants());
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline)
      throws IOException {
    return getFileSystemView(timeline, true);
  }

  protected SyncableFileSystemView getFileSystemViewWithUnCommittedSlices(HoodieTableMetaClient metaClient) {
    try {
      return new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline(),
          HoodieTestTable.of(metaClient).listAllBaseAndLogFiles()
      );
    } catch (IOException ioe) {
      throw new HoodieIOException("Error getting file system view", ioe);
    }
  }

  /**
   * Gets a default {@link HoodieTableType#COPY_ON_WRITE} table type. Sub-classes can override this method to specify a
   * new table type.
   *
   * @return an instance of Hoodie table type.
   */
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }

  public void pollTimelineForAction(String tablePath, StorageConfiguration<?> conf, int numCommits, String action) throws InterruptedException {
    pollForTimeline(tablePath, conf, numCommits, instant -> instant.getAction().equals(action), true);
  }

  public void pollForTimeline(String tablePath, StorageConfiguration<?> conf, int commits) throws InterruptedException {
    pollForTimeline(tablePath, conf, commits, instant -> true, false);
  }

  private void pollForTimeline(String tablePath, StorageConfiguration<?> conf, int commits, Predicate<HoodieInstant> filter, boolean pullAllCommits)
      throws InterruptedException {
    Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();
    ScheduledFuture<?> scheduledFuture = getScheduledExecutorService().scheduleWithFixedDelay(() -> {
      try {
        HoodieTableMetaClient metaClient =
            HoodieTableMetaClient.builder().setConf(conf).setBasePath(tablePath).build();
        HoodieTimeline timeline = pullAllCommits
            ? metaClient.getActiveTimeline().getAllCommitsTimeline()
            : metaClient.getActiveTimeline().getCommitsTimeline();
        List<HoodieInstant> instants = timeline
            .filterCompletedInstants()
            .getInstants()
            .stream()
            .filter(filter::test)
            .collect(Collectors.toList());
        if (instants.size() >= commits) {
          semaphore.release();
        }
      } catch (Exception e) {
        LOG.warn("Error in polling for timeline", e);
      }
    }, 0, 1, TimeUnit.SECONDS);
    int maxWaitInMinutes = 10;
    boolean timelineFound = semaphore.tryAcquire(maxWaitInMinutes, TimeUnit.MINUTES);
    scheduledFuture.cancel(true);
    if (!timelineFound) {
      throw new RuntimeException(String.format(
          "Failed to create timeline in %d minutes", maxWaitInMinutes));
    }
  }

  protected ScheduledThreadPoolExecutor getScheduledExecutorService() {
    if (scheduledThreadPoolExecutor == null || scheduledThreadPoolExecutor.isShutdown()) {
      scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2);
      scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    }
    return scheduledThreadPoolExecutor;
  }

  protected HoodieActiveTimeline getActiveTimeline() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return metaClient.getActiveTimeline();
  }

  protected Boolean hasPendingCommits() {
    HoodieActiveTimeline timeline = getActiveTimeline();
    HoodieTimeline completedTimeline = timeline.filterCompletedInstants();
    Set<String> completedInstants = completedTimeline
        .getInstants()
        .stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    List<String> pendingInstants = timeline
        .getInstants()
        .stream()
        .map(HoodieInstant::requestedTime)
        .filter(t -> !completedInstants.contains(t))
        .collect(Collectors.toList());
    return !pendingInstants.isEmpty();
  }

  protected static List<HoodieLogFile> writeLogFiles(StoragePath partitionPath,
                                                     Schema schema,
                                                     List<HoodieRecord> records,
                                                     int numFiles,
                                                     HoodieStorage storage,
                                                     Properties props,
                                                     String fileId,
                                                     String commitTime)
      throws IOException, InterruptedException {
    List<IndexedRecord> indexedRecords = records.stream()
        .map(record -> {
          try {
            return record.toIndexedRecord(schema, props);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .filter(Option::isPresent)
        .map(Option::get)
        .map(HoodieRecord::getData)
        .collect(Collectors.toList());
    return writeLogFiles(partitionPath, schema, indexedRecords, numFiles, storage, fileId, commitTime, "100");
  }

  protected static List<HoodieLogFile> writeLogFiles(StoragePath partitionPath,
                                                     Schema schema,
                                                     List<IndexedRecord> records,
                                                     int numFiles,
                                                     HoodieStorage storage)
      throws IOException, InterruptedException {
    return writeLogFiles(partitionPath, schema, records, numFiles, storage, "test-fileid1", "100", "100");
  }

  protected static List<HoodieLogFile> writeLogFiles(StoragePath partitionPath,
                                                     Schema schema,
                                                     List<IndexedRecord> records,
                                                     int numFiles,
                                                     HoodieStorage storage,
                                                     String fileId,
                                                     String commitTime,
                                                     String logBlockInstantTime)
      throws IOException, InterruptedException {
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withSizeThreshold(1024).withFileId(fileId)
            .withInstantTime(commitTime)
            .withStorage(storage).build();
    if (storage.exists(writer.getLogFile().getPath())) {
      // enable append for reader test.
      ((HoodieLogFormatWriter) writer).withOutputStream(
          (FSDataOutputStream) storage.append(writer.getLogFile().getPath()));
    }
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, logBlockInstantTime);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());

    List<HoodieLogFile> logFiles = new ArrayList<>();

    // Create log files
    int recordsPerFile = records.size() / numFiles;
    int filesWritten = 0;

    while (filesWritten < numFiles) {
      int targetRecordsCount = filesWritten == numFiles - 1
          ? recordsPerFile + (records.size() % recordsPerFile)
          : recordsPerFile;
      int offset = filesWritten * recordsPerFile;
      List<IndexedRecord> targetRecords = records.subList(offset, offset + targetRecordsCount);

      logFiles.add(writer.getLogFile());
      writer.appendBlock(getDataBlock(DEFAULT_DATA_BLOCK_TYPE, targetRecords, header));
      filesWritten++;
    }

    writer.close();

    return logFiles;
  }

  public static HoodieDataBlock getDataBlock(HoodieLogBlock.HoodieLogBlockType dataBlockType, List<IndexedRecord> records,
                                             Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    return getDataBlock(dataBlockType, records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList()), header, new StoragePath("dummy_path"));
  }

  private static HoodieDataBlock getDataBlock(HoodieLogBlock.HoodieLogBlockType dataBlockType, List<HoodieRecord> records,
                                              Map<HoodieLogBlock.HeaderMetadataType, String> header, StoragePath pathForReader) {
    switch (dataBlockType) {
      case CDC_DATA_BLOCK:
        return new HoodieCDCDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(records, false, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(records, header, HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue(), pathForReader, HoodieReaderConfig.USE_NATIVE_HFILE_READER.defaultValue());
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(records, false, header, HoodieRecord.RECORD_KEY_METADATA_FIELD, PARQUET_COMPRESSION_CODEC_NAME.defaultValue(), 0.1, true);
      default:
        throw new RuntimeException("Unknown data block type " + dataBlockType);
    }
  }
}
