/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieJavaWriteClientInsert extends HoodieJavaClientTestHarness {
  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieJavaWriteClientInsert.class, "/exampleSchema.avsc");

  private static HoodieWriteConfig.Builder makeHoodieClientConfigBuilder(String basePath) {
    return makeHoodieClientConfigBuilder(basePath, SCHEMA.toAvroSchema());
  }

  private static HoodieWriteConfig.Builder makeHoodieClientConfigBuilder(String basePath, Schema schema) {
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(schema.toString());
  }

  private FileStatus[] getIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
      throws Exception {
    // initialize parquet input format
    HoodieParquetInputFormat hoodieInputFormat = new HoodieParquetInputFormat();
    JobConf jobConf = new JobConf(storageConf.unwrap());
    hoodieInputFormat.setConf(jobConf);
    HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE);
    setupIncremental(jobConf, startCommitTime, numCommitsToPull);
    FileInputFormat.setInputPaths(jobConf, Paths.get(basePath, partitionPath).toString());
    return hoodieInputFormat.listStatus(jobConf);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer, boolean passInTimelineServer) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePath())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .build();

    HoodieJavaWriteClient writeClient;
    if (passInTimelineServer) {
      EmbeddedTimelineService timelineService = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(context, null, writeConfig);
      writeConfig.setViewStorageConfig(timelineService.getRemoteFileSystemViewConfig(writeConfig));
      writeClient = new HoodieJavaWriteClient(context, writeConfig, true, Option.of(timelineService));
      // Both the write client and the table service client should use the same passed-in
      // timeline server instance.
      assertEquals(timelineService, writeClient.getTimelineServer().get());
      assertEquals(timelineService, writeClient.getTableServiceClient().getTimelineServer().get());
      // Write config should not be changed
      assertEquals(writeConfig, writeClient.getConfig());
      timelineService.stopForBasePath(writeConfig.getBasePath());
    } else {
      writeClient = new HoodieJavaWriteClient(context, writeConfig);
      // Only one timeline server should be instantiated, and the same timeline server
      // should be used by both the write client and the table service client.
      assertEquals(
          writeClient.getTimelineServer(),
          writeClient.getTableServiceClient().getTimelineServer());
      if (!enableEmbeddedTimelineServer) {
        assertFalse(writeClient.getTimelineServer().isPresent());
      }
    }
    writeClient.close();
  }

  /**
   * HUDI-5011: exercises a Java-engine write config against both marker types with the embedded timeline
   * server. {@code HoodieWriteConfig.Builder} defaults {@link MarkerType#DIRECT} for
   * {@link EngineType#JAVA}, so the timeline-server-based path is only reached when
   * {@code hoodie.write.markers.type} is set explicitly. Note the default applies to the config's engine
   * type, not to the client: {@code HoodieJavaWriteClient} never inspects it, so a config left on the
   * builder's SPARK default would resolve to TIMELINE_SERVER_BASED on its own.
   *
   * <p>Backup for the remote file system view is disabled so that a timeline-server failure fails the test
   * rather than silently falling back to a local view, matching
   * {@code HoodieJavaClientTestHarness#getConfigBuilder}.
   */
  @ParameterizedTest
  @EnumSource(MarkerType.class)
  public void testInsertWithEmbeddedTimelineServerAndMarkerType(MarkerType markerType) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder(basePath)
        .withMarkersType(markerType.name())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .build();

    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);
    assertTrue(writeClient.getTimelineServer().isPresent(),
        "The embedded timeline server should be running for marker type " + markerType);

    List<HoodieRecord> records = new ArrayList<>();
    records.add(createSimpleRecord("1", "2021-09-11T16:16:41.415Z", 1));
    records.add(createSimpleRecord("2", "2021-09-11T16:16:41.415Z", 2));

    String commitTime = makeNewCommitTime(1, "%09d");
    WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
    List<WriteStatus> statuses = writeClient.insert(records, commitTime);

    // Inspect the markers before commit removes them. Only the timeline-server path writes MARKERS.type,
    // so this is what would notice a silent fallback to DirectWriteMarkers.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    StoragePath markerDir = new StoragePath(metaClient.getMarkerFolderPath(commitTime));
    boolean markerTypeFileExists = MarkerUtils.doesMarkerTypeFileExist(metaClient.getStorage(), markerDir);
    List<String> markerFileNames = listMarkerFileNames(markerDir);
    if (markerType == MarkerType.TIMELINE_SERVER_BASED) {
      assertTrue(markerTypeFileExists,
          "Timeline-server-based markers should have written " + MarkerUtils.MARKER_TYPE_FILENAME);
      assertTrue(markerFileNames.stream().anyMatch(
          name -> name.startsWith(MarkerUtils.MARKERS_FILENAME_PREFIX)
              && !name.equals(MarkerUtils.MARKER_TYPE_FILENAME)),
          () -> "Timeline-server-based markers should have written a "
              + MarkerUtils.MARKERS_FILENAME_PREFIX + "<n> file. Found: " + markerFileNames);
    } else {
      assertFalse(markerTypeFileExists,
          "Direct markers should not have written " + MarkerUtils.MARKER_TYPE_FILENAME);
      // Absence of MARKERS.type alone would also hold if the write produced no markers at all, so
      // require the direct markers themselves.
      assertTrue(markerFileNames.stream().anyMatch(name -> name.contains(HoodieTableMetaClient.MARKER_EXTN)),
          () -> "Direct markers should have written a " + HoodieTableMetaClient.MARKER_EXTN
              + " file. Found: " + markerFileNames);
    }

    writeClient.commit(commitTime, statuses);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent(),
        "The commit should have completed for marker type " + markerType);
    assertEquals(1, getIncrementalFiles("2021/09/11", "0", -1).length,
        "One base file should have been written for marker type " + markerType);
  }

  /**
   * The marker file names written under {@code markerDir}, read recursively off storage rather than
   * through the timeline server, so the assertion does not lean on the path it is checking.
   */
  private List<String> listMarkerFileNames(StoragePath markerDir) throws IOException {
    if (!metaClient.getStorage().exists(markerDir)) {
      return Collections.emptyList();
    }
    return metaClient.getStorage().listFiles(markerDir).stream()
        .map(pathInfo -> pathInfo.getPath().getName())
        .collect(Collectors.toList());
  }

  @Test
  public void testInsert() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder(basePath).withMergeAllowDuplicateOnInserts(true).build();

    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    FileFormatUtils fileUtils = getFileUtilsInstance(metaClient);

    // Get some records belong to the same partition (2021/09/11)
    List<HoodieRecord> records1 = new ArrayList<>();
    records1.add(createSimpleRecord("1", "2021-09-11T16:16:41.415Z", 1));
    records1.add(createSimpleRecord("2", "2021-09-11T16:16:41.415Z", 2));

    int startInstant = 1;
    String firstCommitTime = makeNewCommitTime(startInstant++, "%09d");
    // First insert
    WriteClientTestUtils.startCommitWithTime(writeClient, firstCommitTime);
    writeClient.commit(firstCommitTime, writeClient.insert(records1, firstCommitTime));

    String partitionPath = "2021/09/11";
    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path filePath = allFiles[0].getPath();
    BloomFilter filter = fileUtils.readBloomFilterFromMetadata(storage, new StoragePath(filePath.toUri()));
    for (HoodieRecord record : records1) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    List<HoodieRecord> records2 = new ArrayList<>();
    // The recordKey of records2 and records1 are the same, but the values of other fields are different
    records2.add(createSimpleRecord("1", "2021-09-11T16:39:41.415Z", 3));
    records2.add(createSimpleRecord("2", "2021-09-11T16:39:41.415Z", 4));


    String newCommitTime = makeNewCommitTime(startInstant++, "%09d");
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
    // Second insert is the same as the _row_key of the first one,test allowDuplicateInserts
    writeClient.commit(newCommitTime, writeClient.insert(records2, newCommitTime));

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(filePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    filePath = allFiles[0].getPath();
    // The final result should be a collection of records1 and records2
    records1.addAll(records2);

    // Read the base file, check the record content
    List<GenericRecord> fileRecords = fileUtils.readAvroRecords(storage, new StoragePath(filePath.toUri()));
    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertEquals(records1.get(index).getRecordKey(), record.get("_row_key").toString());
      assertEquals(index + 1, record.get("number"));
      index++;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInsertWithDataGenerator(boolean mergeAllowDuplicateOnInsertsEnable) throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder(basePath, AVRO_SCHEMA)
        .withMergeAllowDuplicateOnInserts(mergeAllowDuplicateOnInsertsEnable).build();

    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    FileFormatUtils fileUtils = getFileUtilsInstance(metaClient);

    String partitionPath = "2021/09/11";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[]{partitionPath});

    int startInstant = 1;
    String firstCommitTime = makeNewCommitTime(startInstant++, "%09d");
    List<HoodieRecord> records1 = dataGenerator.generateInserts(firstCommitTime, 100);

    // First insert
    WriteClientTestUtils.startCommitWithTime(writeClient, firstCommitTime);
    writeClient.commit(firstCommitTime, writeClient.insert(records1, firstCommitTime));

    FileStatus[] allFiles = getIncrementalFiles(partitionPath, "0", -1);
    assertEquals(1, allFiles.length);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path filePath = allFiles[0].getPath();
    BloomFilter filter = fileUtils.readBloomFilterFromMetadata(storage, new StoragePath(filePath.toUri()));
    for (HoodieRecord record : records1) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }

    String newCommitTime = makeNewCommitTime(startInstant++, "%09d");
    List<HoodieRecord> records2 = dataGenerator.generateUpdates(newCommitTime, 100);
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
    // Second insert is the same as the _row_key of the first one,test allowDuplicateInserts
    writeClient.commit(newCommitTime, writeClient.insert(records2, newCommitTime));

    allFiles = getIncrementalFiles(partitionPath, firstCommitTime, -1);
    assertEquals(1, allFiles.length);
    // verify new incremental file group is same as the previous one
    assertEquals(FSUtils.getFileId(filePath.getName()), FSUtils.getFileId(allFiles[0].getPath().getName()));

    filePath = allFiles[0].getPath();
    // If mergeAllowDuplicateOnInsertsEnable is true, the final result should be a collection of records1 and records2
    records1.addAll(records2);

    // Read the base file, check the record content
    List<GenericRecord> fileRecords = fileUtils.readAvroRecords(storage, new StoragePath(filePath.toUri()));
    assertEquals(fileRecords.size(), mergeAllowDuplicateOnInsertsEnable ? records1.size() : records2.size());

    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertEquals(records1.get(index).getRecordKey(), record.get("_row_key").toString());
      index++;
    }
  }
}
