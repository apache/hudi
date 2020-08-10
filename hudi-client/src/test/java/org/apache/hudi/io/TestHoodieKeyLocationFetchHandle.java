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

import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import scala.Tuple2;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS;
import static org.apache.hudi.common.testutils.Transformations.recordsToPartitionRecordsMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieKeyLocationFetchHandle}.
 */
public class TestHoodieKeyLocationFetchHandle extends HoodieClientTestHarness {

  private HoodieWriteConfig config;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestRecordFetcher");
    initPath();
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .build()).build();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testFetchHandle() throws Exception {

    String commitTime = "000";
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    Map<String, List<HoodieRecord>> partitionRecordsMap = recordsToPartitionRecordsMap(records);

    Map<Tuple2<String, String>, List<Tuple2<HoodieKey, HoodieRecordLocation>>> expectedList = writeToParquetAndGetExpectedRecordLocations(partitionRecordsMap);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    Files.createDirectories(Paths.get(basePath, ".hoodie"));

    List<Tuple2<String, HoodieBaseFile>> partitionPathFileIdPairs = loadAllFilesForPartitions(new ArrayList<>(partitionRecordsMap.keySet()), jsc, hoodieTable);

    for (Tuple2<String, HoodieBaseFile> entry : partitionPathFileIdPairs) {
      HoodieKeyLocationFetchHandle fetcherHandle = new HoodieKeyLocationFetchHandle(config, hoodieTable, Pair.of(entry._1, entry._2));
      Iterator<Tuple2<HoodieKey, HoodieRecordLocation>> result = fetcherHandle.locations();
      List<Tuple2<HoodieKey, HoodieRecordLocation>> actualList = new ArrayList<>();
      result.forEachRemaining(actualList::add);
      assertEquals(expectedList.get(new Tuple2<>(entry._1, entry._2.getFileId())), actualList);
    }
  }

  private Map<Tuple2<String, String>, List<Tuple2<HoodieKey, HoodieRecordLocation>>> writeToParquetAndGetExpectedRecordLocations(
      Map<String, List<HoodieRecord>> partitionRecordsMap) throws Exception {
    Map<Tuple2<String, String>, List<Tuple2<HoodieKey, HoodieRecordLocation>>> expectedList = new HashMap<>();
    for (Map.Entry<String, List<HoodieRecord>> entry : partitionRecordsMap.entrySet()) {
      int totalRecordsPerPartition = entry.getValue().size();
      int totalSlices = 1;
      if (totalRecordsPerPartition > 5) {
        totalSlices = totalRecordsPerPartition / 3;
      }
      int recordsPerFileSlice = totalRecordsPerPartition / totalSlices;

      List<List<HoodieRecord>> recordsForFileSlices = new ArrayList<>();
      recordsForFileSlices.add(new ArrayList<>());
      int index = 0;
      int count = 0;
      for (HoodieRecord record : entry.getValue()) {
        if (count < recordsPerFileSlice) {
          recordsForFileSlices.get(index).add(record);
          count++;
        } else {
          recordsForFileSlices.add(new ArrayList<>());
          index++;
          count = 0;
        }
      }

      for (List<HoodieRecord> recordsPerSlice : recordsForFileSlices) {
        Tuple2<String, String> fileIdInstantTimePair = writeToParquet(entry.getKey(), recordsPerSlice);
        List<Tuple2<HoodieKey, HoodieRecordLocation>> expectedEntries = new ArrayList<>();
        for (HoodieRecord record : recordsPerSlice) {
          expectedEntries.add(new Tuple2<>(record.getKey(), new HoodieRecordLocation(fileIdInstantTimePair._2, fileIdInstantTimePair._1)));
        }
        expectedList.put(new Tuple2<>(entry.getKey(), fileIdInstantTimePair._1), expectedEntries);
      }
    }
    return expectedList;
  }

  protected List<Tuple2<String, HoodieBaseFile>> loadAllFilesForPartitions(List<String> partitions, final JavaSparkContext jsc,
                                                                           final HoodieTable hoodieTable) {

    // Obtain the latest data files from all the partitions.
    List<Pair<String, HoodieBaseFile>> partitionPathFileIDList = HoodieIndexUtils.getLatestBaseFilesForAllPartitions(partitions, jsc, hoodieTable);
    return partitionPathFileIDList.stream()
        .map(pf -> new Tuple2<>(pf.getKey(), pf.getValue())).collect(toList());
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  private HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  private Tuple2<String, String> writeToParquet(String partitionPath, List<HoodieRecord> records) throws Exception {
    Thread.sleep(100);
    String instantTime = HoodieTestUtils.makeNewCommitTime();
    String fileId = UUID.randomUUID().toString();
    String filename = FSUtils.makeDataFileName(instantTime, "1-0-1", fileId);
    HoodieTestUtils.createCommitFiles(basePath, instantTime);
    HoodieClientTestUtils.writeParquetFile(basePath, partitionPath, filename, records, AVRO_SCHEMA_WITH_METADATA_FIELDS, null,
        true);
    return new Tuple2<>(fileId, instantTime);
  }
}
