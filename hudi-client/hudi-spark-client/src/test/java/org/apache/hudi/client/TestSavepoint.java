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

import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.view.FileSystemViewStorageType.EMBEDDED_KV_STORE;
import static org.apache.hudi.common.table.view.FileSystemViewStorageType.MEMORY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for savepoint operation.
 */
public class TestSavepoint extends HoodieClientTestBase {

  private static Stream<Arguments> testSavepointParams() {
    return Arrays.stream(new Object[][] {
        {true, MEMORY, HoodieTableType.COPY_ON_WRITE}, {true, EMBEDDED_KV_STORE, HoodieTableType.COPY_ON_WRITE},
        {false, MEMORY, HoodieTableType.COPY_ON_WRITE}, {false, EMBEDDED_KV_STORE, HoodieTableType.COPY_ON_WRITE},
        {true, MEMORY, HoodieTableType.MERGE_ON_READ}, {true, EMBEDDED_KV_STORE, HoodieTableType.MERGE_ON_READ},
        {false, MEMORY, HoodieTableType.MERGE_ON_READ}, {false, EMBEDDED_KV_STORE, HoodieTableType.MERGE_ON_READ}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("testSavepointParams")
  public void testSavepoint(boolean enableMetadataTable,
                            FileSystemViewStorageType storageType,
                            HoodieTableType tableType) throws IOException {
    HoodieWriteConfig cfg = getWriteConfig(enableMetadataTable, storageType);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0x17AB);

    initMetaClient(tableType);

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      String commitTime1 = "001";
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime1, 200);
      JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 1);
      List<WriteStatus> statusList = client.upsert(writeRecords1, commitTime1).collect();
      client.commit(commitTime1, jsc.parallelize(statusList), Option.empty(), tableType == HoodieTableType.COPY_ON_WRITE ? COMMIT_ACTION : DELTA_COMMIT_ACTION,
          Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      String commitTime2 = "002";
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      List<HoodieRecord> records2 = dataGen.generateInserts(commitTime2, 200);
      JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 1);
      statusList = client.upsert(writeRecords2, commitTime2).collect();
      client.commit(commitTime2, jsc.parallelize(statusList), Option.empty(), tableType == HoodieTableType.COPY_ON_WRITE ? COMMIT_ACTION : DELTA_COMMIT_ACTION,
          Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      client.savepoint("user", "hoodie-savepoint-unit-test");

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieSparkTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
      HoodieTimeline savepointTimeline = table.getActiveTimeline().getSavePointTimeline();
      assertEquals(1, savepointTimeline.countInstants());

      Map<String, HoodieSavepointPartitionMetadata> savepointPartitionMetadataMap =
          savepointTimeline.readSavepointMetadata(savepointTimeline.firstInstant().get())
              .getPartitionMetadata();

      HoodieTimeline commitsTimeline = table.getActiveTimeline().getCommitsTimeline();
      Map<String, List<HoodieWriteStat>> partitionToWriteStats =
          commitsTimeline.readCommitMetadata(commitsTimeline.lastInstant().get())
          .getPartitionToWriteStats();

      assertEquals(partitionToWriteStats.size(), savepointPartitionMetadataMap.size());
      for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
        String partition = entry.getKey();
        assertTrue(savepointPartitionMetadataMap.containsKey(partition));
        assertEquals(
            entry.getValue().stream().map(path -> getFileNameFromPath(path.getPath()))
                .sorted().collect(Collectors.toList()),
            savepointPartitionMetadataMap.get(partition).getSavepointDataFile()
                .stream().sorted().collect(Collectors.toList())
        );
      }
    }
  }

  private HoodieWriteConfig getWriteConfig(boolean enableMetadataTable,
                                           FileSystemViewStorageType storageType) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(
            ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024)
            .orcMaxFileSize(1024 * 1024).build())
        .forTable(RAW_TRIPS_TEST_NAME)
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort)
            .withStorageType(storageType)
            .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .build();
  }

  private String getFileNameFromPath(String path) {
    String[] parts = path.split("/");
    return parts[parts.length - 1];
  }
}
