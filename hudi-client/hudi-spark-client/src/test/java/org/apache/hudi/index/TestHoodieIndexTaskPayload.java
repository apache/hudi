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

package org.apache.hudi.index;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA_WITH_METADATA_FIELDS;
import static org.apache.hudi.common.testutils.Transformations.recordsToPartitionRecordsMap;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.assertNoHeavyTypesInLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the index lookups keep the table, the write config and the index out of their tasks.
 */
class TestHoodieIndexTaskPayload extends HoodieSparkClientTestHarness {

  private static final String COMMIT_TIME = "001";

  private HoodieWriteConfig config;
  private HoodieTable table;
  private List<String> partitions;
  private Map<String, String> expectedKeyToFileId;
  private Set<String> expectedBaseFiles;

  @BeforeEach
  void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initTestDataGenerator();
    initHoodieStorage();
    initMetaClient();
    config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .build();
    writeBaseFiles();
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  void testSimpleIndexLookupTaskCarriesNoTable() {
    List<Pair<String, HoodieBaseFile>> baseFiles = HoodieIndexUtils.getLatestBaseFilesForAllPartitions(partitions, context, table);
    HoodiePairData<HoodieKey, HoodieRecordLocation> locations =
        new HoodieSimpleIndex(config, Option.empty()).fetchRecordLocations(context, table, baseFiles);

    assertNoHeavyTypesInLineage(HoodieJavaPairRDD.getJavaPairRDD(locations).rdd());
    assertEquals(expectedKeyToFileId, locations.collectAsList().stream()
        .collect(Collectors.toMap(pair -> pair.getKey().getRecordKey(), pair -> pair.getValue().getFileId())));
  }

  @Test
  void testGlobalSimpleIndexLookupTaskCarriesNoTable() {
    List<Pair<String, HoodieBaseFile>> baseFiles = HoodieIndexUtils.getLatestBaseFilesForAllPartitions(partitions, context, table);
    HoodiePairData<String, HoodieRecordGlobalLocation> locations =
        new HoodieGlobalSimpleIndex(config, Option.empty()).fetchRecordGlobalLocations(context, table, baseFiles);

    assertNoHeavyTypesInLineage(HoodieJavaPairRDD.getJavaPairRDD(locations).rdd());
    assertEquals(expectedKeyToFileId, locations.collectAsList().stream()
        .collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue().getFileId())));
  }

  /**
   * With the metadata table the driver reads all partitions in one lookup and launches no tasks; without it
   * each partition is listed by a task. Both return the per-partition result.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLatestBaseFilesForAllPartitions(boolean useMetadataTable) throws Exception {
    HoodieTable lookupTable = useMetadataTable ? table : HoodieSparkTable.create(
        HoodieWriteConfig.newBuilder().withProperties(config.getProps())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build()).build(),
        context, metaClient);
    List<String> requestedPartitions = new ArrayList<>(partitions);
    requestedPartitions.add("2099/01/01");
    Set<String> expected = requestedPartitions.stream()
        .flatMap(partition -> HoodieIndexUtils.getLatestBaseFilesForPartition(partition, lookupTable).stream()
            .map(baseFile -> partition + "/" + baseFile.getFileName()))
        .collect(Collectors.toSet());
    assertEquals(expectedBaseFiles, expected);

    AtomicInteger startedTasks = new AtomicInteger();
    SparkListener listener = new SparkListener() {
      @Override
      public void onTaskStart(SparkListenerTaskStart taskStart) {
        startedTasks.incrementAndGet();
      }
    };
    jsc.sc().addSparkListener(listener);
    try {
      List<Pair<String, HoodieBaseFile>> baseFiles =
          HoodieIndexUtils.getLatestBaseFilesForAllPartitions(requestedPartitions, context, lookupTable);
      jsc.sc().listenerBus().waitUntilEmpty();

      assertEquals(useMetadataTable ? 0 : requestedPartitions.size(), startedTasks.get());
      assertEquals(expected.size(), baseFiles.size());
      assertEquals(expected, baseFiles.stream()
          .map(pair -> pair.getKey() + "/" + pair.getValue().getFileName())
          .collect(Collectors.toSet()));
    } finally {
      jsc.sc().removeSparkListener(listener);
    }
  }

  private void writeBaseFiles() throws Exception {
    List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, 30);
    Map<String, List<HoodieRecord>> partitionToRecords = recordsToPartitionRecordsMap(records);
    expectedKeyToFileId = new HashMap<>();
    expectedBaseFiles = new HashSet<>();
    Map<String, List<Pair<String, Integer>>> partitionToFiles = new HashMap<>();
    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context)) {
      HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(
          metaClient, HOODIE_SCHEMA_WITH_METADATA_FIELDS, metadataWriter, Option.of(context));
      testTable.forCommit(COMMIT_TIME);
      for (Map.Entry<String, List<HoodieRecord>> entry : partitionToRecords.entrySet()) {
        // two base files per partition
        List<HoodieRecord> partitionRecords = entry.getValue();
        int half = partitionRecords.size() / 2;
        writeBaseFile(testTable, entry.getKey(), partitionRecords.subList(0, half), partitionToFiles);
        writeBaseFile(testTable, entry.getKey(), partitionRecords.subList(half, partitionRecords.size()), partitionToFiles);
      }
      partitions = new ArrayList<>(partitionToRecords.keySet());
      testTable.doWriteOperation(COMMIT_TIME, WriteOperationType.UPSERT, partitions, partitionToFiles, false, false);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());
    table = HoodieSparkTable.create(config, context, metaClient);
  }

  private void writeBaseFile(HoodieSparkWriteableTestTable testTable, String partition, List<HoodieRecord> records,
                             Map<String, List<Pair<String, Integer>>> partitionToFiles) throws Exception {
    if (records.isEmpty()) {
      return;
    }
    String fileId = UUID.randomUUID().toString();
    StoragePath baseFile = testTable.withInserts(partition, fileId, records);
    partitionToFiles.computeIfAbsent(partition, p -> new ArrayList<>())
        .add(Pair.of(fileId, (int) storage.getPathInfo(baseFile).getLength()));
    expectedBaseFiles.add(partition + "/" + baseFile.getName());
    records.forEach(record -> expectedKeyToFileId.put(record.getRecordKey(), fileId));
  }
}
