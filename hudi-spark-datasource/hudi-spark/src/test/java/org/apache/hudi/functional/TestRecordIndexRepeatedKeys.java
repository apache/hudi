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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.SCHEMA_STR;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getInserts;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getKeyGenProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getPayloadProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getUpdates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests an insert whose batch repeats a record key into a table indexed by the record level index.
 */
public class TestRecordIndexRepeatedKeys extends SparkClientFunctionalTestHarness {

  private static final Class<?> PAYLOAD_CLASS = DefaultHoodieRecordPayload.class;
  private static final String P1 = "p1";
  private static final String P2 = "p2";

  @Override
  public SparkConf conf() {
    return conf(SparkClientFunctionalTestHarness.getSparkSqlConf());
  }

  // tableType, operation, streamingWrites, partitionedIndex, repeatInOtherPartition
  private static Stream<Arguments> repeatedKeyCases() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, INSERT, true, false, false),
        Arguments.of(MERGE_ON_READ, INSERT, true, false, true),
        Arguments.of(COPY_ON_WRITE, BULK_INSERT, true, false, false),
        Arguments.of(MERGE_ON_READ, INSERT, false, false, true),
        Arguments.of(COPY_ON_WRITE, BULK_INSERT, false, false, true),
        Arguments.of(MERGE_ON_READ, INSERT, true, true, false),
        Arguments.of(MERGE_ON_READ, INSERT, true, true, true),
        Arguments.of(COPY_ON_WRITE, INSERT, false, true, true)
    );
  }

  @ParameterizedTest
  @MethodSource("repeatedKeyCases")
  void testInsertWithRepeatedKey(HoodieTableType tableType, WriteOperationType operation, boolean streamingWrites,
                                 boolean partitionedIndex, boolean repeatInOtherPartition) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig(streamingWrites, partitionedIndex);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      insert(client, getInserts(2, P1, 1, PAYLOAD_CLASS), operation);
      assertRows(metaClient, "0/p1", "1/p1");

      List<HoodieRecord> uniqueInserts = getInserts(4, P1, 2, PAYLOAD_CLASS).subList(2, 4);
      List<HoodieRecord> batch = new ArrayList<>(uniqueInserts);
      batch.addAll(getUpdates(uniqueInserts.subList(0, 1), repeatInOtherPartition ? P2 : P1, 2, PAYLOAD_CLASS));

      if (partitionedIndex && repeatInOtherPartition) {
        // a partitioned index keys entries by partition, so the same key in two partitions is two entries
        insert(client, batch, operation);
        assertRows(metaClient, "0/p1", "1/p1", "2/p1", "2/p2", "3/p1");
        return;
      }

      String failedInstant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, failedInstant);
      Exception e = assertThrows(Exception.class,
          () -> client.commit(failedInstant, write(client, batch, operation, failedInstant)));
      String messages = causeMessages(e);
      assertTrue(messages.contains("Key 2 appears more than once in this write for metadata partition record_index"), messages);
      assertTrue(messages.contains("hoodie.combine.before.insert"), messages);
      assertFalse(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(failedInstant));
      assertRows(metaClient, "0/p1", "1/p1");

      // the same batch without the repeated key commits, and the index locates every key it wrote
      insert(client, uniqueInserts, operation);
      assertRows(metaClient, "0/p1", "1/p1", "2/p1", "3/p1");
      List<HoodieKey> keysToDelete = uniqueInserts.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
      String deleteInstant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, deleteInstant);
      client.commit(deleteInstant, client.delete(jsc().parallelize(keysToDelete, 1), deleteInstant));
      assertRows(metaClient, "0/p1", "1/p1");
    }
  }

  private void insert(SparkRDDWriteClient client, List<HoodieRecord> records, WriteOperationType operation) {
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    assertTrue(client.commit(instant, write(client, records, operation, instant)));
  }

  private JavaRDD<WriteStatus> write(SparkRDDWriteClient client, List<HoodieRecord> records, WriteOperationType operation,
                                     String instant) {
    JavaRDD<HoodieRecord> input = jsc().parallelize(records, 2);
    return operation == BULK_INSERT ? client.bulkInsert(input, instant) : client.insert(input, instant);
  }

  private void assertRows(HoodieTableMetaClient metaClient, String... expectedIdAndPartition) {
    List<String> actual = spark().read().format("hudi").load(metaClient.getBasePath().toString())
        .select("id", "pt").collectAsList().stream()
        .map(row -> row.getInt(0) + "/" + row.getString(1))
        .sorted()
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(expectedIdAndPartition), actual);
  }

  private static String causeMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      sb.append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage()).append('\n');
    }
    return sb.toString();
  }

  private HoodieWriteConfig getWriteConfig(boolean streamingWrites, boolean partitionedIndex) {
    return getConfigBuilder(false)
        .withProperties(getKeyGenProps(PAYLOAD_CLASS))
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableGlobalRecordLevelIndex(!partitionedIndex)
            .withEnableRecordLevelIndex(partitionedIndex)
            .withStreamingWriteEnabled(streamingWrites)
            .build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(partitionedIndex ? IndexType.RECORD_LEVEL_INDEX : IndexType.GLOBAL_RECORD_LEVEL_INDEX)
            .build())
        .withSchema(SCHEMA_STR)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withRecordMergeStrategyId(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .fromProperties(getPayloadProps(PAYLOAD_CLASS)).build())
        .build();
  }
}
