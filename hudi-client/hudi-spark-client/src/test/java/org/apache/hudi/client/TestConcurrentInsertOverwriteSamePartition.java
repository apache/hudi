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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two writers running INSERT_OVERWRITE of the same partition concurrently under OCC. When the partition is
 * empty at planning time neither replaces any file id, so the file-id based conflict check alone would let
 * both commit and leave every record in the partition twice.
 */
public class TestConcurrentInsertOverwriteSamePartition extends HoodieClientTestBase {

  private static final String TARGET_PARTITION = DEFAULT_FIRST_PARTITION_PATH;
  private static final String OTHER_PARTITION = DEFAULT_SECOND_PARTITION_PATH;
  private static final int RECORDS_PER_WRITE = 100;

  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return new SparkRDDWriteClient(context, cfg);
  }

  /** A retried load whose previous attempt is still running: both overwrites planned against an empty partition. */
  @Test
  public void testConcurrentInsertOverwriteOfEmptyPartitionIsRejected() throws Exception {
    HoodieWriteConfig cfg = occWriteConfig();
    // Seed the table with data in an unrelated partition so it is an existing, non-empty table.
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String seedTime = client.startCommit();
      client.commit(seedTime, client.insert(jsc.parallelize(
          dataGen.generateInsertsForPartition(seedTime, RECORDS_PER_WRITE, OTHER_PARTITION), 1), seedTime));
    }

    // Two writers, same payload, same target partition:
    // both start (requested) before either finishes.
    String replaceAction = HoodieTimeline.REPLACE_COMMIT_ACTION;
    String firstInstant;
    String secondInstant;

    try (SparkRDDWriteClient writer1 = getHoodieWriteClient(cfg);
         SparkRDDWriteClient writer2 = getHoodieWriteClient(cfg)) {
      firstInstant = writer1.startCommit(replaceAction);
      secondInstant = writer2.startCommit(replaceAction);
      List<HoodieRecord> payload = dataGen.generateInsertsForPartition(firstInstant, RECORDS_PER_WRITE, TARGET_PARTITION);

      HoodieWriteResult result1 = writer1.insertOverwrite(jsc.parallelize(payload, 1), firstInstant);
      HoodieWriteResult result2 = writer2.insertOverwrite(jsc.parallelize(payload, 1), secondInstant);

      assertTrue(writer1.commit(firstInstant, result1.getWriteStatuses(), Option.empty(),
          replaceAction, result1.getPartitionToReplaceFileIds()), "first overwrite commits");
      assertTrue(result1.getPartitionToReplaceFileIds().get(TARGET_PARTITION).isEmpty()
          && result2.getPartitionToReplaceFileIds().get(TARGET_PARTITION).isEmpty(), "both planned against an empty partition");
      assertThrows(HoodieWriteConflictException.class, () -> writer2.commit(secondInstant, result2.getWriteStatuses(),
          Option.empty(), replaceAction, result2.getPartitionToReplaceFileIds()), "second overwrite of the same partition is rejected");
    }

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
    HoodieTimeline replaceTimeline = metaClient.getActiveTimeline().getCompletedReplaceTimeline();
    assertEquals(Arrays.asList(firstInstant),
        replaceTimeline.getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList()));
    HoodieReplaceCommitMetadata md = metaClient.getActiveTimeline().readReplaceCommitMetadata(replaceTimeline.firstInstant().get());
    assertEquals(WriteOperationType.INSERT_OVERWRITE, md.getOperationType());

    List<HoodieBaseFile> latestBaseFiles = HoodieSparkTable.create(cfg, context, metaClient)
        .getBaseFileOnlyView().getLatestBaseFiles(TARGET_PARTITION).collect(Collectors.toList());
    assertEquals(1, latestBaseFiles.size(), "only the winning overwrite's file group is live");
    Dataset<Row> rows = sqlContext.read().parquet(latestBaseFiles.stream().map(HoodieBaseFile::getPath).toArray(String[]::new));
    assertEquals(RECORDS_PER_WRITE, rows.count());
    assertEquals(RECORDS_PER_WRITE, rows.select(RECORD_KEY_METADATA_FIELD).distinct().count());
  }

  /** Contrast: when the partition already has file groups both writers replace the same ids, and OCC rejects the second. */
  @Test
  public void testConcurrentInsertOverwriteOfPopulatedPartitionIsRejected() throws Exception {
    HoodieWriteConfig cfg = occWriteConfig();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String seedTime = client.startCommit();
      client.commit(seedTime, client.insert(jsc.parallelize(
          dataGen.generateInsertsForPartition(seedTime, RECORDS_PER_WRITE, TARGET_PARTITION), 1), seedTime));
    }

    String replaceAction = HoodieTimeline.REPLACE_COMMIT_ACTION;
    String firstInstant;
    String secondInstant;

    try (SparkRDDWriteClient writer1 = getHoodieWriteClient(cfg);
         SparkRDDWriteClient writer2 = getHoodieWriteClient(cfg)) {
      firstInstant = writer1.startCommit(replaceAction);
      secondInstant = writer2.startCommit(replaceAction);
      List<HoodieRecord> payload = dataGen.generateInsertsForPartition(firstInstant, RECORDS_PER_WRITE, TARGET_PARTITION);
      HoodieWriteResult result1 = writer1.insertOverwrite(jsc.parallelize(payload, 1), firstInstant);
      HoodieWriteResult result2 = writer2.insertOverwrite(jsc.parallelize(payload, 1), secondInstant);

      assertTrue(writer1.commit(firstInstant, result1.getWriteStatuses(), Option.empty(),
          replaceAction, result1.getPartitionToReplaceFileIds()));
      assertThrows(HoodieWriteConflictException.class, () -> writer2.commit(secondInstant, result2.getWriteStatuses(),
          Option.empty(), replaceAction, result2.getPartitionToReplaceFileIds()));
    }
  }

  private HoodieWriteConfig occWriteConfig() throws IOException {
    return getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy())
            .build())
        .build();
  }

  @Override
  public HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
