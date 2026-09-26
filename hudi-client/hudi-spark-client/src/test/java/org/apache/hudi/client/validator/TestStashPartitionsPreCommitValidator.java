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

package org.apache.hudi.client.validator;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link StashPartitionsPreCommitValidator}.
 */
public class TestStashPartitionsPreCommitValidator extends HoodieClientTestBase {

  /**
   * A test validator that extends StashPartitionsPreCommitValidator and can simulate
   * failures at specific points during the stash process.
   *
   * Set {@link #failAfterNPartitions} to control after how many partitions the validator
   * should throw an exception. Set to -1 (default) for no failure.
   */
  public static class FailingStashValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends StashPartitionsPreCommitValidator<T, I, K, O> {

    /** Number of partitions to successfully stash before throwing. -1 means no failure. */
    public static volatile int failAfterNPartitions = -1;

    private static final AtomicInteger PARTITIONS_PROCESSED = new AtomicInteger(0);

    public FailingStashValidator(HoodieSparkTable<T> table,
                                  HoodieEngineContext engineContext,
                                  HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    public static void reset() {
      failAfterNPartitions = -1;
      PARTITIONS_PROCESSED.set(0);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before,
                                                  Dataset<Row> after,
                                                  Set<String> partitionsAffected) {
      if (failAfterNPartitions < 0) {
        super.validateRecordsBeforeAndAfter(before, after, partitionsAffected);
        return;
      }

      HoodieStorage storage = getHoodieTable().getStorage();
      StoragePath basePath = getHoodieTable().getMetaClient().getBasePath();
      String stashPath = getWriteConfig().getProps().getProperty(STASH_PATH_CONFIG);
      StashPartitionRenameHelper renameHelper = new DefaultStashPartitionRenameHelper();

      for (String partition : partitionsAffected) {
        if (PARTITIONS_PROCESSED.get() >= failAfterNPartitions) {
          throw new HoodieValidationException(
              "Simulated failure after stashing " + failAfterNPartitions + " partition(s)");
        }

        StoragePath sourcePartitionPath = new StoragePath(basePath, partition);
        StoragePath targetPartitionPath = new StoragePath(stashPath, partition);

        try {
          if (!storage.exists(sourcePartitionPath) || storage.listDirectEntries(sourcePartitionPath).isEmpty()) {
            continue;
          }
          renameHelper.stashPartitionFiles(storage, sourcePartitionPath, targetPartitionPath);
          PARTITIONS_PROCESSED.incrementAndGet();
        } catch (IOException e) {
          throw new HoodieValidationException("Failed to move partition: " + partition, e);
        }
      }
    }
  }

  @Test
  public void testValidatorMovesFilesToStashOnDeletePartition() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 10);

    HoodieStorage storage = metaClient.getStorage();
    StoragePath sourcePartition = new StoragePath(basePath, DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(storage.exists(sourcePartition));
    assertFalse(storage.listDirectEntries(sourcePartition).isEmpty());

    HoodieWriteConfig config = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(
          Arrays.asList(DEFAULT_FIRST_PARTITION_PATH), instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(storage.exists(stashPartition));
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    assertSourceEmpty(storage, sourcePartition);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());
  }

  @Test
  public void testValidatorSkipsAlreadyStashedPartition() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 10);

    HoodieWriteConfig config = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(
          Arrays.asList(DEFAULT_FIRST_PARTITION_PATH), instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    HoodieStorage storage = metaClient.getStorage();
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> stashFilesFirst = storage.listDirectEntries(stashPartition);
    assertFalse(stashFilesFirst.isEmpty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime2 = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result2 = client.deletePartitions(
          Arrays.asList(DEFAULT_FIRST_PARTITION_PATH), instantTime2);
      client.commit(instantTime2, result2.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result2.getPartitionToReplaceFileIds(), Option.empty());
    }

    List<StoragePathInfo> stashFilesSecond = storage.listDirectEntries(stashPartition);
    assertEquals(stashFilesFirst.size(), stashFilesSecond.size());
  }

  @Test
  public void testValidatorHandlesPartialPriorMove() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 10);

    HoodieStorage storage = metaClient.getStorage();
    StoragePath sourcePartition = new StoragePath(basePath, DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> sourceFiles = storage.listDirectEntries(sourcePartition);
    assertTrue(sourceFiles.size() > 0);

    // Simulate a prior partial move: copy one file to stash
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    storage.createDirectory(stashPartition);
    StoragePathInfo firstFile = sourceFiles.get(0);
    org.apache.hudi.io.util.FileIOUtils.copy(storage, firstFile.getPath(),
        new StoragePath(stashPartition, firstFile.getPath().getName()));

    int totalSourceFiles = sourceFiles.size();

    HoodieWriteConfig config = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(
          Arrays.asList(DEFAULT_FIRST_PARTITION_PATH), instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    List<StoragePathInfo> stashFiles = storage.listDirectEntries(stashPartition);
    assertEquals(totalSourceFiles, stashFiles.size());
    assertSourceEmpty(storage, sourcePartition);
  }

  @Test
  public void testValidatorFailsWithoutStashPathConfig() throws Exception {
    insertRecords("001", 10);

    HoodieWriteConfig config = getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(StashPartitionsPreCommitValidator.class.getName())
                .build())
        .build();

    assertThrows(HoodieException.class, () -> {
      try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
        String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
        HoodieWriteResult result = client.deletePartitions(
            Arrays.asList(DEFAULT_FIRST_PARTITION_PATH), instantTime);
        client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            result.getPartitionToReplaceFileIds(), Option.empty());
      }
    });
  }

  @Test
  public void testValidatorStashesMultiplePartitions() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 20);

    HoodieWriteConfig config = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(
          Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH), instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    HoodieStorage storage = metaClient.getStorage();
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourceEmpty(storage, new StoragePath(basePath, partition));
    }
  }

  /**
   * Scenario (b): Validator stashes 1 partition then crashes. Retry with real validator succeeds.
   */
  @Test
  public void testRetryAfterValidatorPartiallyStashedAndCrashed() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 30);

    HoodieStorage storage = metaClient.getStorage();
    List<String> allPartitions = Arrays.asList(
        DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH);

    // First attempt: validator stashes 1 partition then throws
    FailingStashValidator.reset();
    FailingStashValidator.failAfterNPartitions = 1;

    HoodieWriteConfig failingConfig = getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(FailingStashValidator.class.getName())
                .build())
        .withProperties(buildStashProps(stashPath))
        .build();

    assertThrows(HoodieException.class, () -> {
      try (SparkRDDWriteClient client = getHoodieWriteClient(failingConfig)) {
        String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
        HoodieWriteResult result = client.deletePartitions(allPartitions, instantTime);
        client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            result.getPartitionToReplaceFileIds(), Option.empty());
      }
    });

    FailingStashValidator.reset();

    // Retry with the real validator
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig retryConfig = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(retryConfig)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(allPartitions, instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    for (String partition : allPartitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourceEmpty(storage, new StoragePath(basePath, partition));
    }
  }

  /**
   * Scenario (d): Validator succeeded for all partitions (files moved), but crashed before
   * commit. Retry should succeed — validator skips already-moved partitions.
   */
  @Test
  public void testRetryAfterValidatorSucceededButCrashedBeforeCommit() throws Exception {
    String stashPath = createStashDir();
    insertRecords("001", 30);

    HoodieStorage storage = metaClient.getStorage();
    List<String> allPartitions = Arrays.asList(
        DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH);

    // Manually move all files to stash (simulating validator succeeded but commit didn't land)
    DefaultStashPartitionRenameHelper renameHelper = new DefaultStashPartitionRenameHelper();
    for (String partition : allPartitions) {
      StoragePath src = new StoragePath(basePath, partition);
      StoragePath dest = new StoragePath(stashPath, partition);
      if (storage.exists(src) && !storage.listDirectEntries(src).isEmpty()) {
        renameHelper.stashPartitionFiles(storage, src, dest);
      }
    }

    // Retry with real validator — sources are empty, validator skips all
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig retryConfig = getConfigWithStashValidator(stashPath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(retryConfig)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(allPartitions, instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }

    // All partitions should be stashed and commit should exist
    for (String partition : allPartitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());
  }

  // ---- Helper methods ----

  private void insertRecords(String commitTime, int numRecords) throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      JavaRDD<WriteStatus> statusesRdd = client.insert(writeRecords, commitTime);
      List<WriteStatus> statuses = statusesRdd.collect();
      assertNoWriteErrors(statuses);
      client.commit(commitTime, jsc.parallelize(statuses));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private HoodieWriteConfig getConfigWithStashValidator(String stashPath) {
    return getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(StashPartitionsPreCommitValidator.class.getName())
                .build())
        .withProperties(buildStashProps(stashPath))
        .build();
  }

  private java.util.Properties buildStashProps(String stashPath) {
    java.util.Properties props = new java.util.Properties();
    props.setProperty(StashPartitionsPreCommitValidator.STASH_PATH_CONFIG, stashPath);
    return props;
  }

  private String createStashDir() throws IOException {
    Path stashDir = Paths.get(basePath).getParent().resolve("stash_" + System.currentTimeMillis());
    Files.createDirectories(stashDir);
    return stashDir.toAbsolutePath().toUri().toString();
  }

  private void assertSourceEmpty(HoodieStorage storage, StoragePath sourcePartition) throws IOException {
    if (storage.exists(sourcePartition)) {
      List<StoragePathInfo> remaining = storage.listDirectEntries(sourcePartition).stream()
          .filter(f -> !f.getPath().getName().startsWith("."))
          .collect(Collectors.toList());
      assertTrue(remaining.isEmpty(),
          "Source partition should have no data files, but found: " + remaining);
    }
  }
}
