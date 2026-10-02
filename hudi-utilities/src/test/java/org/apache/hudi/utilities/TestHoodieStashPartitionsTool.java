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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.DefaultStashPartitionRenameHelper;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieStashPartitionsTool}.
 */
public class TestHoodieStashPartitionsTool extends SparkClientFunctionalTestHarness {

  private HoodieTableMetaClient metaClient;
  private HoodieTestDataGenerator dataGen;
  private String stashPath;

  @BeforeEach
  public void init() throws IOException {
    metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    dataGen = new HoodieTestDataGenerator();
    Path stashDir = tempDir.resolve("stash");
    Files.createDirectories(stashDir);
    stashPath = stashDir.toAbsolutePath().toUri().toString();
  }

  /**
   * Given: A COW table with data in multiple partitions.
   * When: Stash tool runs in stash mode for one partition.
   * Then: Files are moved to stash location, partition is deleted from table,
   *       and a replace commit exists on the timeline.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashSinglePartition(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);

    assertTrue(storage.exists(sourcePartition));
    assertFalse(storage.listDirectEntries(sourcePartition).isEmpty());

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    assertTrue(storage.exists(stashPartition));
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    assertSourcePartitionEmpty(storage, sourcePartition);

    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(reloadedMetaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());

    // Other partitions should be untouched
    StoragePath secondPartition = new StoragePath(basePath(), DEFAULT_SECOND_PARTITION_PATH);
    assertTrue(storage.exists(secondPartition));
    assertFalse(storage.listDirectEntries(secondPartition).isEmpty());
  }

  /**
   * Given: A COW table with data, and one partition already stashed.
   * When: Stash tool runs in stash mode for the same partition again (retry/MDT case).
   * Then: The validator skips the already-stashed partition gracefully.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashAlreadyStashedPartitionIsIdempotent(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    HoodieStorage storage = hoodieStorage();
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> stashFilesAfterFirst = storage.listDirectEntries(stashPartition);

    // Run again
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    List<StoragePathInfo> stashFilesAfterSecond = storage.listDirectEntries(stashPartition);
    assertEquals(stashFilesAfterFirst.size(), stashFilesAfterSecond.size());
  }

  /**
   * Given: A COW table with data in multiple partitions.
   * When: Stash tool runs in stash mode for multiple partitions.
   * Then: All specified partitions are moved to stash location.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashMultiplePartitions(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    String partitions = DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH;
    HoodieStashPartitionsTool.Config cfg = buildConfig(partitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    HoodieStorage storage = hoodieStorage();
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }

    // Third partition untouched
    StoragePath thirdPartition = new StoragePath(basePath(), DEFAULT_THIRD_PARTITION_PATH);
    assertTrue(storage.exists(thirdPartition));
    assertFalse(storage.listDirectEntries(thirdPartition).isEmpty());
  }

  /**
   * Given: A stash fully completed (replace commit landed, data in stash, no active files).
   * When: Rollback stash is run.
   * Then: Rollback is skipped — files stay in stash, source remains empty. The user must
   *       use insert_overwrite to restore the partition.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStashSkipsCompletedStash(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);

    // Stash the partition (replace commit lands)
    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata)).run();
    assertSourcePartitionEmpty(storage, sourcePartition);
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty());

    // Run rollback — should be a no-op since stash fully completed
    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata)).run();

    // Stash files should still be in stash (not moved back)
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty(),
        "Stash files should remain in stash after rollback of a completed stash");
    // Source should still be empty
    assertSourcePartitionEmpty(storage, sourcePartition);
  }

  /**
   * Given: A partial stash attempt — pre-commit validator moved files to stash but the
   *        commit never landed, so partition still has active files and stash has data.
   * When: Rollback stash is run.
   * Then: Files are restored from stash back to source, making the partition whole again.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStashRestoresPartialAttempt(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> originalFiles = storage.listDirectEntries(sourcePartition);
    int originalFileCount = originalFiles.size();
    assertTrue(originalFileCount > 0);

    // Simulate partial stash: the rename helper copies all files to stash first, then deletes
    // from source. A crash after copy but before delete leaves files in both locations.
    // We simulate this by copying the source files to stash without deleting them from source.
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    storage.createDirectory(stashPartition);
    for (StoragePathInfo fileInfo : originalFiles) {
      StoragePath destFile = new StoragePath(stashPartition, fileInfo.getPath().getName());
      org.apache.hudi.io.util.FileIOUtils.copy(storage, fileInfo.getPath(), destFile);
    }

    // Verify: partition still has files, stash also has data
    assertFalse(storage.listDirectEntries(sourcePartition).isEmpty());
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty());

    // Run rollback — should restore stash data back to source
    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata)).run();

    // Stash should be empty after rollback
    assertTrue(storage.listDirectEntries(stashPartition).isEmpty(),
        "Stash should be empty after rollback of partial attempt");
    // Source should still have all its files
    assertFalse(storage.listDirectEntries(sourcePartition).isEmpty());
  }

  /**
   * Given: No partition has been stashed (no stash data), partition has active files.
   * When: Rollback stash is run.
   * Then: Tool completes without error (no-op), partition is untouched.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStashNoOpWhenNothingStashed(boolean enableMetadata) throws IOException {
    insertRecords("001", 10, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    int fileCountBefore = storage.listDirectEntries(sourcePartition).size();

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Source should be untouched
    assertEquals(fileCountBefore, storage.listDirectEntries(sourcePartition).size());
  }

  /**
   * Given: No stash data and no active files in the partition.
   * When: Rollback stash is run.
   * Then: Tool completes without error (no-op, warns).
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStashNoOpWhenNoDataAnywhere(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    // Stash the partition first (so it gets deleted), then clean up stash manually
    // to simulate a state where there's no data anywhere.
    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata)).run();

    HoodieStorage storage = hoodieStorage();
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    // Remove stash data to simulate the "no data anywhere" state
    for (StoragePathInfo file : storage.listDirectEntries(stashPartition)) {
      storage.deleteFile(file.getPath());
    }
    assertTrue(storage.listDirectEntries(stashPartition).isEmpty());

    // Run rollback — should be a no-op with warning
    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata)).run();

    // Nothing should change
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    assertSourcePartitionEmpty(storage, sourcePartition);
  }

  /**
   * Given: A COW table with data.
   * When: Dry run mode is executed.
   * Then: No files are moved, no commits created.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRunDoesNotModifyTable(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> filesBefore = storage.listDirectEntries(sourcePartition);
    int commitCountBefore = metaClient.getActiveTimeline().countInstants();

    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "dry_run", enableMetadata)).run();

    List<StoragePathInfo> filesAfter = storage.listDirectEntries(sourcePartition);
    assertEquals(filesBefore.size(), filesAfter.size());

    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(storage.exists(stashPartition) && !storage.listDirectEntries(stashPartition).isEmpty());

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertEquals(commitCountBefore, reloaded.getActiveTimeline().countInstants());
  }

  /**
   * Scenario (a): Stash for multiple partitions was started. Operation failed before reaching
   * the pre-commit validator (no files moved, no commit). Retry should succeed without issues.
   *
   * We simulate this by manually creating a requested replace commit instant (as if startCommit
   * ran but deletePartitions failed), then retrying with the tool.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRetryAfterFailureBeforePreCommitValidator(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    String partitions = DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH;

    // Verify all partitions have data and nothing is in stash
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath src = new StoragePath(basePath(), partition);
      assertTrue(storage.exists(src));
      assertFalse(storage.listDirectEntries(src).isEmpty());
    }

    // The first attempt "failed" before reaching the validator — no files moved, no commit.
    // This is the initial state. Just retry the tool — it should work from scratch.
    HoodieStashPartitionsTool.Config cfg = buildConfig(partitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Verify success
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }
  }

  /**
   * Scenario (b): Stash for multiple partitions was started. The pre-commit validator
   * stashed some partitions (moved files) but crashed before completing. On retry, the
   * tool's pre-check (non-MDT) should recover partial state, and the validator (MDT)
   * should handle it. The stash should eventually succeed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRetryAfterValidatorPartiallyStashedAndCrashed(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    String[] partitions = {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};

    // Simulate partial stash: move first partition's files to stash (as if validator did it),
    // but no commit landed.
    DefaultStashPartitionRenameHelper renameHelper = new DefaultStashPartitionRenameHelper();
    StoragePath src = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath dest = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    renameHelper.stashPartitionFiles(storage, src, dest);

    // Verify partial state: first partition in stash, others untouched
    assertTrue(storage.exists(dest));
    assertFalse(storage.listDirectEntries(dest).isEmpty());
    assertSourcePartitionEmpty(storage, src);

    // Retry with the tool — should handle partial state and complete all partitions
    String allPartitions = String.join(",", partitions);
    HoodieStashPartitionsTool.Config cfg = buildConfig(allPartitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Verify: all partitions stashed
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition), "Stash should exist for: " + partition);
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty(),
          "Stash should have files for: " + partition);
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertFalse(reloaded.getActiveTimeline().getCompletedReplaceTimeline().empty());
  }

  /**
   * Scenario (d): Pre-commit validator succeeded for all partitions (files moved), but
   * crashed just after that (before the commit landed). On retry, the tool should handle
   * the state where all files are in stash but no commit exists, and succeed.
   *
   * We simulate this by running a real stash (which lands the replace commit), then deleting
   * the completed replace commit from the timeline. This leaves the table in a state where
   * files are in stash, source is empty, and no replace commit exists — exactly the scenario
   * where the validator succeeded but the commit failed to persist.
   */
  @Test
  public void testRetryAfterValidatorSucceededButCrashedBeforeCommit() throws IOException {
    insertRecords("001", 30, true);

    HoodieStorage storage = hoodieStorage();
    String[] partitions = {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
    String allPartitions = String.join(",", partitions);

    // Step 1: Run a real stash — this moves files and lands a replace commit
    HoodieStashPartitionsTool.Config stashCfg = buildConfig(allPartitions, "stash", true);
    new HoodieStashPartitionsTool(jsc(), stashCfg).run();

    // Verify stash completed: files in stash, source empty, replace commit exists
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());

    // Step 2: Delete the completed replace commit to simulate a crash before commit landed
    metaClient.getActiveTimeline().getCompletedReplaceTimeline().getInstantsAsStream()
        .forEach(instant -> metaClient.getActiveTimeline().deleteInstantFileIfExists(instant));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty(),
        "Replace commit should be removed to simulate crash");

    // Step 3: Retry with the tool — should restore files from stash, then re-run stash
    HoodieStashPartitionsTool.Config retryCfg = buildConfig(allPartitions, "stash", true);
    new HoodieStashPartitionsTool(jsc(), retryCfg).run();

    // Verify: replace commit exists now
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());

    // Stash files should still be there
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    }
  }

  // ---- Helper methods ----

  private void insertRecords(String commitTime, int numRecords, boolean enableMetadata) throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build())
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);
      WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
      JavaRDD<WriteStatus> statusesRdd = writeClient.insert(writeRecords, commitTime);
      List<WriteStatus> statuses = statusesRdd.collect();
      assertNoWriteErrors(statuses);
      writeClient.commit(commitTime, jsc().parallelize(statuses));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private void assertSourcePartitionEmpty(HoodieStorage storage, StoragePath sourcePartition) throws IOException {
    if (storage.exists(sourcePartition)) {
      List<StoragePathInfo> remainingFiles = storage.listDirectEntries(sourcePartition).stream()
          .filter(f -> !f.getPath().getName().startsWith("."))
          .collect(Collectors.toList());
      assertTrue(remainingFiles.isEmpty(),
          "Source partition should have no data files after stash, but found: " + remainingFiles);
    }
  }

  private HoodieStashPartitionsTool.Config buildConfig(String partitions, String mode, boolean enableMetadata) {
    HoodieStashPartitionsTool.Config cfg = new HoodieStashPartitionsTool.Config();
    cfg.basePath = basePath();
    cfg.tableName = "test-trip-table";
    cfg.stashPath = stashPath;
    cfg.partitions = partitions;
    cfg.runningMode = mode;
    cfg.parallelism = 2;
    if (enableMetadata) {
      cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=true");
    } else {
      cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=false");
    }
    return cfg;
  }
}
