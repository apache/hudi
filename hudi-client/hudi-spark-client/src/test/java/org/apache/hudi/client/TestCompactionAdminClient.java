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

import org.apache.hudi.client.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TestCompactionAdminClient extends HoodieClientTestBase {

  private HoodieTableMetaClient metaClient;
  private CompactionAdminClient client;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath, MERGE_ON_READ);
    client = new CompactionAdminClient(context, basePath);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    cleanupResources();
  }

  @Test
  public void testUnscheduleCompactionPlan() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    // There are delta-commits after compaction instant
    validateUnSchedulePlan(client, "000", "001", numEntriesPerInstant);
    // There are delta-commits after compaction instant
    validateUnSchedulePlan(client, "002", "003", numEntriesPerInstant);
    // There are no delta-commits after compaction instant
    validateUnSchedulePlan(client, "004", "005", numEntriesPerInstant);
    // There are no delta-commits after compaction instant
    validateUnSchedulePlan(client, "006", "007", numEntriesPerInstant);
  }

  @Test
  public void testUnscheduleCompactionFileId() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    Map<String, CompactionOperation> instantsWithOp =
        Stream.of("001", "003", "005", "007").map(instant -> {
          try {
            return Pair.of(instant, CompactionUtils.getCompactionPlan(metaClient, instant));
          } catch (Exception ioe) {
            throw new HoodieException(ioe);
          }
        }).map(instantWithPlan -> instantWithPlan.getRight().getOperations().stream()
            .map(op -> Pair.of(instantWithPlan.getLeft(), CompactionOperation.convertFromAvroRecordInstance(op)))
            .findFirst().get()).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    // There are delta-commits after compaction instant
    validateUnScheduleFileId(client, "000", "001", instantsWithOp.get("001"));
    // There are delta-commits after compaction instant
    validateUnScheduleFileId(client, "002", "003", instantsWithOp.get("003"));
    // There are no delta-commits after compaction instant
    validateUnScheduleFileId(client, "004", "005", instantsWithOp.get("005"));
    // There are no delta-commits after compaction instant
    validateUnScheduleFileId(client, "006", "007", instantsWithOp.get("007"));
  }

  @Test
  public void testRepairCompactionPlan() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    // There are delta-commits after compaction instant
    validateRepair("000", "001", numEntriesPerInstant);
    // There are delta-commits after compaction instant
    validateRepair("002", "003", numEntriesPerInstant);
    // There are no delta-commits after compaction instant
    validateRepair("004", "005", numEntriesPerInstant);
    // There are no delta-commits after compaction instant
    validateRepair("006", "007", numEntriesPerInstant);
  }

  private void validateRepair(String ingestionInstant, String compactionInstant, int numEntriesPerInstant) throws Exception {
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        validateUnSchedulePlan(client, ingestionInstant, compactionInstant, numEntriesPerInstant);
    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
    assertFalse(metaClient.getCommitsAndCompactionTimeline().containsInstant(compactionInstant), "Compaction should be unscheduled");
    assertTrue(renameFiles.isEmpty(), "Rename Files must be empty");
  }

  /**
   * Ensure compaction plan is valid.
   *
   * @param compactionInstant Compaction Instant
   */
  private void ensureValidCompactionPlan(String compactionInstant) throws Exception {
    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
    // Ensure compaction-plan is good to begin with
    List<ValidationOpResult> validationResults = client.validateCompactionPlan(metaClient, compactionInstant, 1);
    assertFalse(validationResults.stream().anyMatch(v -> !v.isSuccess()),
        "Some validations failed");
  }

  private void validateRenameFiles(List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles, String ingestionInstant,
      String compactionInstant, HoodieTableFileSystemView fsView) {
    // Ensure new names of log-files are on expected lines
    Set<HoodieLogFile> uniqNewLogFiles = new HashSet<>();
    Set<HoodieLogFile> uniqOldLogFiles = new HashSet<>();

    renameFiles.forEach(lfPair -> {
      assertFalse(uniqOldLogFiles.contains(lfPair.getKey()), "Old Log File Names do not collide");
      assertFalse(uniqNewLogFiles.contains(lfPair.getValue()), "New Log File Names do not collide");
      uniqOldLogFiles.add(lfPair.getKey());
      uniqNewLogFiles.add(lfPair.getValue());
    });

    renameFiles.forEach(lfPair -> {
      HoodieLogFile oldLogFile = lfPair.getLeft();
      HoodieLogFile newLogFile = lfPair.getValue();
      assertEquals(ingestionInstant, newLogFile.getDeltaCommitTime(), "Base Commit time of ingestion instant is expected");
      assertEquals(compactionInstant, oldLogFile.getDeltaCommitTime(), "Base Commit time of compaction instant is expected");
      assertEquals(oldLogFile.getFileId(), newLogFile.getFileId(), "File Id is expected");
      HoodieLogFile lastLogFileBeforeCompaction =
          fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], ingestionInstant)
              .filter(fs -> fs.getFileId().equals(oldLogFile.getFileId())).map(fs -> fs.getLogFiles().findFirst().get())
              .findFirst().get();
      assertEquals(lastLogFileBeforeCompaction.getLogVersion() + oldLogFile.getLogVersion(),
          newLogFile.getLogVersion(), "Log Version expected");
      assertTrue(newLogFile.getLogVersion() > lastLogFileBeforeCompaction.getLogVersion(),
          "Log version does not collide");
    });
  }

  /**
   * Validate Unschedule operations.
   */
  private List<Pair<HoodieLogFile, HoodieLogFile>> validateUnSchedulePlan(CompactionAdminClient client,
      String ingestionInstant, String compactionInstant, int numEntriesPerInstant) throws Exception {

    ensureValidCompactionPlan(compactionInstant);

    // Check suggested rename operations
    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);

    // Log files belonging to file-slices created because of compaction request should not be renamed
    // because the file slicing is based on completion time.

    final HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getCommitsAndCompactionTimeline());

    Map<String, Long> fileIdToCountsBeforeRenaming =
        fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Call the main unschedule API

    client.unscheduleCompactionPlan(compactionInstant, false, 1, false);

    metaClient.reloadActiveTimeline();
    final HoodieTableFileSystemView newFsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getCommitsAndCompactionTimeline());
    Set<String> commitsWithDataFile = CollectionUtils.createSet("000", "004");
    // Expect each file-slice whose base-commit is same as compaction commit to contain no new Log files
    newFsView.getLatestFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant, true)
        .filter(fs -> fs.getBaseInstantTime().compareTo(compactionInstant) <= 0)
        .forEach(fs -> {
          if (commitsWithDataFile.contains(fs.getBaseInstantTime())) {
            assertTrue(fs.getBaseFile().isPresent(), "Data file must be present");
          } else {
            assertFalse(fs.getBaseFile().isPresent(), "No Data file should be present");
          }
          assertEquals(2, fs.getLogFiles().count(), "Has Log Files");
        });

    // Ensure same number of log-files before and after renaming per fileId
    Map<String, Long> fileIdToCountsAfterRenaming =
        newFsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], ingestionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    assertEquals(fileIdToCountsBeforeRenaming, fileIdToCountsAfterRenaming,
        "Each File Id has same number of log-files");
    assertEquals(numEntriesPerInstant, fileIdToCountsAfterRenaming.size(), "Not Empty");
    return new ArrayList<>();
  }

  /**
   * Validate Unschedule operations.
   */
  private void validateUnScheduleFileId(CompactionAdminClient client, String ingestionInstant, String compactionInstant, CompactionOperation op) throws Exception {

    ensureValidCompactionPlan(compactionInstant);

    // Check suggested rename operations
    metaClient.reloadActiveTimeline();

    // Log files belonging to file-slices created because of compaction request must be renamed

    final HoodieTableFileSystemView fsView =
        HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getCommitsAndCompactionTimeline());

    Map<String, Long> fileIdToCountsBeforeRenaming =
        fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .filter(fs -> fs.getFileId().equals(op.getFileId()))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Call the main unschedule API
    client.unscheduleCompactionFileId(op.getFileGroupId(), false, false);

    metaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
    final HoodieTableFileSystemView newFsView =
        HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getCommitsAndCompactionTimeline());
    // Expect all file-slice whose base-commit is same as compaction commit to contain no new Log files
    newFsView.getLatestFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant, true)
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant))
        .filter(fs -> fs.getFileId().equals(op.getFileId()))
        .forEach(fs -> {
          assertFalse(fs.getBaseFile().isPresent(), "No Data file must be present");
          assertEquals(0, fs.getLogFiles().count(), "No Log Files");
        });

    // Ensure same number of log-files before and after renaming per fileId
    Map<String, Long> fileIdToCountsAfterRenaming =
        newFsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], ingestionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .filter(fs -> fs.getFileId().equals(op.getFileId()))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    assertEquals(fileIdToCountsBeforeRenaming, fileIdToCountsAfterRenaming,
        "Each File Id has same number of log-files");
    assertEquals(1, fileIdToCountsAfterRenaming.size(), "Not Empty");
  }
}
