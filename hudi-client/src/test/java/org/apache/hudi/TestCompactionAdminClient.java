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

package org.apache.hudi;

import org.apache.hudi.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CompactionTestUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

public class TestCompactionAdminClient extends TestHoodieClientBase {

  private HoodieTableMetaClient metaClient;
  private CompactionAdminClient client;

  @Before
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath, MERGE_ON_READ);
    client = new CompactionAdminClient(jsc, basePath);
  }

  @After
  public void tearDown() {
    client.close();
    metaClient = null;
    cleanupSparkContexts();
  }

  @Test
  public void testUnscheduleCompactionPlan() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateUnSchedulePlan(client, "000", "001", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateUnSchedulePlan(client, "002", "003", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are no delta-commits after compaction instant
    validateUnSchedulePlan(client, "004", "005", numEntriesPerInstant, 0);
    // THere are no delta-commits after compaction instant
    validateUnSchedulePlan(client, "006", "007", numEntriesPerInstant, 0);
  }

  @Test
  public void testUnscheduleCompactionFileId() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    Map<String, CompactionOperation> instantsWithOp =
        Arrays.asList("001", "003", "005", "007").stream().map(instant -> {
          try {
            return Pair.of(instant, CompactionUtils.getCompactionPlan(metaClient, instant));
          } catch (IOException ioe) {
            throw new HoodieException(ioe);
          }
        }).map(instantWithPlan -> instantWithPlan.getRight().getOperations().stream()
            .map(op -> Pair.of(instantWithPlan.getLeft(), CompactionOperation.convertFromAvroRecordInstance(op)))
            .findFirst().get()).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    // THere are delta-commits after compaction instant
    validateUnScheduleFileId(client, "000", "001", instantsWithOp.get("001"), 2);
    // THere are delta-commits after compaction instant
    validateUnScheduleFileId(client, "002", "003", instantsWithOp.get("003"), 2);
    // THere are no delta-commits after compaction instant
    validateUnScheduleFileId(client, "004", "005", instantsWithOp.get("005"), 0);
    // THere are no delta-commits after compaction instant
    validateUnScheduleFileId(client, "006", "007", instantsWithOp.get("007"), 0);
  }

  @Test
  public void testRepairCompactionPlan() throws Exception {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateRepair("000", "001", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateRepair("002", "003", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are no delta-commits after compaction instant
    validateRepair("004", "005", numEntriesPerInstant, 0);
    // THere are no delta-commits after compaction instant
    validateRepair("006", "007", numEntriesPerInstant, 0);
  }

  private void validateRepair(String ingestionInstant, String compactionInstant, int numEntriesPerInstant,
      int expNumRepairs) throws Exception {
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        validateUnSchedulePlan(client, ingestionInstant, compactionInstant, numEntriesPerInstant, expNumRepairs, true);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    List<ValidationOpResult> result = client.validateCompactionPlan(metaClient, compactionInstant, 1);
    if (expNumRepairs > 0) {
      Assert.assertTrue("Expect some failures in validation", result.stream().filter(r -> !r.isSuccess()).count() > 0);
    }
    // Now repair
    List<Pair<HoodieLogFile, HoodieLogFile>> undoFiles =
        result.stream().flatMap(r -> client.getRenamingActionsToAlignWithCompactionOperation(metaClient,
            compactionInstant, r.getOperation(), Option.empty()).stream()).map(rn -> {
              try {
                client.renameLogFile(metaClient, rn.getKey(), rn.getValue());
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
              return rn;
            }).collect(Collectors.toList());
    Map<String, String> renameFilesFromUndo = undoFiles.stream()
        .collect(Collectors.toMap(p -> p.getRight().getPath().toString(), x -> x.getLeft().getPath().toString()));
    Map<String, String> expRenameFiles = renameFiles.stream()
        .collect(Collectors.toMap(p -> p.getLeft().getPath().toString(), x -> x.getRight().getPath().toString()));
    if (expNumRepairs > 0) {
      Assert.assertFalse("Rename Files must be non-empty", renameFiles.isEmpty());
    } else {
      Assert.assertTrue("Rename Files must be empty", renameFiles.isEmpty());
    }
    expRenameFiles.entrySet().stream().forEach(r -> {
      logger.info("Key :" + r.getKey() + " renamed to " + r.getValue() + " rolled back to "
          + renameFilesFromUndo.get(r.getKey()));
    });

    Assert.assertEquals("Undo must completely rollback renames", expRenameFiles, renameFilesFromUndo);
    // Now expect validation to succeed
    result = client.validateCompactionPlan(metaClient, compactionInstant, 1);
    Assert.assertTrue("Expect no failures in validation", result.stream().filter(r -> !r.isSuccess()).count() == 0);
    Assert.assertEquals("Expected Num Repairs", expNumRepairs, undoFiles.size());
  }

  /**
   * Enssure compaction plan is valid.
   *
   * @param compactionInstant Compaction Instant
   */
  private void ensureValidCompactionPlan(String compactionInstant) throws Exception {
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    // Ensure compaction-plan is good to begin with
    List<ValidationOpResult> validationResults = client.validateCompactionPlan(metaClient, compactionInstant, 1);
    Assert.assertFalse("Some validations failed",
        validationResults.stream().filter(v -> !v.isSuccess()).findAny().isPresent());
  }

  private void validateRenameFiles(List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles, String ingestionInstant,
      String compactionInstant, HoodieTableFileSystemView fsView) {
    // Ensure new names of log-files are on expected lines
    Set<HoodieLogFile> uniqNewLogFiles = new HashSet<>();
    Set<HoodieLogFile> uniqOldLogFiles = new HashSet<>();

    renameFiles.stream().forEach(lfPair -> {
      Assert.assertFalse("Old Log File Names do not collide", uniqOldLogFiles.contains(lfPair.getKey()));
      Assert.assertFalse("New Log File Names do not collide", uniqNewLogFiles.contains(lfPair.getValue()));
      uniqOldLogFiles.add(lfPair.getKey());
      uniqNewLogFiles.add(lfPair.getValue());
    });

    renameFiles.stream().forEach(lfPair -> {
      HoodieLogFile oldLogFile = lfPair.getLeft();
      HoodieLogFile newLogFile = lfPair.getValue();
      Assert.assertEquals("Base Commit time is expected", ingestionInstant, newLogFile.getBaseCommitTime());
      Assert.assertEquals("Base Commit time is expected", compactionInstant, oldLogFile.getBaseCommitTime());
      Assert.assertEquals("File Id is expected", oldLogFile.getFileId(), newLogFile.getFileId());
      HoodieLogFile lastLogFileBeforeCompaction =
          fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], ingestionInstant)
              .filter(fs -> fs.getFileId().equals(oldLogFile.getFileId())).map(fs -> fs.getLogFiles().findFirst().get())
              .findFirst().get();
      Assert.assertEquals("Log Version expected",
          lastLogFileBeforeCompaction.getLogVersion() + oldLogFile.getLogVersion(), newLogFile.getLogVersion());
      Assert.assertTrue("Log version does not collide",
          newLogFile.getLogVersion() > lastLogFileBeforeCompaction.getLogVersion());
    });
  }

  /**
   * Validate Unschedule operations.
   */
  private List<Pair<HoodieLogFile, HoodieLogFile>> validateUnSchedulePlan(CompactionAdminClient client,
      String ingestionInstant, String compactionInstant, int numEntriesPerInstant, int expNumRenames) throws Exception {
    return validateUnSchedulePlan(client, ingestionInstant, compactionInstant, numEntriesPerInstant, expNumRenames,
        false);
  }

  /**
   * Validate Unschedule operations.
   */
  private List<Pair<HoodieLogFile, HoodieLogFile>> validateUnSchedulePlan(CompactionAdminClient client,
      String ingestionInstant, String compactionInstant, int numEntriesPerInstant, int expNumRenames,
      boolean skipUnSchedule) throws Exception {

    ensureValidCompactionPlan(compactionInstant);

    // Check suggested rename operations
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        client.getRenamingActionsForUnschedulingCompactionPlan(metaClient, compactionInstant, 1, Option.empty(), false);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);

    // Log files belonging to file-slices created because of compaction request must be renamed

    Set<HoodieLogFile> gotLogFilesToBeRenamed = renameFiles.stream().map(p -> p.getLeft()).collect(Collectors.toSet());
    final HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    Set<HoodieLogFile> expLogFilesToBeRenamed = fsView.getLatestFileSlices(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0])
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant)).flatMap(fs -> fs.getLogFiles())
        .collect(Collectors.toSet());
    Assert.assertEquals("Log files belonging to file-slices created because of compaction request must be renamed",
        expLogFilesToBeRenamed, gotLogFilesToBeRenamed);

    if (skipUnSchedule) {
      // Do the renaming only but do not touch the compaction plan - Needed for repair tests
      renameFiles.stream().forEach(lfPair -> {
        try {
          client.renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      });
    } else {
      validateRenameFiles(renameFiles, ingestionInstant, compactionInstant, fsView);
    }

    Map<String, Long> fileIdToCountsBeforeRenaming =
        fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Call the main unschedule API

    client.unscheduleCompactionPlan(compactionInstant, false, 1, false);

    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    final HoodieTableFileSystemView newFsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    // Expect all file-slice whose base-commit is same as compaction commit to contain no new Log files
    newFsView.getLatestFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant, true)
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant)).forEach(fs -> {
          Assert.assertFalse("No Data file must be present", fs.getDataFile().isPresent());
          Assert.assertTrue("No Log Files", fs.getLogFiles().count() == 0);
        });

    // Ensure same number of log-files before and after renaming per fileId
    Map<String, Long> fileIdToCountsAfterRenaming =
        newFsView.getAllFileGroups(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0]).flatMap(fg -> fg.getAllFileSlices())
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Assert.assertEquals("Each File Id has same number of log-files", fileIdToCountsBeforeRenaming,
        fileIdToCountsAfterRenaming);
    Assert.assertEquals("Not Empty", numEntriesPerInstant, fileIdToCountsAfterRenaming.size());
    Assert.assertEquals("Expected number of renames", expNumRenames, renameFiles.size());
    return renameFiles;
  }

  /**
   * Validate Unschedule operations.
   */
  private void validateUnScheduleFileId(CompactionAdminClient client, String ingestionInstant, String compactionInstant,
      CompactionOperation op, int expNumRenames) throws Exception {

    ensureValidCompactionPlan(compactionInstant);

    // Check suggested rename operations
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles = client
        .getRenamingActionsForUnschedulingCompactionOperation(metaClient, compactionInstant, op, Option.empty(), false);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);

    // Log files belonging to file-slices created because of compaction request must be renamed

    Set<HoodieLogFile> gotLogFilesToBeRenamed = renameFiles.stream().map(p -> p.getLeft()).collect(Collectors.toSet());
    final HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    Set<HoodieLogFile> expLogFilesToBeRenamed = fsView.getLatestFileSlices(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0])
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant))
        .filter(fs -> fs.getFileId().equals(op.getFileId())).flatMap(fs -> fs.getLogFiles())
        .collect(Collectors.toSet());
    Assert.assertEquals("Log files belonging to file-slices created because of compaction request must be renamed",
        expLogFilesToBeRenamed, gotLogFilesToBeRenamed);
    validateRenameFiles(renameFiles, ingestionInstant, compactionInstant, fsView);

    Map<String, Long> fileIdToCountsBeforeRenaming =
        fsView.getLatestMergedFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .filter(fs -> fs.getFileId().equals(op.getFileId()))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Call the main unschedule API
    client.unscheduleCompactionFileId(op.getFileGroupId(), false, false);

    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    final HoodieTableFileSystemView newFsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    // Expect all file-slice whose base-commit is same as compaction commit to contain no new Log files
    newFsView.getLatestFileSlicesBeforeOrOn(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0], compactionInstant, true)
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant))
        .filter(fs -> fs.getFileId().equals(op.getFileId())).forEach(fs -> {
          Assert.assertFalse("No Data file must be present", fs.getDataFile().isPresent());
          Assert.assertTrue("No Log Files", fs.getLogFiles().count() == 0);
        });

    // Ensure same number of log-files before and after renaming per fileId
    Map<String, Long> fileIdToCountsAfterRenaming =
        newFsView.getAllFileGroups(HoodieTestUtils.DEFAULT_PARTITION_PATHS[0]).flatMap(fg -> fg.getAllFileSlices())
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .filter(fs -> fs.getFileId().equals(op.getFileId()))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Assert.assertEquals("Each File Id has same number of log-files", fileIdToCountsBeforeRenaming,
        fileIdToCountsAfterRenaming);
    Assert.assertEquals("Not Empty", 1, fileIdToCountsAfterRenaming.size());
    Assert.assertEquals("Expected number of renames", expNumRenames, renameFiles.size());
  }
}
