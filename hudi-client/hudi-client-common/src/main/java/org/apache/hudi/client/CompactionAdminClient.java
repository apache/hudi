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

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.action.compact.OperationResult;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Client to perform admin operations related to compaction.
 */
@Slf4j
public class CompactionAdminClient extends BaseHoodieClient {

  public CompactionAdminClient(HoodieEngineContext context, String basePath) {
    super(context, HoodieWriteConfig.newBuilder().withPath(basePath).build());
  }

  /**
   * Validate all compaction operations in a compaction plan. Verifies the file-slices are consistent with corresponding
   * compaction operations.
   *
   * @param metaClient Hoodie Table Meta Client
   * @param compactionInstant Compaction Instant
   */
  public List<ValidationOpResult> validateCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant,
      int parallelism) throws IOException {
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getCommitsAndCompactionTimeline());

    if (plan.getOperations() != null) {
      List<CompactionOperation> ops = plan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      context.setJobStatus(this.getClass().getSimpleName(), "Validate compaction operations: " + config.getTableName());
      return context.map(ops, op -> {
        try {
          return validateCompactionOperation(metaClient, compactionInstant, op, fsView);
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }, parallelism);
    }
    return new ArrayList<>();
  }

  /**
   * Un-schedules compaction plan. Remove All compaction operation scheduled.
   *
   * @param compactionInstant Compaction Instant
   * @param skipValidation Skip validation step
   * @param parallelism Parallelism
   * @param dryRun Dry Run
   */
  public List<RenameOpResult> unscheduleCompactionPlan(String compactionInstant, boolean skipValidation,
      int parallelism, boolean dryRun) throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    // Only if all operations are successfully executed
    if (!dryRun) {
      // Overwrite compaction request with empty compaction operations
      HoodieInstant inflight = metaClient.createNewInstant(State.INFLIGHT, COMPACTION_ACTION, compactionInstant);
      StoragePath inflightPath = new StoragePath(metaClient.getTimelinePath(), metaClient.getInstantFileNameGenerator().getFileName(inflight));
      if (metaClient.getStorage().exists(inflightPath)) {
        // We need to rollback data-files because of this inflight compaction before unscheduling
        throw new IllegalStateException("Please rollback the inflight compaction before unscheduling");
      }
      // Leave the trace in aux folder but delete from metapath.
      // TODO: Add a rollback instant but for compaction
      HoodieInstant instant = metaClient.createNewInstant(State.REQUESTED, COMPACTION_ACTION, compactionInstant);
      boolean deleted = metaClient.getStorage().deleteFile(
          new StoragePath(metaClient.getTimelinePath(), instantFileNameGenerator.getFileName(instant)));
      ValidationUtils.checkArgument(deleted, "Unable to delete compaction instant.");
    }
    return new ArrayList<>();
  }

  /**
   * Remove a fileId from pending compaction. Removes the associated compaction operation and rename delta-files that
   * were generated for that file-id after the compaction operation was scheduled.
   *
   * This operation MUST be executed with compactions and writer turned OFF.
   *
   * @param fgId FileGroupId to be unscheduled
   * @param skipValidation Skip validation
   * @param dryRun Dry Run Mode
   */
  public List<RenameOpResult> unscheduleCompactionFileId(HoodieFileGroupId fgId, boolean skipValidation, boolean dryRun)
      throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();

    if (!dryRun) {
      // Ready to remove this file-Id from compaction request
      Pair<String, HoodieCompactionOperation> compactionOperationWithInstant =
          CompactionUtils.getAllPendingCompactionOperations(metaClient).get(fgId);
      HoodieCompactionPlan plan =
          CompactionUtils.getCompactionPlan(metaClient, compactionOperationWithInstant.getKey());
      List<HoodieCompactionOperation> newOps = plan.getOperations().stream().filter(op ->
              (!op.getFileId().equals(fgId.getFileId())) && (!op.getPartitionPath().equals(fgId.getPartitionPath())))
          .collect(Collectors.toList());
      if (newOps.size() == plan.getOperations().size()) {
        return new ArrayList<>();
      }
      HoodieCompactionPlan newPlan =
          HoodieCompactionPlan.newBuilder().setOperations(newOps).setExtraMetadata(plan.getExtraMetadata()).build();
      HoodieInstant inflight =
          metaClient.createNewInstant(State.INFLIGHT, COMPACTION_ACTION, compactionOperationWithInstant.getLeft());
      StoragePath inflightPath = new StoragePath(metaClient.getTimelinePath(), instantFileNameGenerator.getFileName(inflight));
      if (metaClient.getStorage().exists(inflightPath)) {
        // revert if in inflight state
        metaClient.getActiveTimeline().revertInstantFromInflightToRequested(inflight);
      }
      // Overwrite compaction plan with updated info
      metaClient.getActiveTimeline().saveToCompactionRequested(
          metaClient.createNewInstant(State.REQUESTED, COMPACTION_ACTION, compactionOperationWithInstant.getLeft()), newPlan, true);
    }
    return new ArrayList<>();
  }

  /**
   * Renames delta files to make file-slices consistent with the timeline as dictated by Hoodie metadata. Use when
   * compaction unschedule fails partially.
   *
   * This operation MUST be executed with compactions and writer turned OFF.
   * 
   * @param compactionInstant Compaction Instant to be repaired
   * @param dryRun Dry Run Mode
   */
  public List<RenameOpResult> repairCompaction(String compactionInstant, int parallelism, boolean dryRun)
      throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    validateCompactionPlan(metaClient, compactionInstant, parallelism);

    return new ArrayList<>();
  }

  /**
   * Construction Compaction Plan from compaction instant.
   */
  private static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant)
      throws IOException {
    return metaClient.getActiveTimeline().readCompactionPlan(
        metaClient.getInstantGenerator().getCompactionRequestedInstant(compactionInstant));
  }

  /**
   * Rename log files. This is done for un-scheduling a pending compaction operation NOTE: Can only be used safely when
   * no writer (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table Meta-Client
   * @param oldLogFile Old Log File
   * @param newLogFile New Log File
   */
  protected static void renameLogFile(HoodieTableMetaClient metaClient, HoodieLogFile oldLogFile,
      HoodieLogFile newLogFile) throws IOException {
    List<StoragePathInfo> pathInfoList =
        metaClient.getStorage().listDirectEntries(oldLogFile.getPath());
    ValidationUtils.checkArgument(pathInfoList.size() == 1, "Only one status must be present");
    ValidationUtils.checkArgument(pathInfoList.get(0).isFile(), "Source File must exist");
    ValidationUtils.checkArgument(
        oldLogFile.getPath().getParent().equals(newLogFile.getPath().getParent()),
        "Log file must only be moved within the parent directory");
    metaClient.getStorage().rename(oldLogFile.getPath(), newLogFile.getPath());
  }

  /**
   * Check if a compaction operation is valid.
   *
   * @param metaClient Hoodie Table Meta client
   * @param compactionInstant Compaction Instant
   * @param operation Compaction Operation
   * @param fileSystemView File System View
   */
  private ValidationOpResult validateCompactionOperation(HoodieTableMetaClient metaClient, String compactionInstant,
      CompactionOperation operation,HoodieTableFileSystemView fileSystemView) throws IOException {
    Option<HoodieInstant> lastInstant = metaClient.getCommitsAndCompactionTimeline().lastInstant();
    try {
      if (lastInstant.isPresent()) {
        Option<FileSlice> fileSliceOptional =
            Option.fromJavaOptional(fileSystemView.getLatestUnCompactedFileSlices(operation.getPartitionPath())
                .filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst());
        if (fileSliceOptional.isPresent()) {
          FileSlice fs = fileSliceOptional.get();
          Option<HoodieBaseFile> df = fs.getBaseFile();
          if (operation.getDataFileName().isPresent()) {
            String expPath = metaClient.getStorage()
                .getPathInfo(new StoragePath(
                    FSUtils.constructAbsolutePath(metaClient.getBasePath(), operation.getPartitionPath()),
                    operation.getDataFileName().get()))
                .getPath().toString();
            ValidationUtils.checkArgument(df.isPresent(),
                "Data File must be present. File Slice was : " + fs + ", operation :" + operation);
            ValidationUtils.checkArgument(df.get().getPath().equals(expPath),
                "Base Path in operation is specified as " + expPath + " but got path " + df.get().getPath());
          }
          Set<HoodieLogFile> logFilesInFileSlice = fs.getLogFiles().collect(Collectors.toSet());
          Set<HoodieLogFile> logFilesInCompactionOp = operation.getDeltaFileNames().stream().map(dp -> {
            try {
              List<StoragePathInfo> pathInfoList = metaClient.getStorage()
                  .listDirectEntries(new StoragePath(
                      FSUtils.constructAbsolutePath(metaClient.getBasePath(), operation.getPartitionPath()), dp));
              ValidationUtils.checkArgument(pathInfoList.size() == 1, "Expect only 1 file-status");
              return new HoodieLogFile(pathInfoList.get(0));
            } catch (FileNotFoundException fe) {
              throw new CompactionValidationException(fe.getMessage());
            } catch (IOException ioe) {
              throw new HoodieIOException(ioe.getMessage(), ioe);
            }
          }).collect(Collectors.toSet());
          Set<HoodieLogFile> missing = logFilesInCompactionOp.stream().filter(lf -> !logFilesInFileSlice.contains(lf))
              .collect(Collectors.toSet());
          ValidationUtils.checkArgument(missing.isEmpty(),
              "All log files specified in compaction operation is not present. Missing :" + missing + ", Exp :"
                  + logFilesInCompactionOp + ", Got :" + logFilesInFileSlice);
          Set<HoodieLogFile> diff = logFilesInFileSlice.stream().filter(lf -> !logFilesInCompactionOp.contains(lf))
              .collect(Collectors.toSet());
          ValidationUtils.checkArgument(diff.stream().allMatch(lf -> compareTimestamps(lf.getDeltaCommitTime(), GREATER_THAN_OR_EQUALS, compactionInstant)),
              "There are some log-files which are neither specified in compaction plan "
                  + "nor present after compaction request instant. Some of these :" + diff);
        } else {
          throw new CompactionValidationException(
              "Unable to find file-slice for file-id (" + operation.getFileId() + " Compaction operation is invalid.");
        }
      } else {
        throw new CompactionValidationException(
            "Unable to find any committed instant. Compaction Operation may be pointing to stale file-slices");
      }
    } catch (CompactionValidationException | IllegalArgumentException e) {
      return new ValidationOpResult(operation, false, Option.of(e));
    }
    return new ValidationOpResult(operation, true, Option.empty());
  }

  /**
   * Execute Renaming operation.
   *
   * @param metaClient HoodieTable MetaClient
   * @param renameActions List of rename operations
   */
  private List<RenameOpResult> runRenamingOps(HoodieTableMetaClient metaClient,
      List<Pair<HoodieLogFile, HoodieLogFile>> renameActions, int parallelism, boolean dryRun) {
    if (renameActions.isEmpty()) {
      log.info("No renaming of log-files needed. Proceeding to removing file-id from compaction-plan");
      return new ArrayList<>();
    } else {
      log.info("The following compaction renaming operations needs to be performed to un-schedule");
      if (!dryRun) {
        context.setJobStatus(this.getClass().getSimpleName(), "Execute unschedule operations: " + config.getTableName());
        return context.map(renameActions, lfPair -> {
          try {
            log.info("RENAME " + lfPair.getLeft().getPath() + " => " + lfPair.getRight().getPath());
            renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
            return new RenameOpResult(lfPair, true, Option.empty());
          } catch (IOException e) {
            log.error("Error renaming log file", e);
            log.error("\n\n\n***NOTE Compaction is in inconsistent state. Try running \"compaction repair "
                + lfPair.getLeft().getDeltaCommitTime() + "\" to recover from failure ***\n\n\n");
            return new RenameOpResult(lfPair, false, Option.of(e));
          }
        }, parallelism);
      } else {
        log.info("Dry-Run Mode activated for rename operations");
        return renameActions.parallelStream().map(lfPair -> new RenameOpResult(lfPair, false, false, Option.empty()))
            .collect(Collectors.toList());
      }
    }
  }

  @Override
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op
  }

  /**
   * Holds Operation result for Renaming.
   */
  @NoArgsConstructor
  public static class RenameOpResult extends OperationResult<RenameInfo> {

    public RenameOpResult(Pair<HoodieLogFile, HoodieLogFile> op, boolean success, Option<Exception> exception) {
      super(
          new RenameInfo(op.getKey().getFileId(), op.getKey().getPath().toString(), op.getRight().getPath().toString()),
          success, exception);
    }

    public RenameOpResult(Pair<HoodieLogFile, HoodieLogFile> op, boolean executed, boolean success,
        Option<Exception> exception) {
      super(
          new RenameInfo(op.getKey().getFileId(), op.getKey().getPath().toString(), op.getRight().getPath().toString()),
          executed, success, exception);
    }
  }

  /**
   * Holds Operation result for Renaming.
   */
  public static class ValidationOpResult extends OperationResult<CompactionOperation> {
    public ValidationOpResult(CompactionOperation operation, boolean success, Option<Exception> exception) {
      super(operation, success, exception);
    }
  }

  public static class RenameInfo implements Serializable {

    public String fileId;
    public String srcPath;
    public String destPath;

    public RenameInfo(String fileId, String srcPath, String destPath) {
      this.fileId = fileId;
      this.srcPath = srcPath;
      this.destPath = destPath;
    }
  }

  public static class CompactionValidationException extends RuntimeException {

    public CompactionValidationException(String msg) {
      super(msg);
    }
  }
}
