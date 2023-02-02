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
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.compact.OperationResult;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;

/**
 * Client to perform admin operations related to compaction.
 */
public class CompactionAdminClient extends BaseHoodieClient {

  private static final Logger LOG = LogManager.getLogger(CompactionAdminClient.class);

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
    HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());

    if (plan.getOperations() != null) {
      List<CompactionOperation> ops = plan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      context.setJobStatus(this.getClass().getSimpleName(), "Validate compaction operations: " + config.getTableName());
      return context.map(ops, op -> {
        try {
          return validateCompactionOperation(metaClient, compactionInstant, op, Option.of(fsView));
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }, parallelism);
    }
    return new ArrayList<>();
  }

  /**
   * Un-schedules compaction plan. Remove All compaction operation scheduled and re-arrange delta-files that were
   * created after the compaction was scheduled.
   *
   * This operation MUST be executed with compactions and writer turned OFF.
   *
   * @param compactionInstant Compaction Instant
   * @param skipValidation Skip validation step
   * @param parallelism Parallelism
   * @param dryRun Dry Run
   */
  public List<RenameOpResult> unscheduleCompactionPlan(String compactionInstant, boolean skipValidation,
      int parallelism, boolean dryRun) throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions = getRenamingActionsForUnschedulingCompactionPlan(metaClient,
        compactionInstant, parallelism, Option.empty(), skipValidation);

    List<RenameOpResult> res = runRenamingOps(metaClient, renameActions, parallelism, dryRun);

    Option<Boolean> success =
        Option.fromJavaOptional(res.stream().map(r -> (r.isExecuted() && r.isSuccess())).reduce(Boolean::logicalAnd));
    Option<Boolean> allSuccess = success.isPresent() ? Option.of(success.get()) : Option.empty();

    // Only if all operations are successfully executed
    if (!dryRun && allSuccess.isPresent() && allSuccess.get()) {
      // Overwrite compaction request with empty compaction operations
      HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, compactionInstant);
      Path inflightPath = new Path(metaClient.getMetaPath(), inflight.getFileName());
      if (metaClient.getFs().exists(inflightPath)) {
        // We need to rollback data-files because of this inflight compaction before unscheduling
        throw new IllegalStateException("Please rollback the inflight compaction before unscheduling");
      }
      // Leave the trace in aux folder but delete from metapath.
      // TODO: Add a rollback instant but for compaction
      HoodieInstant instant = new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionInstant);
      boolean deleted = metaClient.getFs().delete(new Path(metaClient.getMetaPath(), instant.getFileName()), false);
      ValidationUtils.checkArgument(deleted, "Unable to delete compaction instant.");
    }
    return res;
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
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        getRenamingActionsForUnschedulingCompactionForFileId(metaClient, fgId, Option.empty(), skipValidation);

    List<RenameOpResult> res = runRenamingOps(metaClient, renameActions, 1, dryRun);

    if (!dryRun && !res.isEmpty() && res.get(0).isExecuted() && res.get(0).isSuccess()) {
      // Ready to remove this file-Id from compaction request
      Pair<String, HoodieCompactionOperation> compactionOperationWithInstant =
          CompactionUtils.getAllPendingCompactionOperations(metaClient).get(fgId);
      HoodieCompactionPlan plan =
          CompactionUtils.getCompactionPlan(metaClient, compactionOperationWithInstant.getKey());
      List<HoodieCompactionOperation> newOps = plan.getOperations().stream().filter(
          op -> (!op.getFileId().equals(fgId.getFileId())) && (!op.getPartitionPath().equals(fgId.getPartitionPath())))
          .collect(Collectors.toList());
      HoodieCompactionPlan newPlan =
          HoodieCompactionPlan.newBuilder().setOperations(newOps).setExtraMetadata(plan.getExtraMetadata()).build();
      HoodieInstant inflight =
          new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, compactionOperationWithInstant.getLeft());
      Path inflightPath = new Path(metaClient.getMetaPath(), inflight.getFileName());
      if (metaClient.getFs().exists(inflightPath)) {
        // revert if in inflight state
        metaClient.getActiveTimeline().revertInstantFromInflightToRequested(inflight);
      }
      // Overwrite compaction plan with updated info
      metaClient.getActiveTimeline().saveToCompactionRequested(
          new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionOperationWithInstant.getLeft()),
          TimelineMetadataUtils.serializeCompactionPlan(newPlan), true);
    }
    return res;
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
    List<ValidationOpResult> validationResults = validateCompactionPlan(metaClient, compactionInstant, parallelism);
    List<ValidationOpResult> failed =
        validationResults.stream().filter(v -> !v.isSuccess()).collect(Collectors.toList());
    if (failed.isEmpty()) {
      return new ArrayList<>();
    }

    final HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        failed.stream().flatMap(v -> getRenamingActionsToAlignWithCompactionOperation(metaClient, compactionInstant,
            v.getOperation(), Option.of(fsView)).stream()).collect(Collectors.toList());
    return runRenamingOps(metaClient, renameActions, parallelism, dryRun);
  }

  /**
   * Construction Compaction Plan from compaction instant.
   */
  private static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant)
      throws IOException {
    return TimelineMetadataUtils.deserializeCompactionPlan(
            metaClient.getActiveTimeline().readCompactionPlanAsBytes(
                    HoodieTimeline.getCompactionRequestedInstant(compactionInstant)).get());
  }

  /**
   * Get Renaming actions to ensure the log-files of merged file-slices is aligned with compaction operation. This
   * method is used to recover from failures during unschedule compaction operations.
   *
   * @param metaClient Hoodie Table Meta Client
   * @param compactionInstant Compaction Instant
   * @param op Compaction Operation
   * @param fsViewOpt File System View
   */
  protected static List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsToAlignWithCompactionOperation(
      HoodieTableMetaClient metaClient, String compactionInstant, CompactionOperation op,
      Option<HoodieTableFileSystemView> fsViewOpt) {
    HoodieTableFileSystemView fileSystemView = fsViewOpt.isPresent() ? fsViewOpt.get()
        : new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    HoodieInstant lastInstant = metaClient.getCommitsAndCompactionTimeline().lastInstant().get();
    FileSlice merged =
        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(op.getPartitionPath(), lastInstant.getTimestamp())
            .filter(fs -> fs.getFileId().equals(op.getFileId())).findFirst().get();
    final int maxVersion = op.getDeltaFileNames().stream().map(lf -> FSUtils.getFileVersionFromLog(new Path(lf)))
        .reduce((x, y) -> x > y ? x : y).orElse(0);
    List<HoodieLogFile> logFilesToBeMoved =
        merged.getLogFiles().filter(lf -> lf.getLogVersion() > maxVersion).collect(Collectors.toList());
    return logFilesToBeMoved.stream().map(lf -> {
      ValidationUtils.checkArgument(lf.getLogVersion() - maxVersion > 0, "Expect new log version to be sane");
      HoodieLogFile newLogFile = new HoodieLogFile(new Path(lf.getPath().getParent(),
          FSUtils.makeLogFileName(lf.getFileId(), "." + FSUtils.getFileExtensionFromLog(lf.getPath()),
              compactionInstant, lf.getLogVersion() - maxVersion, HoodieLogFormat.UNKNOWN_WRITE_TOKEN)));
      return Pair.of(lf, newLogFile);
    }).collect(Collectors.toList());
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
    FileStatus[] statuses = metaClient.getFs().listStatus(oldLogFile.getPath());
    ValidationUtils.checkArgument(statuses.length == 1, "Only one status must be present");
    ValidationUtils.checkArgument(statuses[0].isFile(), "Source File must exist");
    ValidationUtils.checkArgument(oldLogFile.getPath().getParent().equals(newLogFile.getPath().getParent()),
        "Log file must only be moved within the parent directory");
    metaClient.getFs().rename(oldLogFile.getPath(), newLogFile.getPath());
  }

  /**
   * Check if a compaction operation is valid.
   *
   * @param metaClient Hoodie Table Meta client
   * @param compactionInstant Compaction Instant
   * @param operation Compaction Operation
   * @param fsViewOpt File System View
   */
  private ValidationOpResult validateCompactionOperation(HoodieTableMetaClient metaClient, String compactionInstant,
      CompactionOperation operation, Option<HoodieTableFileSystemView> fsViewOpt) throws IOException {
    HoodieTableFileSystemView fileSystemView = fsViewOpt.isPresent() ? fsViewOpt.get()
        : new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
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
            String expPath = metaClient.getFs()
                .getFileStatus(
                    new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()),
                        new Path(operation.getDataFileName().get())))
                .getPath().toString();
            ValidationUtils.checkArgument(df.isPresent(),
                "Data File must be present. File Slice was : " + fs + ", operation :" + operation);
            ValidationUtils.checkArgument(df.get().getPath().equals(expPath),
                "Base Path in operation is specified as " + expPath + " but got path " + df.get().getPath());
          }
          Set<HoodieLogFile> logFilesInFileSlice = fs.getLogFiles().collect(Collectors.toSet());
          Set<HoodieLogFile> logFilesInCompactionOp = operation.getDeltaFileNames().stream().map(dp -> {
            try {
              FileStatus[] fileStatuses = metaClient.getFs().listStatus(new Path(
                  FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()), new Path(dp)));
              ValidationUtils.checkArgument(fileStatuses.length == 1, "Expect only 1 file-status");
              return new HoodieLogFile(fileStatuses[0]);
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
          ValidationUtils.checkArgument(diff.stream().allMatch(lf -> lf.getBaseCommitTime().equals(compactionInstant)),
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
      LOG.info("No renaming of log-files needed. Proceeding to removing file-id from compaction-plan");
      return new ArrayList<>();
    } else {
      LOG.info("The following compaction renaming operations needs to be performed to un-schedule");
      if (!dryRun) {
        context.setJobStatus(this.getClass().getSimpleName(), "Execute unschedule operations: " + config.getTableName());
        return context.map(renameActions, lfPair -> {
          try {
            LOG.info("RENAME " + lfPair.getLeft().getPath() + " => " + lfPair.getRight().getPath());
            renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
            return new RenameOpResult(lfPair, true, Option.empty());
          } catch (IOException e) {
            LOG.error("Error renaming log file", e);
            LOG.error("\n\n\n***NOTE Compaction is in inconsistent state. Try running \"compaction repair "
                + lfPair.getLeft().getBaseCommitTime() + "\" to recover from failure ***\n\n\n");
            return new RenameOpResult(lfPair, false, Option.of(e));
          }
        }, parallelism);
      } else {
        LOG.info("Dry-Run Mode activated for rename operations");
        return renameActions.parallelStream().map(lfPair -> new RenameOpResult(lfPair, false, false, Option.empty()))
            .collect(Collectors.toList());
      }
    }
  }

  /**
   * Generate renaming actions for unscheduling a pending compaction plan. NOTE: Can only be used safely when no writer
   * (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table MetaClient
   * @param compactionInstant Compaction Instant to be unscheduled
   * @param fsViewOpt Cached File System View
   * @param skipValidation Skip Validation
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   *         compaction.
   */
  public List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionPlan(
      HoodieTableMetaClient metaClient, String compactionInstant, int parallelism,
      Option<HoodieTableFileSystemView> fsViewOpt, boolean skipValidation) throws IOException {
    HoodieTableFileSystemView fsView = fsViewOpt.isPresent() ? fsViewOpt.get()
        : new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    if (plan.getOperations() != null) {
      LOG.info(
          "Number of Compaction Operations :" + plan.getOperations().size() + " for instant :" + compactionInstant);
      List<CompactionOperation> ops = plan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      context.setJobStatus(this.getClass().getSimpleName(), "Generate compaction unscheduling operations: " + config.getTableName());
      return context.flatMap(ops, op -> {
        try {
          return getRenamingActionsForUnschedulingCompactionOperation(metaClient, compactionInstant, op,
              Option.of(fsView), skipValidation).stream();
        } catch (IOException ioe) {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        } catch (CompactionValidationException ve) {
          throw new HoodieException(ve);
        }
      }, parallelism);
    }
    LOG.warn("No operations for compaction instant : " + compactionInstant);
    return new ArrayList<>();
  }

  /**
   * Generate renaming actions for unscheduling a compaction operation NOTE: Can only be used safely when no writer
   * (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table MetaClient
   * @param compactionInstant Compaction Instant
   * @param operation Compaction Operation
   * @param fsViewOpt Cached File System View
   * @param skipValidation Skip Validation
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   *         compaction.
   */
  public List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionOperation(
      HoodieTableMetaClient metaClient, String compactionInstant, CompactionOperation operation,
      Option<HoodieTableFileSystemView> fsViewOpt, boolean skipValidation) throws IOException {
    List<Pair<HoodieLogFile, HoodieLogFile>> result = new ArrayList<>();
    HoodieTableFileSystemView fileSystemView = fsViewOpt.isPresent() ? fsViewOpt.get()
        : new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    if (!skipValidation) {
      validateCompactionOperation(metaClient, compactionInstant, operation, Option.of(fileSystemView));
    }
    HoodieInstant lastInstant = metaClient.getCommitsAndCompactionTimeline().lastInstant().get();
    FileSlice merged =
        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(operation.getPartitionPath(), lastInstant.getTimestamp())
            .filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst().get();
    List<HoodieLogFile> logFilesToRepair =
        merged.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(compactionInstant))
            .sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    FileSlice fileSliceForCompaction =
        fileSystemView.getLatestFileSlicesBeforeOrOn(operation.getPartitionPath(), operation.getBaseInstantTime(), true)
            .filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst().get();
    int maxUsedVersion = fileSliceForCompaction.getLogFiles().findFirst().map(HoodieLogFile::getLogVersion)
        .orElse(HoodieLogFile.LOGFILE_BASE_VERSION - 1);
    String logExtn = fileSliceForCompaction.getLogFiles().findFirst().map(lf -> "." + lf.getFileExtension())
        .orElse(HoodieLogFile.DELTA_EXTENSION);
    String parentPath = fileSliceForCompaction.getBaseFile().map(df -> new Path(df.getPath()).getParent().toString())
        .orElse(fileSliceForCompaction.getLogFiles().findFirst().map(lf -> lf.getPath().getParent().toString()).get());
    for (HoodieLogFile toRepair : logFilesToRepair) {
      int version = maxUsedVersion + 1;
      HoodieLogFile newLf = new HoodieLogFile(new Path(parentPath, FSUtils.makeLogFileName(operation.getFileId(),
          logExtn, operation.getBaseInstantTime(), version, HoodieLogFormat.UNKNOWN_WRITE_TOKEN)));
      result.add(Pair.of(toRepair, newLf));
      maxUsedVersion = version;
    }
    return result;
  }

  /**
   * Generate renaming actions for unscheduling a fileId from pending compaction. NOTE: Can only be used safely when no
   * writer (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table MetaClient
   * @param fgId FileGroupId to remove compaction
   * @param fsViewOpt Cached File System View
   * @param skipValidation Skip Validation
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   *         compaction.
   */
  public List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionForFileId(
      HoodieTableMetaClient metaClient, HoodieFileGroupId fgId, Option<HoodieTableFileSystemView> fsViewOpt,
      boolean skipValidation) throws IOException {
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> allPendingCompactions =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
    if (allPendingCompactions.containsKey(fgId)) {
      Pair<String, HoodieCompactionOperation> opWithInstant = allPendingCompactions.get(fgId);
      return getRenamingActionsForUnschedulingCompactionOperation(metaClient, opWithInstant.getKey(),
          CompactionOperation.convertFromAvroRecordInstance(opWithInstant.getValue()), fsViewOpt, skipValidation);
    }
    throw new HoodieException("FileGroupId " + fgId + " not in pending compaction");
  }

  /**
   * Holds Operation result for Renaming.
   */
  public static class RenameOpResult extends OperationResult<RenameInfo> {

    public RenameOpResult() {}

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

    public ValidationOpResult() {}

    public ValidationOpResult(CompactionOperation operation, boolean success, Option<Exception> exception) {
      super(operation, success, exception);
    }
  }

  public static class RenameInfo implements Serializable {

    public String fileId;
    public String srcPath;
    public String destPath;

    public RenameInfo() {}

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
