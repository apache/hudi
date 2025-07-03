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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.getHoodieCommitMetadata;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.table.action.rollback.RollbackHelper.EMPTY_STRING;

/**
 * Listing based rollback strategy to fetch list of {@link HoodieRollbackRequest}s.
 */
public class ListingBasedRollbackStrategy implements BaseRollbackPlanActionExecutor.RollbackStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ListingBasedRollbackStrategy.class);

  protected final HoodieTable<?, ?, ?, ?> table;

  protected final transient HoodieEngineContext context;

  protected final HoodieWriteConfig config;

  protected final String instantTime;

  protected final Boolean isRestore;

  public ListingBasedRollbackStrategy(HoodieTable<?, ?, ?, ?> table,
                                      HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      String instantTime,
                                      boolean isRestore) {
    this.table = table;
    this.context = context;
    this.config = config;
    this.instantTime = instantTime;
    this.isRestore = isRestore;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      boolean isTableVersionLessThanEight = metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT);
      List<String> partitionPaths =
          FSUtils.getAllPartitionPaths(context, table.getMetaClient(), false);
      int numPartitions = Math.max(Math.min(partitionPaths.size(), config.getRollbackParallelism()), 1);

      context.setJobStatus(this.getClass().getSimpleName(), "Creating Listing Rollback Plan: " + config.getTableName());

      HoodieTableType tableType = table.getMetaClient().getTableType();
      String baseFileExtension = table.getBaseFileExtension();
      Option<HoodieCommitMetadata> commitMetadataOptional = getHoodieCommitMetadata(metaClient, instantToRollback);
      Boolean isCommitMetadataCompleted = checkCommitMetadataCompleted(instantToRollback, commitMetadataOptional);
      AtomicBoolean isCompaction = new AtomicBoolean(false);
      if (commitMetadataOptional.isPresent()) {
        isCompaction.set(commitMetadataOptional.get().getOperationType() == WriteOperationType.COMPACT);
      }
      AtomicBoolean isLogCompaction = new AtomicBoolean(false);
      if (commitMetadataOptional.isPresent()) {
        isLogCompaction.set(commitMetadataOptional.get().getOperationType() == WriteOperationType.LOG_COMPACT);
      }

      return context.flatMap(partitionPaths, partitionPath -> {
        List<HoodieRollbackRequest> hoodieRollbackRequests = new ArrayList<>(partitionPaths.size());

        Supplier<List<StoragePathInfo>> filesToDelete = () -> {
          try {
            return fetchFilesFromInstant(instantToRollback, partitionPath, metaClient.getBasePath().toString(), baseFileExtension,
                metaClient.getStorage(),
                commitMetadataOptional, isCommitMetadataCompleted, tableType);
          } catch (IOException e) {
            throw new HoodieIOException("Fetching files to delete error", e);
          }
        };

        if (HoodieTableType.COPY_ON_WRITE == tableType) {
          hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));
        } else if (HoodieTableType.MERGE_ON_READ == tableType) {
          table.getMetaClient().reloadActiveTimeline();
          String action = instantToRollback.getAction();
          if (isCompaction.get()) { // compaction's action in hoodie instant will be "commit". So, we might need to override.
            action = HoodieTimeline.COMPACTION_ACTION;
          }
          if (isLogCompaction.get()) {
            action = HoodieTimeline.LOG_COMPACTION_ACTION;
          }
          switch (action) {
            case HoodieTimeline.COMMIT_ACTION:
            case HoodieTimeline.REPLACE_COMMIT_ACTION:
            case HoodieTimeline.CLUSTERING_ACTION:
              hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));
              break;
            case HoodieTimeline.COMPACTION_ACTION:
              // Depending on whether we are rolling back compaction as part of restore or a regular rollback, logic differs/
              // as part of regular rollback(on re-attempting a failed compaction), we might have to delete/rollback only the base file that could have
              // potentially been created. Even if there are log files added to the file slice of interest, we should not touch them.
              // but if its part of a restore operation, rolling back a compaction should rollback entire file slice, i.e base file and all log files.
              if (!isRestore) {
                // Rollback of a compaction action if not for restore means that the compaction is scheduled
                // and has not yet finished. In this scenario we should delete only the newly created base files
                // and not corresponding base commit log files created with this as baseCommit since updates would
                // have been written to the log files.
                hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath,
                    listBaseFilesToBeDeleted(instantToRollback.requestedTime(), baseFileExtension, partitionPath, metaClient.getStorage())));
              } else {
                // if this is part of a restore operation, we should rollback/delete entire file slice.
                // For table version 6, the files can be directly fetched from the instant to rollback
                // For table version 8, the files are computed based on completion time. All files completed after
                // the requested time of instant to rollback are included
                hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, isTableVersionLessThanEight ? filesToDelete.get() :
                    listAllFilesSinceCommit(instantToRollback.requestedTime(), baseFileExtension, partitionPath,
                        metaClient)));
              }
              break;
            case HoodieTimeline.DELTA_COMMIT_ACTION:
            case HoodieTimeline.LOG_COMPACTION_ACTION:

              // In case all data was inserts and the commit failed, delete the file belonging to that commit
              // We do not know fileIds for inserts (first inserts are either log files or base files),
              // delete all files for the corresponding failed commit, if present (same as COW)
              hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));
              if (isTableVersionLessThanEight) {

                // --------------------------------------------------------------------------------------------------
                // (A) The following cases are possible if index.canIndexLogFiles and/or index.isGlobal
                // --------------------------------------------------------------------------------------------------
                // (A.1) Failed first commit - Inserts were written to log files and HoodieWriteStat has no entries. In
                // this scenario we would want to delete these log files.
                // (A.2) Failed recurring commit - Inserts/Updates written to log files. In this scenario,
                // HoodieWriteStat will have the baseCommitTime for the first log file written, add rollback blocks.
                // (A.3) Rollback triggered for first commit - Inserts were written to the log files but the commit is
                // being reverted. In this scenario, HoodieWriteStat will be `null` for the attribute prevCommitTime
                // and hence will end up deleting these log files. This is done so there are no orphan log files
                // lying around.
                // (A.4) Rollback triggered for recurring commits - Inserts/Updates are being rolled back, the actions
                // taken in this scenario is a combination of (A.2) and (A.3)
                // ---------------------------------------------------------------------------------------------------
                // (B) The following cases are possible if !index.canIndexLogFiles and/or !index.isGlobal
                // ---------------------------------------------------------------------------------------------------
                // (B.1) Failed first commit - Inserts were written to base files and HoodieWriteStat has no entries.
                // In this scenario, we delete all the base files written for the failed commit.
                // (B.2) Failed recurring commits - Inserts were written to base files and updates to log files. In
                // this scenario, perform (A.1) and for updates written to log files, write rollback blocks.
                // (B.3) Rollback triggered for first commit - Same as (B.1)
                // (B.4) Rollback triggered for recurring commits - Same as (B.2) plus we need to delete the log files
                // as well if the base file gets deleted.
                HoodieCommitMetadata commitMetadata = commitMetadataOptional.get();
                if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
                  hoodieRollbackRequests.addAll(getRollbackRequestToAppendForVersionSix(partitionPath, instantToRollback, commitMetadata, table));
                }
              }
              break;
            default:
              throw new HoodieRollbackException("Unknown listing type, during rollback of " + instantToRollback);
          }
        } else {
          throw new HoodieRollbackException(
              String.format("Unsupported table type: %s, during listing rollback of %s", tableType, instantToRollback));
        }
        return hoodieRollbackRequests.stream();
      }, numPartitions);
    } catch (Exception e) {
      LOG.error("Generating rollback requests failed for " + instantToRollback.requestedTime(), e);
      throw new HoodieRollbackException("Generating rollback requests failed for " + instantToRollback.requestedTime(), e);
    }
  }

  public static List<HoodieRollbackRequest> getRollbackRequestToAppendForVersionSix(String partitionPath, HoodieInstant rollbackInstant,
                                                                                    HoodieCommitMetadata commitMetadata, HoodieTable<?, ?, ?, ?> table) {
    List<HoodieRollbackRequest> hoodieRollbackRequests =  new ArrayList<>();
    checkArgument(table.version().lesserThan(HoodieTableVersion.EIGHT));
    checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the base and not the requested, hence get the
    // baseCommit always by listing the file slice
    // With multi writers, rollbacks could be lazy. and so we need to use getLatestFileSlicesBeforeOrOn() instead of getLatestFileSlices()
    Map<String, FileSlice> latestFileSlices = table.getSliceView()
        .getLatestFileSlicesBeforeOrOn(partitionPath, rollbackInstant.requestedTime(), true)
        .collect(Collectors.toMap(FileSlice::getFileId, Function.identity()));

    List<HoodieWriteStat> hoodieWriteStats = Option.ofNullable(commitMetadata.getPartitionToWriteStats().get(partitionPath)).orElse(Collections.emptyList());
    hoodieWriteStats = hoodieWriteStats.stream()
        .filter(writeStat -> {
          // Filter out stats without prevCommit since they are all inserts
          boolean validForRollback = (writeStat != null) && (!writeStat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT))
              && (writeStat.getPrevCommit() != null) && latestFileSlices.containsKey(writeStat.getFileId());

          if (!validForRollback) {
            return false;
          }

          FileSlice latestFileSlice = latestFileSlices.get(writeStat.getFileId());

          // For sanity, log-file base-instant time can never be less than base-commit on which we are rolling back
          checkArgument(
              compareTimestamps(latestFileSlice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, rollbackInstant.requestedTime()),
              "Log-file base-instant could not be less than the instant being rolled back");

          // Command block "rolling back" the preceding block {@link HoodieCommandBlockTypeEnum#ROLLBACK_PREVIOUS_BLOCK}
          // w/in the latest file-slice is appended iff base-instant of the log-file is _strictly_ less
          // than the instant of the Delta Commit being rolled back. Otherwise, log-file will be cleaned up
          // in a different branch of the flow.
          return compareTimestamps(latestFileSlice.getBaseInstantTime(), LESSER_THAN, rollbackInstant.requestedTime());
        })
        .collect(Collectors.toList());

    for (HoodieWriteStat writeStat : hoodieWriteStats.stream().filter(
        hoodieWriteStat -> !StringUtils.isNullOrEmpty(hoodieWriteStat.getFileId())).collect(Collectors.toList())) {
      FileSlice latestFileSlice = latestFileSlices.get(writeStat.getFileId());
      String fileId = writeStat.getFileId();
      String latestBaseInstant = latestFileSlice.getBaseInstantTime();
      Path fullLogFilePath = HadoopFSUtils.constructAbsolutePathInHadoopPath(table.getConfig().getBasePath(), writeStat.getPath());
      Map<String, Long> logFilesWithBlocksToRollback = Collections.singletonMap(
          fullLogFilePath.toString(), writeStat.getTotalWriteBytes() > 0 ? writeStat.getTotalWriteBytes() : 1L);
      hoodieRollbackRequests.add(new HoodieRollbackRequest(partitionPath, fileId, latestBaseInstant,
          Collections.emptyList(), logFilesWithBlocksToRollback));
    }
    return hoodieRollbackRequests;
  }

  private List<StoragePathInfo> listAllFilesSinceCommit(String commit,
                                                        String baseFileExtension,
                                                        String partitionPath,
                                                        HoodieTableMetaClient metaClient) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    CompletionTimeQueryView completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient);
    StoragePathFilter filter = (path) -> {
      if (path.toString().contains(baseFileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return compareTimestamps(commit, LESSER_THAN_OR_EQUALS,
            fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        String fileCommitTime = FSUtils.getDeltaCommitTimeFromLogPath(path);
        return completionTimeQueryView.isSlicedAfterOrOn(commit, fileCommitTime);
      }
      return false;
    };
    return metaClient.getStorage()
        .listDirectEntries(FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath), filter);
  }

  @NotNull
  private List<HoodieRollbackRequest> getHoodieRollbackRequests(String partitionPath, List<StoragePathInfo> filesToDeletedStatus) {
    return filesToDeletedStatus.stream()
        .map(pathInfo -> {
          String dataFileToBeDeleted = pathInfo.getPath().toString();
          return formatDeletePath(dataFileToBeDeleted);
        })
        .map(s -> new HoodieRollbackRequest(partitionPath, EMPTY_STRING, EMPTY_STRING, Collections.singletonList(s), Collections.emptyMap()))
        .collect(Collectors.toList());
  }

  private static String formatDeletePath(String path) {
    // strip scheme E.g: file:/var/folders
    return path.substring(path.indexOf(":") + 1);
  }

  private List<StoragePathInfo> listBaseFilesToBeDeleted(String commit,
                                                         String basefileExtension,
                                                         String partitionPath,
                                                         HoodieStorage storage) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    StoragePathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    return storage.listDirectEntries(FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath), filter);
  }

  private List<StoragePathInfo> fetchFilesFromInstant(HoodieInstant instantToRollback,
                                                      String partitionPath, String basePath,
                                                      String baseFileExtension, HoodieStorage storage,
                                                      Option<HoodieCommitMetadata> commitMetadataOptional,
                                                      Boolean isCommitMetadataCompleted,
                                                      HoodieTableType tableType) throws IOException {
    // go w/ commit metadata only for COW table. for MOR, we need to get associated log files when commit corresponding to base file is rolledback.
    if (isCommitMetadataCompleted && tableType == HoodieTableType.COPY_ON_WRITE) {
      return fetchFilesFromCommitMetadata(instantToRollback, partitionPath, basePath, commitMetadataOptional.get(),
          baseFileExtension, storage);
    } else {
      return fetchFilesFromListFiles(instantToRollback, partitionPath, basePath, baseFileExtension, storage);
    }
  }

  private List<StoragePathInfo> fetchFilesFromCommitMetadata(HoodieInstant instantToRollback,
                                                             String partitionPath,
                                                             String basePath,
                                                             HoodieCommitMetadata commitMetadata,
                                                             String baseFileExtension,
                                                             HoodieStorage storage) throws IOException {
    StoragePathFilter pathFilter = getPathFilter(baseFileExtension,
        instantToRollback.requestedTime());
    List<StoragePath> filePaths = getFilesFromCommitMetadata(basePath, commitMetadata, partitionPath)
        .filter(entry -> {
          try {
            return storage.exists(entry);
          } catch (Exception e) {
            LOG.error("Exists check failed for " + entry.toString(), e);
          }
          // if any Exception is thrown, do not ignore. let's try to add the file of interest to be deleted. we can't miss any files to be rolled back.
          return true;
        }).collect(Collectors.toList());

    return storage.listDirectEntries(filePaths, pathFilter);
  }

  /**
   * returns matching base files and log files if any for the instant time of the commit to be rolled back.
   * @param instantToRollback
   * @param partitionPath
   * @param basePath
   * @param baseFileExtension
   * @param storage
   * @return
   * @throws IOException
   */
  private List<StoragePathInfo> fetchFilesFromListFiles(HoodieInstant instantToRollback,
                                                        String partitionPath,
                                                        String basePath,
                                                        String baseFileExtension,
                                                        HoodieStorage storage) throws IOException {
    StoragePathFilter pathFilter = getPathFilter(baseFileExtension, instantToRollback.requestedTime());
    List<StoragePath> filePaths = listFilesToBeDeleted(basePath, partitionPath);

    return storage.listDirectEntries(filePaths, pathFilter);
  }

  private Boolean checkCommitMetadataCompleted(HoodieInstant instantToRollback,
                                               Option<HoodieCommitMetadata> commitMetadataOptional) {
    return commitMetadataOptional.isPresent() && instantToRollback.isCompleted()
        && !WriteOperationType.UNKNOWN.equals(commitMetadataOptional.get().getOperationType());
  }

  private static List<StoragePath> listFilesToBeDeleted(String basePath, String partitionPath) {
    return Collections.singletonList(FSUtils.constructAbsolutePath(basePath, partitionPath));
  }

  private static Stream<StoragePath> getFilesFromCommitMetadata(String basePath,
                                                                HoodieCommitMetadata commitMetadata,
                                                                String partitionPath) {
    List<String> fullPaths = commitMetadata.getFullPathsByPartitionPath(basePath, partitionPath);
    return fullPaths.stream().map(StoragePath::new);
  }

  @NotNull
  private static StoragePathFilter getPathFilter(String basefileExtension, String commit) {
    return (path) -> {
      if (path.toString().endsWith(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getDeltaCommitTimeFromLogPath(path);
        return commit.equals(fileCommitTime);
      }
      return false;
    };
  }
}
