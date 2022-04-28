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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import static org.apache.hudi.client.utils.MetadataConversionUtils.getHoodieCommitMetadata;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.table.action.rollback.BaseRollbackHelper.EMPTY_STRING;

/**
 * Listing based rollback strategy to fetch list of {@link HoodieRollbackRequest}s.
 */
public class ListingBasedRollbackStrategy implements BaseRollbackPlanActionExecutor.RollbackStrategy {

  private static final Logger LOG = LogManager.getLogger(ListingBasedRollbackStrategy.class);

  protected final HoodieTable<?, ?, ?, ?> table;

  protected final transient HoodieEngineContext context;

  protected final HoodieWriteConfig config;

  protected final String instantTime;

  public ListingBasedRollbackStrategy(HoodieTable<?, ?, ?, ?> table,
                                      HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      String instantTime) {
    this.table = table;
    this.context = context;
    this.config = config;
    this.instantTime = instantTime;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      List<String> partitionPaths =
          FSUtils.getAllPartitionPaths(context, table.getMetaClient().getBasePath(), false, false);
      int numPartitions = Math.max(Math.min(partitionPaths.size(), config.getRollbackParallelism()), 1);

      context.setJobStatus(this.getClass().getSimpleName(), "Creating Listing Rollback Plan");

      HoodieTableType tableType = table.getMetaClient().getTableType();
      String baseFileExtension = getBaseFileExtension(metaClient);
      Option<HoodieCommitMetadata> commitMetadataOptional = getHoodieCommitMetadata(metaClient, instantToRollback);
      Boolean isCommitMetadataCompleted = checkCommitMetadataCompleted(instantToRollback, commitMetadataOptional);

      return context.flatMap(partitionPaths, partitionPath -> {
        List<HoodieRollbackRequest> hoodieRollbackRequests = new ArrayList<>(partitionPaths.size());
        FileStatus[] filesToDelete =
            fetchFilesFromInstant(instantToRollback, partitionPath, metaClient.getBasePath(), baseFileExtension,
                metaClient.getFs(), commitMetadataOptional, isCommitMetadataCompleted);

        if (HoodieTableType.COPY_ON_WRITE == tableType) {
          hoodieRollbackRequests.add(getHoodieRollbackRequest(partitionPath, filesToDelete));
        } else if (HoodieTableType.MERGE_ON_READ == tableType) {
          String commit = instantToRollback.getTimestamp();
          HoodieActiveTimeline activeTimeline = table.getMetaClient().reloadActiveTimeline();
          switch (instantToRollback.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
            case HoodieTimeline.REPLACE_COMMIT_ACTION:
              hoodieRollbackRequests.add(getHoodieRollbackRequest(partitionPath, filesToDelete));
              break;
            case HoodieTimeline.COMPACTION_ACTION:
              // If there is no delta commit present after the current commit (if compaction), no action, else we
              // need to make sure that a compaction commit rollback also deletes any log files written as part of the
              // succeeding deltacommit.
              boolean higherDeltaCommits =
                  !activeTimeline.getDeltaCommitTimeline().filterCompletedInstants().findInstantsAfter(commit, 1)
                      .empty();
              if (higherDeltaCommits) {
                // Rollback of a compaction action with no higher deltacommit means that the compaction is scheduled
                // and has not yet finished. In this scenario we should delete only the newly created base files
                // and not corresponding base commit log files created with this as baseCommit since updates would
                // have been written to the log files.
                hoodieRollbackRequests.add(getHoodieRollbackRequest(partitionPath,
                    listFilesToBeDeleted(instantToRollback.getTimestamp(), baseFileExtension, partitionPath,
                        metaClient.getFs())));
              } else {
                // No deltacommits present after this compaction commit (inflight or requested). In this case, we
                // can also delete any log files that were created with this compaction commit as base
                // commit.
                hoodieRollbackRequests.add(getHoodieRollbackRequest(partitionPath, filesToDelete));
              }
              break;
            case HoodieTimeline.DELTA_COMMIT_ACTION:
              // --------------------------------------------------------------------------------------------------
              // (A) The following cases are possible if index.canIndexLogFiles and/or index.isGlobal
              // --------------------------------------------------------------------------------------------------
              // (A.1) Failed first commit - Inserts were written to log files and HoodieWriteStat has no entries. In
              // this scenario we would want to delete these log files.
              // (A.2) Failed recurring commit - Inserts/Updates written to log files. In this scenario,
              // HoodieWriteStat will have the baseCommitTime for the first log file written, add rollback blocks.
              // (A.3) Rollback triggered for first commit - Inserts were written to the log files but the commit is
              // being reverted. In this scenario, HoodieWriteStat will be `null` for the attribute prevCommitTime and
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
              // as well if the base base file gets deleted.
              HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                  table.getMetaClient().getCommitTimeline().getInstantDetails(instantToRollback).get(),
                  HoodieCommitMetadata.class);

              // In case all data was inserts and the commit failed, delete the file belonging to that commit
              // We do not know fileIds for inserts (first inserts are either log files or base files),
              // delete all files for the corresponding failed commit, if present (same as COW)
              hoodieRollbackRequests.add(getHoodieRollbackRequest(partitionPath, filesToDelete));

              // append rollback blocks for updates and inserts as A.2 and B.2
              if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
                hoodieRollbackRequests.addAll(
                    getRollbackRequestToAppend(partitionPath, instantToRollback, commitMetadata, table));
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
      LOG.error("Generating rollback requests failed for " + instantToRollback.getTimestamp(), e);
      throw new HoodieRollbackException("Generating rollback requests failed for " + instantToRollback.getTimestamp(), e);
    }
  }

  private String getBaseFileExtension(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
  }

  @NotNull
  private HoodieRollbackRequest getHoodieRollbackRequest(String partitionPath, FileStatus[] filesToDeletedStatus) {
    List<String> filesToDelete = getFilesToBeDeleted(filesToDeletedStatus);
    return new HoodieRollbackRequest(
        partitionPath, EMPTY_STRING, EMPTY_STRING, filesToDelete, Collections.emptyMap());
  }

  @NotNull
  private List<String> getFilesToBeDeleted(FileStatus[] dataFilesToDeletedStatus) {
    return Arrays.stream(dataFilesToDeletedStatus).map(fileStatus -> {
      String dataFileToBeDeleted = fileStatus.getPath().toString();
      // strip scheme E.g: file:/var/folders
      return dataFileToBeDeleted.substring(dataFileToBeDeleted.indexOf(":") + 1);
    }).collect(Collectors.toList());
  }

  private FileStatus[] listFilesToBeDeleted(String commit, String basefileExtension, String partitionPath,
                                            FileSystem fs) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    PathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    return fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
  }

  private FileStatus[] fetchFilesFromInstant(HoodieInstant instantToRollback, String partitionPath, String basePath,
                                             String baseFileExtension, HoodieWrapperFileSystem fs,
                                             Option<HoodieCommitMetadata> commitMetadataOptional,
                                             Boolean isCommitMetadataCompleted) throws IOException {
    if (isCommitMetadataCompleted) {
      return fetchFilesFromCommitMetadata(instantToRollback, partitionPath, basePath, commitMetadataOptional.get(),
          baseFileExtension, fs);
    } else {
      return fetchFilesFromListFiles(instantToRollback, partitionPath, basePath, baseFileExtension, fs);
    }
  }

  private FileStatus[] fetchFilesFromCommitMetadata(HoodieInstant instantToRollback, String partitionPath,
                                                    String basePath, HoodieCommitMetadata commitMetadata,
                                                    String baseFileExtension, HoodieWrapperFileSystem fs)
      throws IOException {
    SerializablePathFilter pathFilter = getSerializablePathFilter(baseFileExtension, instantToRollback.getTimestamp());
    Path[] filePaths = getFilesFromCommitMetadata(basePath, commitMetadata, partitionPath);

    return fs.listStatus(Arrays.stream(filePaths).filter(entry -> {
      try {
        return fs.exists(entry);
      } catch (IOException e) {
        LOG.error("Exists check failed for " + entry.toString(), e);
      }
      // if IOException is thrown, do not ignore. lets try to add the file of interest to be deleted. we can't miss any files to be rolled back.
      return false;
    }).toArray(Path[]::new), pathFilter);
  }

  private FileStatus[] fetchFilesFromListFiles(HoodieInstant instantToRollback, String partitionPath, String basePath,
                                               String baseFileExtension, HoodieWrapperFileSystem fs)
      throws IOException {
    SerializablePathFilter pathFilter = getSerializablePathFilter(baseFileExtension, instantToRollback.getTimestamp());
    Path[] filePaths = listFilesToBeDeleted(basePath, partitionPath);

    return fs.listStatus(filePaths, pathFilter);
  }

  private Boolean checkCommitMetadataCompleted(HoodieInstant instantToRollback,
                                               Option<HoodieCommitMetadata> commitMetadataOptional) {
    return commitMetadataOptional.isPresent() && instantToRollback.isCompleted()
        && !WriteOperationType.UNKNOWN.equals(commitMetadataOptional.get().getOperationType());
  }

  private static Path[] listFilesToBeDeleted(String basePath, String partitionPath) {
    return new Path[] {FSUtils.getPartitionPath(basePath, partitionPath)};
  }

  private static Path[] getFilesFromCommitMetadata(String basePath, HoodieCommitMetadata commitMetadata, String partitionPath) {
    List<String> fullPaths = commitMetadata.getFullPathsByPartitionPath(basePath, partitionPath);
    return fullPaths.stream().map(Path::new).toArray(Path[]::new);
  }

  @NotNull
  private static SerializablePathFilter getSerializablePathFilter(String basefileExtension, String commit) {
    return (path) -> {
      if (path.toString().endsWith(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
        return commit.equals(fileCommitTime);
      }
      return false;
    };
  }

  public static List<HoodieRollbackRequest> getRollbackRequestToAppend(String partitionPath, HoodieInstant rollbackInstant,
                                                                       HoodieCommitMetadata commitMetadata, HoodieTable<?, ?, ?, ?> table) {
    List<HoodieRollbackRequest> hoodieRollbackRequests =  new ArrayList<>();
    checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the base and not the requested, hence get the
    // baseCommit always by listing the file slice
    // With multi writers, rollbacks could be lazy. and so we need to use getLatestFileSlicesBeforeOrOn() instead of getLatestFileSlices()
    Map<String, FileSlice> latestFileSlices = table.getSliceView()
        .getLatestFileSlicesBeforeOrOn(partitionPath, rollbackInstant.getTimestamp(), true)
        .collect(Collectors.toMap(FileSlice::getFileId, Function.identity()));

    List<HoodieWriteStat> hoodieWriteStats = commitMetadata.getPartitionToWriteStats().get(partitionPath)
        .stream()
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
              HoodieTimeline.compareTimestamps(latestFileSlice.getBaseInstantTime(),
                  HoodieTimeline.LESSER_THAN_OR_EQUALS, rollbackInstant.getTimestamp()),
              "Log-file base-instant could not be less than the instant being rolled back");

          // Command block "rolling back" the preceding block {@link HoodieCommandBlockTypeEnum#ROLLBACK_PREVIOUS_BLOCK}
          // w/in the latest file-slice is appended iff base-instant of the log-file is _strictly_ less
          // than the instant of the Delta Commit being rolled back. Otherwise, log-file will be cleaned up
          // in a different branch of the flow.
          return HoodieTimeline.compareTimestamps(latestFileSlice.getBaseInstantTime(), HoodieTimeline.LESSER_THAN, rollbackInstant.getTimestamp());
        })
        .collect(Collectors.toList());

    for (HoodieWriteStat writeStat : hoodieWriteStats) {
      FileSlice latestFileSlice = latestFileSlices.get(writeStat.getFileId());
      String fileId = writeStat.getFileId();
      String latestBaseInstant = latestFileSlice.getBaseInstantTime();

      Path fullLogFilePath = FSUtils.getPartitionPath(table.getConfig().getBasePath(), writeStat.getPath());

      Map<String, Long> logFilesWithBlocksToRollback =
          Collections.singletonMap(fullLogFilePath.toString(), writeStat.getTotalWriteBytes());

      hoodieRollbackRequests.add(new HoodieRollbackRequest(partitionPath, fileId, latestBaseInstant,
          Collections.emptyList(), logFilesWithBlocksToRollback));
    }

    return hoodieRollbackRequests;
  }
}
