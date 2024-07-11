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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.getHoodieCommitMetadata;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;
import static org.apache.hudi.table.action.rollback.BaseRollbackHelper.EMPTY_STRING;

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
      List<String> partitionPaths =
          FSUtils.getAllPartitionPaths(context, table.getStorage(), table.getMetaClient().getBasePath(), false);
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

      return context.flatMap(partitionPaths, partitionPath -> {
        List<HoodieRollbackRequest> hoodieRollbackRequests = new ArrayList<>(partitionPaths.size());

        Supplier<FileStatus[]> filesToDelete = () -> {
          try {
            return fetchFilesFromInstant(instantToRollback, partitionPath, metaClient.getBasePath().toString(), baseFileExtension,
                (FileSystem) metaClient.getStorage().getFileSystem(),
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
                    listBaseFilesToBeDeleted(instantToRollback.getTimestamp(), baseFileExtension, partitionPath,
                        (FileSystem) metaClient.getStorage().getFileSystem())));
              } else {
                // if this is part of a restore operation, we should rollback/delete entire file slice.
                hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath,
                    listAllFilesSinceCommit(instantToRollback.getTimestamp(), baseFileExtension, partitionPath,
                        metaClient)));
              }
              break;
            case HoodieTimeline.DELTA_COMMIT_ACTION:

              // In case all data was inserts and the commit failed, delete the file belonging to that commit
              // We do not know fileIds for inserts (first inserts are either log files or base files),
              // delete all files for the corresponding failed commit, if present (same as COW)
              hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));

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

  private FileStatus[] listAllFilesSinceCommit(
      String commit,
      String baseFileExtension,
      String partitionPath,
      HoodieTableMetaClient metaClient) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    CompletionTimeQueryView completionTimeQueryView = new CompletionTimeQueryView(metaClient);
    PathFilter filter = (path) -> {
      if (path.toString().contains(baseFileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return HoodieTimeline.compareTimestamps(commit, HoodieTimeline.LESSER_THAN_OR_EQUALS,
            fileCommitTime);
      } else if (HadoopFSUtils.isLogFile(path)) {
        String fileCommitTime = FSUtils.getDeltaCommitTimeFromLogPath(convertToStoragePath(path));
        return completionTimeQueryView.isSlicedAfterOrOn(commit, fileCommitTime);
      }
      return false;
    };
    return ((FileSystem) metaClient.getStorage().getFileSystem())
        .listStatus(HadoopFSUtils.constructAbsolutePathInHadoopPath(config.getBasePath(), partitionPath),
            filter);
  }

  @NotNull
  private List<HoodieRollbackRequest> getHoodieRollbackRequests(String partitionPath, FileStatus[] filesToDeletedStatus) {
    return Arrays.stream(filesToDeletedStatus)
        .map(fileStatus -> {
          String dataFileToBeDeleted = fileStatus.getPath().toString();
          return formatDeletePath(dataFileToBeDeleted);
        })
        .map(s -> new HoodieRollbackRequest(partitionPath, EMPTY_STRING, EMPTY_STRING, Collections.singletonList(s), Collections.emptyMap()))
        .collect(Collectors.toList());
  }

  private static String formatDeletePath(String path) {
    // strip scheme E.g: file:/var/folders
    return path.substring(path.indexOf(":") + 1);
  }

  private FileStatus[] listBaseFilesToBeDeleted(String commit, String basefileExtension, String partitionPath,
                                                FileSystem fs) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    PathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    return fs.listStatus(HadoopFSUtils.constructAbsolutePathInHadoopPath(config.getBasePath(), partitionPath), filter);
  }

  private FileStatus[] fetchFilesFromInstant(HoodieInstant instantToRollback, String partitionPath, String basePath,
                                             String baseFileExtension, FileSystem fs,
                                             Option<HoodieCommitMetadata> commitMetadataOptional,
                                             Boolean isCommitMetadataCompleted,
                                             HoodieTableType tableType) throws IOException {
    // go w/ commit metadata only for COW table. for MOR, we need to get associated log files when commit corresponding to base file is rolledback.
    if (isCommitMetadataCompleted && tableType == HoodieTableType.COPY_ON_WRITE) {
      return fetchFilesFromCommitMetadata(instantToRollback, partitionPath, basePath, commitMetadataOptional.get(),
          baseFileExtension, fs);
    } else {
      return fetchFilesFromListFiles(instantToRollback, partitionPath, basePath, baseFileExtension, fs);
    }
  }

  private FileStatus[] fetchFilesFromCommitMetadata(HoodieInstant instantToRollback, String partitionPath,
                                                    String basePath, HoodieCommitMetadata commitMetadata,
                                                    String baseFileExtension, FileSystem fs)
      throws IOException {
    SerializablePathFilter pathFilter = getSerializablePathFilter(baseFileExtension, instantToRollback.getTimestamp());
    Path[] filePaths = getFilesFromCommitMetadata(basePath, commitMetadata, partitionPath);

    return fs.listStatus(Arrays.stream(filePaths).filter(entry -> {
      try {
        return fs.exists(entry);
      } catch (Exception e) {
        LOG.error("Exists check failed for " + entry.toString(), e);
      }
      // if any Exception is thrown, do not ignore. let's try to add the file of interest to be deleted. we can't miss any files to be rolled back.
      return true;
    }).toArray(Path[]::new), pathFilter);
  }

  /**
   * returns matching base files and log files if any for the instant time of the commit to be rolled back.
   * @param instantToRollback
   * @param partitionPath
   * @param basePath
   * @param baseFileExtension
   * @param fs
   * @return
   * @throws IOException
   */
  private FileStatus[] fetchFilesFromListFiles(HoodieInstant instantToRollback, String partitionPath, String basePath,
                                               String baseFileExtension, FileSystem fs)
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
    return new Path[] {HadoopFSUtils.constructAbsolutePathInHadoopPath(basePath, partitionPath)};
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
      } else if (HadoopFSUtils.isLogFile(path)) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getDeltaCommitTimeFromLogPath(convertToStoragePath(path));
        return commit.equals(fileCommitTime);
      }
      return false;
    };
  }
}
