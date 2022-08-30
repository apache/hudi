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

package org.apache.hudi.common.table.cdc;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.cdc.HoodieCDCLogicalFileType.ADD_BASE_FILE;
import static org.apache.hudi.common.table.cdc.HoodieCDCLogicalFileType.CDC_LOG_FILE;
import static org.apache.hudi.common.table.cdc.HoodieCDCLogicalFileType.MOR_LOG_FILE;
import static org.apache.hudi.common.table.cdc.HoodieCDCLogicalFileType.REMOVE_BASE_FILE;
import static org.apache.hudi.common.table.cdc.HoodieCDCLogicalFileType.REPLACED_FILE_GROUP;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.isInRange;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class CDCExtractor {

  private final HoodieTableMetaClient metaClient;

  private final Path basePath;

  private final FileSystem fs;

  private final String supplementalLoggingMode;

  private final String startInstant;

  private final String endInstant;

  // TODO: this will be used when support the cdc query type of 'read_optimized'.
  private final String cdcQueryType;

  private Map<HoodieInstant, HoodieCommitMetadata> commits;

  private HoodieTableFileSystemView fsView;

  public CDCExtractor(
      HoodieTableMetaClient metaClient,
      String startInstant,
      String endInstant,
      String cdcqueryType) {
    this.metaClient = metaClient;
    this.basePath = metaClient.getBasePathV2();
    this.fs = metaClient.getFs().getFileSystem();
    this.supplementalLoggingMode = metaClient.getTableConfig().cdcSupplementalLoggingMode();
    this.startInstant = startInstant;
    this.endInstant = endInstant;
    if (HoodieTableType.MERGE_ON_READ == metaClient.getTableType()
        && cdcqueryType.equals("read_optimized")) {
      throw new HoodieNotSupportedException("The 'read_optimized' cdc query type hasn't been supported for now.");
    }
    this.cdcQueryType = cdcqueryType;
    init();
  }

  private void init() {
    initInstantAndCommitMetadatas();
    initFSView();
  }

  /**
   * At the granularity of a file group, trace the mapping between
   * each commit/instant and changes to this file group.
   */
  public Map<HoodieFileGroupId, List<Pair<HoodieInstant, CDCFileSplit>>> extractor() {
    if (commits == null || fsView == null) {
      throw new HoodieException("Fail to init CDCExtractor");
    }

    Map<HoodieFileGroupId, List<Pair<HoodieInstant, CDCFileSplit>>> fgToCommitChanges = new HashMap<>();
    for (HoodieInstant instant : commits.keySet()) {
      HoodieCommitMetadata commitMetadata = commits.get(instant);

      // parse `partitionToWriteStats` in the metadata of commit
      Map<String, List<HoodieWriteStat>> ptToWriteStats = commitMetadata.getPartitionToWriteStats();
      for (String partition : ptToWriteStats.keySet()) {
        List<HoodieWriteStat> hoodieWriteStats = ptToWriteStats.get(partition);
        hoodieWriteStats.forEach(writeStat -> {
          HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partition, writeStat.getFileId());
          // Identify the CDC source involved in this commit and
          // determine its type for subsequent loading using different methods.
          CDCFileSplit changeFile =
              parseWriteStat(fileGroupId, instant, writeStat, commitMetadata.getOperationType());
          if (!fgToCommitChanges.containsKey(fileGroupId)) {
            fgToCommitChanges.put(fileGroupId, new ArrayList<>());
          }
          fgToCommitChanges.get(fileGroupId).add(Pair.of(instant, changeFile));
        });
      }

      if (commitMetadata instanceof HoodieReplaceCommitMetadata) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> ptToReplacedFileId = replaceCommitMetadata.getPartitionToReplaceFileIds();
        for (String partition : ptToReplacedFileId.keySet()) {
          List<String> fileIds = ptToReplacedFileId.get(partition);
          fileIds.forEach(fileId -> {
            Option<FileSlice> latestFileSliceOpt = fsView.fetchLatestFileSlice(partition, fileId);
            if (latestFileSliceOpt.isPresent()) {
              HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partition, fileId);
              CDCFileSplit changeFile = new CDCFileSplit(
                      REPLACED_FILE_GROUP, null, latestFileSliceOpt, Option.empty());
              if (!fgToCommitChanges.containsKey(fileGroupId)) {
                fgToCommitChanges.put(fileGroupId, new ArrayList<>());
              }
              fgToCommitChanges.get(fileGroupId).add(Pair.of(instant, changeFile));
            }
          });
        }
      }
    }
    return fgToCommitChanges;
  }

  /**
   * Parse the commit metadata between (startInstant, endInstant], and extract the touched partitions
   * and files to build the filesystem view.
   */
  private void initFSView() {
    Set<String> touchedPartitions = new HashSet<>();
    for (Map.Entry<HoodieInstant, HoodieCommitMetadata> entry : commits.entrySet()) {
      HoodieCommitMetadata commitMetadata = entry.getValue();
      touchedPartitions.addAll(commitMetadata.getPartitionToWriteStats().keySet());
      if (commitMetadata instanceof HoodieReplaceCommitMetadata) {
        touchedPartitions.addAll(
            ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds().keySet()
        );
      }
    }
    try {
      List<FileStatus> touchedFiles = new ArrayList<>();
      for (String touchedPartition : touchedPartitions) {
        Path partitionPath = FSUtils.getPartitionPath(basePath, touchedPartition);
        touchedFiles.addAll(Arrays.asList(fs.listStatus(partitionPath)));
      }
      this.fsView = new HoodieTableFileSystemView(
          metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(),
          touchedFiles.toArray(new FileStatus[0])
      );
    } catch (Exception e) {
      throw new HoodieException("Fail to init FileSystem View for CDC", e);
    }
  }


  /**
   * Extract the required instants from all the instants between (startInstant, endInstant].
   *
   * There are some conditions:
   * 1) the instant should be completed;
   * 2) the instant should be in (startInstant, endInstant];
   * 3) the action of the instant is one of 'commit', 'deltacommit', 'replacecommit';
   * 4) the write type of the commit should have the ability to change the data.
   *
   *  And, we need to recognize which is a 'replacecommit', that help to find the list of file group replaced.
   */
  private void initInstantAndCommitMetadatas() {
    try {
      List<String> requiredActions = Arrays.asList(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION);
      HoodieActiveTimeline activeTimeLine = metaClient.getActiveTimeline();
      Map<HoodieInstant, HoodieCommitMetadata> result = activeTimeLine.getInstants()
          .filter(instant ->
              instant.isCompleted()
                  && isInRange(instant.getTimestamp(), startInstant, endInstant)
                  && requiredActions.contains(instant.getAction().toLowerCase(Locale.ROOT))
          ).map(instant -> {
            HoodieCommitMetadata commitMetadata;
            try {
              if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
                commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
                    activeTimeLine.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
              } else {
                commitMetadata = HoodieCommitMetadata.fromBytes(
                    activeTimeLine.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
              }
            } catch (IOException e) {
              throw new HoodieIOException(e.getMessage());
            }
            return Pair.of(instant, commitMetadata);
          }).filter(pair ->
              maybeChangeData(pair.getRight().getOperationType())
          ).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      this.commits = result;
    } catch (Exception e) {
      throw new HoodieIOException("Fail to get the commit metadata for CDC");
    }
  }

  private Boolean maybeChangeData(WriteOperationType operation) {
    return operation == WriteOperationType.INSERT
        || operation == WriteOperationType.UPSERT
        || operation == WriteOperationType.DELETE
        || operation == WriteOperationType.BULK_INSERT
        || operation == WriteOperationType.DELETE_PARTITION
        || operation == WriteOperationType.INSERT_OVERWRITE
        || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
        || operation == WriteOperationType.BOOTSTRAP;
  }

  /**
   * Parse HoodieWriteStat, judge which type the file is, and what strategy should be used to parse CDC data.
   * Then build a [[ChangeFileForSingleFileGroupAndCommit]] object.
   */
  private CDCFileSplit parseWriteStat(
      HoodieFileGroupId fileGroupId,
      HoodieInstant instant,
      HoodieWriteStat writeStat,
      WriteOperationType operation) {
    Path basePath = metaClient.getBasePathV2();
    FileSystem fs = metaClient.getFs().getFileSystem();

    CDCFileSplit cdcFileSplit;
    if (StringUtils.isNullOrEmpty(writeStat.getCdcPath())) {
      // no cdc log files can be used directly. we reuse the existing data file to retrieve the change data.
      String path = writeStat.getPath();
      if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        // this is a base file
        if (operation == WriteOperationType.DELETE && writeStat.getNumWrites() == 0L
            && writeStat.getNumDeletes() != 0) {
          // This is a delete operation wherein all the records in this file group are deleted
          // and no records have been writen out a new file.
          // So, we find the previous file that this operation delete from, and treat each of
          // records as a deleted one.
          HoodieBaseFile beforeBaseFile = fsView.getBaseFileOn(
              fileGroupId.getPartitionPath(), writeStat.getPrevCommit(), fileGroupId.getFileId()
          ).orElseThrow(() ->
              new HoodieIOException("Can not get the previous version of the base file")
          );
          FileSlice beforeFileSlice = new FileSlice(fileGroupId, writeStat.getPrevCommit(), beforeBaseFile, new ArrayList<>());
          cdcFileSplit = new CDCFileSplit(REMOVE_BASE_FILE, null, Option.empty(), Option.of(beforeFileSlice));
        } else if (writeStat.getNumUpdateWrites() == 0L && writeStat.getNumDeletes() == 0
            && writeStat.getNumWrites() == writeStat.getNumInserts()) {
          // all the records in this file are new.
          cdcFileSplit = new CDCFileSplit(ADD_BASE_FILE, path);
        } else {
          throw new HoodieException("There should be a cdc log file.");
        }
      } else {
        // this is a log file
        Option<FileSlice> beforeFileSliceOpt = getDependentFileSliceForLogFile(fileGroupId, instant, path);
        cdcFileSplit = new CDCFileSplit(MOR_LOG_FILE, path, beforeFileSliceOpt, Option.empty());
      }
    } else {
      // this is a cdc log
      if (supplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE_AFTER)) {
        cdcFileSplit = new CDCFileSplit(CDC_LOG_FILE, writeStat.getCdcPath());
      } else {
        try {
          HoodieBaseFile beforeBaseFile = fsView.getBaseFileOn(
              fileGroupId.getPartitionPath(), writeStat.getPrevCommit(), fileGroupId.getFileId()
          ).orElseThrow(() ->
              new HoodieIOException("Can not get the previous version of the base file")
          );
          FileSlice beforeFileSlice = null;
          FileSlice currentFileSlice = new FileSlice(fileGroupId, instant.getTimestamp(),
              new HoodieBaseFile(fs.getFileStatus(new Path(basePath, writeStat.getPath()))), new ArrayList<>());
          if (supplementalLoggingMode.equals(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_OP_KEY)) {
            beforeFileSlice = new FileSlice(fileGroupId, writeStat.getPrevCommit(), beforeBaseFile, new ArrayList<>());
          }
          cdcFileSplit = new CDCFileSplit(CDC_LOG_FILE, writeStat.getCdcPath(),
              Option.ofNullable(beforeFileSlice), Option.ofNullable(currentFileSlice));
        } catch (Exception e) {
          throw new HoodieException("Fail to parse HoodieWriteStat", e);
        }
      }
    }
    return cdcFileSplit;
  }

  /**
   * For a mor log file, get the completed previous file slice from the related commit metadata.
   * This file slice will be used when we extract the change data from this mor log file.
   */
  private Option<FileSlice> getDependentFileSliceForLogFile(
      HoodieFileGroupId fgId,
      HoodieInstant instant,
      String currentLogFile) {
    Path partitionPath = FSUtils.getPartitionPath(basePath, fgId.getPartitionPath());
    if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
      String currentLogFileName = new Path(currentLogFile).getName();
      Option<Pair<String, List<String>>> fileSliceOpt =
          HoodieCommitMetadata.getFileSliceForFileGroupFromDeltaCommit(
              metaClient.getActiveTimeline().getInstantDetails(instant).get(), fgId);
      if (fileSliceOpt.isPresent()) {
        Pair<String, List<String>> fileSlice = fileSliceOpt.get();
        try {
          HoodieBaseFile baseFile = new HoodieBaseFile(
              fs.getFileStatus(new Path(partitionPath, fileSlice.getLeft())));
          Path[] logFilePaths = fileSlice.getRight().stream()
              .filter(logFile -> !logFile.equals(currentLogFileName))
              .map(logFile -> new Path(partitionPath, logFile))
              .toArray(Path[]::new);
          List<HoodieLogFile> logFiles = Arrays.stream(fs.listStatus(logFilePaths))
              .map(HoodieLogFile::new).collect(Collectors.toList());
          return Option.of(new FileSlice(fgId, instant.getTimestamp(), baseFile, logFiles));
        } catch (Exception e) {
          throw new HoodieException("Fail to get the dependent file slice for a log file", e);
        }
      } else {
        return Option.empty();
      }
    }
    return Option.empty();
  }
}
