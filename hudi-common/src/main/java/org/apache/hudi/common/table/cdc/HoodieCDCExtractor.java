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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase.AS_IS;
import static org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase.BASE_FILE_DELETE;
import static org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase.BASE_FILE_INSERT;
import static org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase.LOG_FILE;
import static org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase.REPLACE_COMMIT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * This class helps to extract all the information which will be used when CDC query.
 *
 * There are some steps:
 * 1. filter out the completed commit instants, and get the related {@link HoodieCommitMetadata} objects.
 * 2. initialize the {@link HoodieTableFileSystemView} by the touched data files.
 * 3. extract the cdc information:
 *   generate a {@link HoodieCDCFileSplit} object for each of the instant in (startInstant, endInstant)
 *   and each of the file group which is touched in the range of instants.
 */
public class HoodieCDCExtractor {

  private final HoodieTableMetaClient metaClient;

  private final StoragePath basePath;

  private final HoodieStorage storage;

  private final HoodieCDCSupplementalLoggingMode supplementalLoggingMode;

  private final InstantRange instantRange;

  private Map<HoodieInstant, HoodieCommitMetadata> commits;

  private HoodieTableFileSystemView fsView;

  private final boolean consumeChangesFromCompaction;

  public HoodieCDCExtractor(
      HoodieTableMetaClient metaClient,
      InstantRange range,
      boolean consumeChangesFromCompaction) {
    this.metaClient = metaClient;
    this.basePath = metaClient.getBasePath();
    this.storage = metaClient.getStorage();
    this.supplementalLoggingMode = metaClient.getTableConfig().cdcSupplementalLoggingMode();
    this.instantRange = range;
    this.consumeChangesFromCompaction = consumeChangesFromCompaction;
    init();
  }

  private void init() {
    initInstantAndCommitMetadata();
  }

  /**
   * At the granularity of a file group, trace the mapping between
   * each commit/instant and changes to this file group.
   */
  public Map<HoodieFileGroupId, List<HoodieCDCFileSplit>> extractCDCFileSplits() {
    ValidationUtils.checkState(commits != null, "Empty commits");

    Map<HoodieFileGroupId, List<HoodieCDCFileSplit>> fgToCommitChanges = new HashMap<>();
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
          HoodieCDCFileSplit changeFile =
              parseWriteStat(fileGroupId, instant, writeStat, commitMetadata.getOperationType());
          fgToCommitChanges.computeIfAbsent(fileGroupId, k -> new ArrayList<>());
          fgToCommitChanges.get(fileGroupId).add(changeFile);
        });
      }

      if (commitMetadata instanceof HoodieReplaceCommitMetadata) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> ptToReplacedFileId = replaceCommitMetadata.getPartitionToReplaceFileIds();
        for (String partition : ptToReplacedFileId.keySet()) {
          List<String> fileIds = ptToReplacedFileId.get(partition);
          fileIds.forEach(fileId -> {
            Option<FileSlice> latestFileSliceOpt = getOrCreateFsView().fetchLatestFileSlice(partition, fileId);
            if (latestFileSliceOpt.isPresent()) {
              HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partition, fileId);
              HoodieCDCFileSplit changeFile = new HoodieCDCFileSplit(instant.requestedTime(),
                  REPLACE_COMMIT, new ArrayList<>(), latestFileSliceOpt, Option.empty());
              if (!fgToCommitChanges.containsKey(fileGroupId)) {
                fgToCommitChanges.put(fileGroupId, new ArrayList<>());
              }
              fgToCommitChanges.get(fileGroupId).add(changeFile);
            }
          });
        }
      }
    }
    return fgToCommitChanges;
  }

  /**
   * Returns the fs view directly or creates a new one.
   *
   * <p>There is no need to initialize the fs view when supplemental logging mode is: WITH_BEFORE_AFTER.
   */
  private HoodieTableFileSystemView getOrCreateFsView() {
    if (this.fsView == null) {
      this.fsView = initFSView();
    }
    return this.fsView;
  }

  /**
   * Parse the commit metadata between (startInstant, endInstant], and extract the touched partitions
   * and files to build the filesystem view.
   */
  private HoodieTableFileSystemView initFSView() {
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
      List<StoragePathInfo> touchedFiles = new ArrayList<>();
      for (String touchedPartition : touchedPartitions) {
        StoragePath partitionPath = FSUtils.constructAbsolutePath(basePath, touchedPartition);
        touchedFiles.addAll(storage.listDirectEntries(partitionPath));
      }
      return new HoodieTableFileSystemView(
          metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(),
          touchedFiles
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
  private void initInstantAndCommitMetadata() {
    try {
      Set<String> requiredActions = new HashSet<>(Arrays.asList(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION));
      HoodieActiveTimeline activeTimeLine = metaClient.getActiveTimeline();
      if (instantRange.getStartInstant().isPresent() && !metaClient.getArchivedTimeline().empty()
          && InstantComparison.compareTimestamps(metaClient.getArchivedTimeline().lastInstant().get().requestedTime(), InstantComparison.GREATER_THAN, instantRange.getStartInstant().get())) {
        throw new HoodieException("Start instant time " + instantRange.getStartInstant().get()
            + " for CDC query has to be in the active timeline. Beginning of active timeline " + activeTimeLine.firstInstant().get().requestedTime());
      }
      this.commits = activeTimeLine.getInstantsAsStream()
          .filter(instant ->
              instant.isCompleted()
                  && instantRange.isInRange(instant.requestedTime())
                  && requiredActions.contains(instant.getAction().toLowerCase(Locale.ROOT))
          ).map(instant -> {
            final HoodieCommitMetadata commitMetadata;
            try {
              commitMetadata = TimelineUtils.getCommitMetadata(instant, activeTimeLine);
            } catch (IOException e) {
              throw new HoodieIOException(e.getMessage());
            }
            return Pair.of(instant, commitMetadata);
          }).filter(pair ->
              WriteOperationType.yieldChanges(pair.getRight().getOperationType())
                  || (this.consumeChangesFromCompaction && pair.getRight().getOperationType() == WriteOperationType.COMPACT)
          ).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    } catch (Exception e) {
      throw new HoodieIOException("Fail to get the commit metadata for CDC");
    }
  }

  /**
   * Parse HoodieWriteStat, judge which type the file is, and what strategy should be used to parse CDC data.
   * Then build a [[HoodieCDCFileSplit]] object.
   */
  private HoodieCDCFileSplit parseWriteStat(
      HoodieFileGroupId fileGroupId,
      HoodieInstant instant,
      HoodieWriteStat writeStat,
      WriteOperationType operation) {
    final StoragePath basePath = metaClient.getBasePath();
    final HoodieStorage storage = metaClient.getStorage();
    final String instantTs = instant.requestedTime();

    HoodieCDCFileSplit cdcFileSplit;
    if (CollectionUtils.isNullOrEmpty(writeStat.getCdcStats())) {
      // no cdc log files can be used directly. we reuse the existing data file to retrieve the change data.
      String path = writeStat.getPath();
      if (FSUtils.isBaseFile(new StoragePath(path))) {
        // this is a base file
        if (WriteOperationType.isDelete(operation) && writeStat.getNumWrites() == 0L
            && writeStat.getNumDeletes() != 0) {
          // This is a delete operation wherein all the records in this file group are deleted
          // and no records have been written out a new file.
          // So, we find the previous file that this operation delete from, and treat each of
          // records as a deleted one.
          HoodieBaseFile beforeBaseFile = getOrCreateFsView().getBaseFileOn(
              fileGroupId.getPartitionPath(), writeStat.getPrevCommit(), fileGroupId.getFileId()
          ).orElseThrow(() ->
              new HoodieIOException("Can not get the previous version of the base file")
          );
          FileSlice beforeFileSlice = new FileSlice(fileGroupId, writeStat.getPrevCommit(), beforeBaseFile, Collections.emptyList());
          cdcFileSplit = new HoodieCDCFileSplit(instantTs, BASE_FILE_DELETE, new ArrayList<>(), Option.of(beforeFileSlice), Option.empty());
        } else if ((writeStat.getNumUpdateWrites() == 0L && writeStat.getNumWrites() == writeStat.getNumInserts())) {
          // all the records in this file are new.
          cdcFileSplit = new HoodieCDCFileSplit(instantTs, BASE_FILE_INSERT, path);
        } else {
          throw new HoodieException("There should be a cdc log file.");
        }
      } else {
        // this is a log file
        Option<FileSlice> beforeFileSliceOpt = getDependentFileSliceForLogFile(fileGroupId, instant, path);
        cdcFileSplit = new HoodieCDCFileSplit(instantTs, LOG_FILE, path, beforeFileSliceOpt, Option.empty());
      }
    } else {
      // this is a cdc log
      if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER) {
        cdcFileSplit = new HoodieCDCFileSplit(instantTs, AS_IS, writeStat.getCdcStats().keySet());
      } else {
        try {
          HoodieBaseFile beforeBaseFile = getOrCreateFsView().getBaseFileOn(
              fileGroupId.getPartitionPath(), writeStat.getPrevCommit(), fileGroupId.getFileId()
          ).orElseThrow(() ->
              new HoodieIOException("Can not get the previous version of the base file")
          );
          FileSlice beforeFileSlice = null;
          FileSlice currentFileSlice = new FileSlice(fileGroupId, instant.requestedTime(),
              new HoodieBaseFile(
                  storage.getPathInfo(new StoragePath(basePath, writeStat.getPath()))),
              new ArrayList<>());
          if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY) {
            beforeFileSlice = new FileSlice(fileGroupId, writeStat.getPrevCommit(), beforeBaseFile, new ArrayList<>());
          }
          cdcFileSplit = new HoodieCDCFileSplit(instantTs, AS_IS, writeStat.getCdcStats().keySet(),
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
    StoragePath partitionPath = FSUtils.constructAbsolutePath(basePath, fgId.getPartitionPath());
    if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
      String currentLogFileName = new StoragePath(currentLogFile).getName();
      Option<Pair<String, List<String>>> fileSliceOpt =
          HoodieCommitMetadata.getFileSliceForFileGroupFromDeltaCommit(
              metaClient.getActiveTimeline().getInstantContentStream(instant), fgId);
      if (fileSliceOpt.isPresent()) {
        Pair<String, List<String>> fileSlice = fileSliceOpt.get();
        try {
          HoodieBaseFile baseFile = fileSlice.getLeft().isEmpty()
              ? null
              : new HoodieBaseFile(storage.getPathInfo(new StoragePath(partitionPath, fileSlice.getLeft())));
          List<StoragePath> logFilePaths = fileSlice.getRight().stream()
              .filter(logFile -> !logFile.equals(currentLogFileName))
              .map(logFile -> new StoragePath(partitionPath, logFile))
              .collect(Collectors.toList());
          // get files list from unfinished compaction commit
          List<StoragePath> filesToCompact = new ArrayList<>();
          AtomicReference<String> lastBaseFile =  new AtomicReference<>();
          metaClient.getActiveTimeline().getInstants().stream().filter(
                  i -> i.compareTo(instant) < 0 && !i.isCompleted() && i.getAction()
                      .equals(HoodieActiveTimeline.COMPACTION_ACTION))
              .forEach(i -> {
                try {
                  metaClient.getActiveTimeline().readCompactionPlan(i).getOperations()
                      .stream()
                      .filter(op -> op.getPartitionPath().equals(fgId.getPartitionPath()) && op.getFileId().equals(fgId.getFileId()))
                      .forEach(operation ->  {
                        filesToCompact.addAll(operation.getDeltaFilePaths().stream().map(logFile -> new StoragePath(partitionPath, logFile)).collect(Collectors.toList()));
                        lastBaseFile.set(operation.getDataFilePath());
                      });
                } catch (IOException e) {
                  throw new HoodieIOException("Failed to read a compaction plan on instant " + i, e);
                }
              });
          if (baseFile == null && lastBaseFile.get() != null) {
            baseFile = new HoodieBaseFile(storage.getPathInfo(new StoragePath(partitionPath, lastBaseFile.get())));
          }
          logFilePaths.addAll(filesToCompact);
          List<HoodieLogFile> logFiles = storage.listDirectEntries(logFilePaths).stream()
              .map(HoodieLogFile::new).collect(Collectors.toList());
          return Option.of(new FileSlice(fgId, instant.requestedTime(), baseFile, logFiles));
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
