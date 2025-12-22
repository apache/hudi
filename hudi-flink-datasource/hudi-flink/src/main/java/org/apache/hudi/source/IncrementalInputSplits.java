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

package org.apache.hudi.source;

import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.format.cdc.CdcInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;

/**
 * Utilities to generate incremental input splits {@link MergeOnReadInputSplit}.
 * The input splits are used for streaming and incremental read.
 *
 * <p>How to generate the input splits:
 * <ol>
 *   <li>first fetch all the commit metadata for the incremental instants;</li>
 *   <li>resolve the incremental commit file paths;</li>
 *   <li>filter the full file paths by required partitions;</li>
 *   <li>use the file paths from #step 3 as the back-up of the filesystem view.</li>
 * </ol>
 */
public class IncrementalInputSplits implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalInputSplits.class);
  private final Configuration conf;
  private final Path path;
  private final RowType rowType;
  private final long maxCompactionMemoryInBytes;
  // for partition pruning
  private final PartitionPruners.PartitionPruner partitionPruner;
  // skip compaction
  private final boolean skipCompaction;
  // skip clustering
  private final boolean skipClustering;

  private IncrementalInputSplits(
      Configuration conf,
      Path path,
      RowType rowType,
      long maxCompactionMemoryInBytes,
      @Nullable PartitionPruners.PartitionPruner partitionPruner,
      boolean skipCompaction,
      boolean skipClustering) {
    this.conf = conf;
    this.path = path;
    this.rowType = rowType;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.partitionPruner = partitionPruner;
    this.skipCompaction = skipCompaction;
    this.skipClustering = skipClustering;
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the incremental input splits.
   *
   * @param metaClient The meta client
   * @param cdcEnabled Whether cdc is enabled
   *
   * @return The list of incremental input splits or empty if there are no new instants
   */
  public Result inputSplits(
      HoodieTableMetaClient metaClient,
      boolean cdcEnabled) {
    HoodieTimeline commitTimeline = getReadTimeline(metaClient);
    if (commitTimeline.empty()) {
      LOG.warn("No splits found for the table under path " + path);
      return Result.EMPTY;
    }

    final String startCommit = this.conf.getString(FlinkOptions.READ_START_COMMIT);
    final String endCommit = this.conf.getString(FlinkOptions.READ_END_COMMIT);
    final boolean startFromEarliest = FlinkOptions.START_COMMIT_EARLIEST.equalsIgnoreCase(startCommit);
    final boolean startOutOfRange = startCommit != null && commitTimeline.isBeforeTimelineStarts(startCommit);
    final boolean endOutOfRange = endCommit != null && commitTimeline.isBeforeTimelineStarts(endCommit);
    // We better add another premise: whether the endCommit is cleaned.
    boolean fullTableScan = startFromEarliest || startOutOfRange || endOutOfRange;

    List<HoodieInstant> instants = filterInstantsWithRange(commitTimeline, null);

    // Step1: generates the instant range
    // if the specified end commit is archived, still uses the specified timestamp,
    // else uses the latest filtered instant time
    // (would be the latest instant time if the specified end commit is greater than the latest instant time)
    final String rangeEnd = endOutOfRange || instants.isEmpty() ? endCommit : instants.get(instants.size() - 1).getTimestamp();
    // keep the same semantics with streaming read, default start from the latest commit
    final String rangeStart = startFromEarliest ? null : (startCommit == null ? rangeEnd : startCommit);
    final InstantRange instantRange;
    if (!fullTableScan) {
      instantRange = InstantRange.builder().startInstant(rangeStart).endInstant(rangeEnd)
          .rangeType(InstantRange.RangeType.CLOSE_CLOSE).nullableBoundary(cdcEnabled).build();
    } else if (startFromEarliest && endCommit == null) {
      // short-cut for snapshot read
      instantRange = null;
    } else {
      instantRange = InstantRange.builder().startInstant(rangeStart).endInstant(rangeEnd)
          .rangeType(InstantRange.RangeType.CLOSE_CLOSE).nullableBoundary(true).build();
    }
    // Step2: decides the read end commit
    final String endInstant = endOutOfRange || endCommit == null
        ? commitTimeline.lastInstant().get().getTimestamp()
        : rangeEnd;

    // Step3: find out the files to read, tries to read the files from the commit metadata first,
    // fallback to full table scan if any of the following conditions matches:
    //   1. there are files in metadata be deleted;
    //   2. read from earliest
    //   3. the start commit is archived
    //   4. the end commit is archived
    Set<String> readPartitions;
    final List<StoragePathInfo> fileInfoList;
    if (fullTableScan) {
      // scans the partitions and files directly.
      FileIndex fileIndex = getFileIndex();
      readPartitions = new TreeSet<>(fileIndex.getOrBuildPartitionPaths());
      if (readPartitions.size() == 0) {
        LOG.warn("No partitions found for reading in user provided path.");
        return Result.EMPTY;
      }
      fileInfoList = fileIndex.getFilesInPartitions();
    } else {
      if (instants.size() == 0) {
        LOG.info("No new instant found for the table under path " + path + ", skip reading");
        return Result.EMPTY;
      }
      if (cdcEnabled) {
        // case1: cdc change log enabled
        List<MergeOnReadInputSplit> inputSplits = getCdcInputSplits(metaClient, instantRange);
        return Result.instance(inputSplits, endInstant);
      }
      // case2: normal incremental read
      String tableName = conf.getString(FlinkOptions.TABLE_NAME);
      List<HoodieCommitMetadata> metadataList = instants.stream()
          .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, commitTimeline))
          .collect(Collectors.toList());
      readPartitions = getReadPartitions(metadataList);
      if (readPartitions.size() == 0) {
        LOG.warn("No partitions found for reading in user provided path.");
        return Result.EMPTY;
      }
      List<StoragePathInfo> files = WriteProfiles.getFilesFromMetadata(
          path, (org.apache.hadoop.conf.Configuration) metaClient.getStorageConf().unwrap(),
          metadataList, metaClient.getTableType(), false);
      if (files == null) {
        LOG.warn("Found deleted files in metadata, fall back to full table scan.");
        // fallback to full table scan
        // reading from the earliest, scans the partitions and files directly.
        FileIndex fileIndex = getFileIndex();
        readPartitions = new TreeSet<>(fileIndex.getOrBuildPartitionPaths());
        if (readPartitions.size() == 0) {
          LOG.warn("No partitions found for reading in user provided path.");
          return Result.EMPTY;
        }
        fileInfoList = fileIndex.getFilesInPartitions();
      } else {
        fileInfoList = files;
      }
    }

    if (fileInfoList.size() == 0) {
      LOG.warn("No files found for reading in user provided path.");
      return Result.EMPTY;
    }

    List<MergeOnReadInputSplit> inputSplits = getInputSplits(metaClient, commitTimeline,
        fileInfoList, readPartitions, endInstant, instantRange, false);

    return Result.instance(inputSplits, endInstant);
  }

  /**
   * Returns the incremental input splits.
   *
   * @param metaClient    The meta client
   * @param issuedInstant The last issued instant, only valid in streaming read
   * @param issuedOffset  The last issued offset, only valid in streaming read
   * @param cdcEnabled    Whether cdc is enabled
   *
   * @return The list of incremental input splits or empty if there are no new instants
   */
  public Result inputSplits(
      HoodieTableMetaClient metaClient,
      @Nullable String issuedInstant,
      @Nullable String issuedOffset,
      boolean cdcEnabled) {
    metaClient.reloadActiveTimeline();
    HoodieTimeline commitTimeline = getReadTimeline(metaClient);
    if (commitTimeline.empty()) {
      LOG.warn("No splits found for the table under path " + path);
      return Result.EMPTY;
    }

    // Assumes a timeline:
    //    c1.inflight, c2(issued instant), c3, c4
    // -> c1,          c2(issued instant), c3, c4
    // c1, c3 and c4 are the candidate instants,
    // we call c1 a 'hollow' instant which has lower version number but greater completion time,
    // filtering the timeline using just c2 could cause data loss,
    // check these hollow instants first.
    Result hollowSplits = getHollowInputSplits(metaClient,
        metaClient.getStorageConf().unwrapAs(org.apache.hadoop.conf.Configuration.class), issuedInstant, issuedOffset, commitTimeline, cdcEnabled);

    List<HoodieInstant> instants = filterInstantsWithRange(commitTimeline, issuedInstant);
    // get the latest instant that satisfies condition
    final String endInstant = instants.size() == 0 ? null : instants.get(instants.size() - 1).getTimestamp();
    final InstantRange instantRange;
    if (endInstant != null) {
      // when cdc is enabled, returns instant range with nullable boundary
      // to filter the reading instants on the timeline
      instantRange = getInstantRange(issuedInstant, endInstant, cdcEnabled);
    } else if (hollowSplits.isEmpty()) {
      LOG.info("No new instant found for the table under path " + path + ", skip reading");
      return Result.EMPTY;
    } else {
      return hollowSplits;
    }

    // version number should be monotonically increasing
    // fetch the instant offset by completion time
    String offsetToIssue = instants.stream().map(HoodieInstant::getStateTransitionTime).max(String::compareTo).orElse(endInstant);

    if (instantRange == null) {
      // reading from the earliest, scans the partitions and files directly.
      FileIndex fileIndex = getFileIndex();

      Set<String> readPartitions = new TreeSet<>(fileIndex.getOrBuildPartitionPaths());
      if (readPartitions.size() == 0) {
        LOG.warn("No partitions found for reading under path: " + path);
        return Result.EMPTY;
      }

      List<StoragePathInfo> pathInfoList = fileIndex.getFilesInPartitions();
      if (pathInfoList.size() == 0) {
        LOG.warn("No files found for reading under path: " + path);
        return Result.EMPTY;
      }

      List<MergeOnReadInputSplit> inputSplits = getInputSplits(metaClient, commitTimeline,
          pathInfoList, readPartitions, endInstant, null, false);

      return Result.instance(inputSplits, endInstant, offsetToIssue);
    } else {
      List<MergeOnReadInputSplit> inputSplits = getIncInputSplits(metaClient,
          metaClient.getStorageConf().unwrapAs(org.apache.hadoop.conf.Configuration.class),
          commitTimeline, instants, instantRange, endInstant, cdcEnabled);
      return Result.instance(mergeList(hollowSplits.getInputSplits(), inputSplits), endInstant, offsetToIssue);
    }
  }

  /**
   * Returns the input splits for streaming incremental read.
   */
  private List<MergeOnReadInputSplit> getIncInputSplits(
      HoodieTableMetaClient metaClient,
      org.apache.hadoop.conf.Configuration hadoopConf,
      HoodieTimeline commitTimeline,
      List<HoodieInstant> instants,
      InstantRange instantRange,
      String endInstant,
      boolean cdcEnabled) {
    // streaming read
    if (cdcEnabled) {
      // case1: cdc change log enabled
      return getCdcInputSplits(metaClient, instantRange);
    }
    // case2: normal streaming read
    String tableName = conf.getString(FlinkOptions.TABLE_NAME);
    List<HoodieCommitMetadata> activeMetadataList = instants.stream()
        .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, commitTimeline)).collect(Collectors.toList());
    List<HoodieCommitMetadata> archivedMetadataList = getArchivedMetadata(metaClient, instantRange, commitTimeline, tableName);
    if (archivedMetadataList.size() > 0) {
      LOG.warn("\n"
          + "--------------------------------------------------------------------------------\n"
          + "---------- caution: the reader has fall behind too much from the writer,\n"
          + "---------- tweak 'read.tasks' option to add parallelism of read tasks.\n"
          + "--------------------------------------------------------------------------------");
    }
    // IMPORTANT: the merged metadata list must be in ascending order by instant time
    List<HoodieCommitMetadata> metadataList = mergeList(archivedMetadataList, activeMetadataList);

    Set<String> readPartitions = getReadPartitions(metadataList);
    if (readPartitions.size() == 0) {
      LOG.warn("No partitions found for reading under path: " + path);
      return Collections.emptyList();
    }
    List<StoragePathInfo> pathInfoList = WriteProfiles.getFilesFromMetadata(
        path, hadoopConf, metadataList, metaClient.getTableType());

    if (pathInfoList.size() == 0) {
      LOG.warn("No files found for reading under path: " + path);
      return Collections.emptyList();
    }

    return getInputSplits(metaClient, commitTimeline,
        pathInfoList, readPartitions, endInstant, instantRange, skipCompaction);
  }

  /**
   * Returns the input splits for 'hollow' instants.
   */
  private Result getHollowInputSplits(
      HoodieTableMetaClient metaClient,
      org.apache.hadoop.conf.Configuration hadoopConf,
      @Nullable String issuedInstant,
      @Nullable String issuedOffset,
      HoodieTimeline commitTimeline,
      boolean cdcEnabled) {
    if (issuedInstant == null || issuedOffset == null) {
      return Result.EMPTY;
    }
    // find the write commit instant that finishes later than the issued instant
    // while with smaller txn start time.
    List<HoodieInstant> instants = commitTimeline.getInstantsAsStream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN, issuedInstant))
        .filter(s -> HoodieTimeline.compareTimestamps(s.getStateTransitionTime(), GREATER_THAN, issuedOffset))
        .filter(s -> StreamerUtil.isWriteCommit(metaClient.getTableType(), s, commitTimeline)).collect(Collectors.toList());
    if (instants.isEmpty()) {
      return Result.EMPTY;
    }
    String offsetToIssue = instants.stream().map(HoodieInstant::getStateTransitionTime).max(String::compareTo).orElse(issuedOffset);
    List<MergeOnReadInputSplit> inputSplits = instants.stream().map(instant -> {
      String instantTs = instant.getTimestamp();

      // Assumes we consume from timeline:
      //    c0, c1.inflight, c2(issued instant), c3, c4
      // -> c0, c1,          c2(issued instant), c3, c4
      // c1, c3 and c4 are the candidate instants,

      // c4 data file could include overlapping records from c2,
      // use (c2, c4] instant range for c3 and c4,

      // c1 data file could include overlapping records from c0,
      // use the [c1, c1] instant range for c1.
      InstantRange instantRange = InstantRange.builder()
          .startInstant(instantTs)
          .endInstant(instantTs)
          .nullableBoundary(cdcEnabled)
          .rangeType(InstantRange.RangeType.CLOSE_CLOSE).build();
      return getIncInputSplits(metaClient, hadoopConf, commitTimeline, Collections.singletonList(instant), instantRange, instantTs, cdcEnabled);
    }).flatMap(Collection::stream).collect(Collectors.toList());
    return Result.instance(inputSplits, issuedInstant, offsetToIssue);
  }

  @Nullable
  private InstantRange getInstantRange(String issuedInstant, String instantToIssue, boolean nullableBoundary) {
    if (issuedInstant != null) {
      // the streaming reader may record the last issued instant, if the issued instant is present,
      // the instant range should be: (issued instant, the latest instant].
      return InstantRange.builder().startInstant(issuedInstant).endInstant(instantToIssue)
          .nullableBoundary(nullableBoundary).rangeType(InstantRange.RangeType.OPEN_CLOSE).build();
    } else if (this.conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent()) {
      // first time consume and has a start commit
      final String startCommit = this.conf.getString(FlinkOptions.READ_START_COMMIT);
      return startCommit.equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)
          ? null
          : InstantRange.builder().startInstant(startCommit).endInstant(instantToIssue)
          .nullableBoundary(nullableBoundary).rangeType(InstantRange.RangeType.CLOSE_CLOSE).build();
    } else {
      // first time consume and no start commit, consumes the latest incremental data set.
      return InstantRange.builder().startInstant(instantToIssue).endInstant(instantToIssue)
          .nullableBoundary(nullableBoundary).rangeType(InstantRange.RangeType.CLOSE_CLOSE).build();
    }
  }

  private List<MergeOnReadInputSplit> getInputSplits(
      HoodieTableMetaClient metaClient,
      HoodieTimeline commitTimeline,
      List<StoragePathInfo> pathInfoList,
      Set<String> readPartitions,
      String endInstant,
      InstantRange instantRange,
      boolean skipBaseFiles) {
    final HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, commitTimeline, pathInfoList);
    final AtomicInteger cnt = new AtomicInteger(0);
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    return readPartitions.stream()
        .map(relPartitionPath -> getFileSlices(fsView, relPartitionPath, endInstant, skipBaseFiles)
            .map(fileSlice -> {
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .filter(logPath -> !logPath.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
                  .collect(Collectors.toList()));
              String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
              return new MergeOnReadInputSplit(cnt.getAndAdd(1),
                  basePath, logPaths, endInstant,
                  metaClient.getBasePath().toString(), maxCompactionMemoryInBytes, mergeType, instantRange, fileSlice.getFileId());
            }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private List<MergeOnReadInputSplit> getCdcInputSplits(
      HoodieTableMetaClient metaClient,
      InstantRange instantRange) {
    HoodieCDCExtractor extractor = new HoodieCDCExtractor(metaClient, instantRange);
    Map<HoodieFileGroupId, List<HoodieCDCFileSplit>> fileSplits = extractor.extractCDCFileSplits();

    if (fileSplits.isEmpty()) {
      LOG.warn("No change logs found for reading in path: " + path);
      return Collections.emptyList();
    }

    final AtomicInteger cnt = new AtomicInteger(0);
    return fileSplits.entrySet().stream()
        .map(splits ->
            new CdcInputSplit(cnt.getAndAdd(1), metaClient.getBasePath().toString(), maxCompactionMemoryInBytes,
                splits.getKey().getFileId(), splits.getValue().stream().sorted().toArray(HoodieCDCFileSplit[]::new)))
        .collect(Collectors.toList());
  }

  private static Stream<FileSlice> getFileSlices(
      HoodieTableFileSystemView fsView,
      String relPartitionPath,
      String endInstant,
      boolean skipBaseFiles) {
    return skipBaseFiles ? fsView.getAllLogsMergedFileSliceBeforeOrOn(relPartitionPath, endInstant)
        : fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, endInstant);
  }

  private FileIndex getFileIndex() {
    return FileIndex.builder()
        .path(new org.apache.hadoop.fs.Path(path.toUri()))
        .conf(conf)
        .rowType(rowType)
        .partitionPruner(partitionPruner)
        .build();
  }

  /**
   * Returns the partitions to read with given metadata list.
   * The partitions would be filtered by the pushed down required partitions.
   *
   * @param metadataList The metadata list
   * @return the set of read partitions
   */
  private Set<String> getReadPartitions(List<HoodieCommitMetadata> metadataList) {
    Set<String> partitions = HoodieTableMetadataUtil.getWritePartitionPaths(metadataList);
    // apply partition push down
    if (this.partitionPruner != null) {
      Set<String> selectedPartitions = this.partitionPruner.filter(partitions);
      double total = partitions.size();
      double selectedNum = selectedPartitions.size();
      double percentPruned = total == 0 ? 0 : (1 - selectedNum / total) * 100;
      LOG.info("Selected " + selectedNum + " partitions out of " + total
          + ", pruned " + percentPruned + "% partitions.");
      return selectedPartitions;
    }
    return partitions;
  }

  /**
   * Returns the archived metadata in case the reader consumes untimely or it wants
   * to read from the earliest.
   *
   * <p>Note: should improve it with metadata table when the metadata table is stable enough.
   *
   * @param metaClient     The meta client
   * @param instantRange   The instant range to filter the timeline instants
   * @param commitTimeline The commit timeline
   * @param tableName      The table name
   * @return the list of archived metadata, or empty if there is no need to read the archived timeline
   */
  private List<HoodieCommitMetadata> getArchivedMetadata(
      HoodieTableMetaClient metaClient,
      InstantRange instantRange,
      HoodieTimeline commitTimeline,
      String tableName) {
    if (commitTimeline.isBeforeTimelineStarts(instantRange.getStartInstant())) {
      // read the archived metadata if the start instant is archived.
      HoodieTimeline archivedTimeline = getArchivedReadTimeline(metaClient, instantRange.getStartInstant());
      if (!archivedTimeline.empty()) {
        return archivedTimeline.getInstantsAsStream()
            .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, archivedTimeline)).collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }

  private HoodieTimeline getReadTimeline(HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    return filterInstantsAsPerUserConfigs(timeline);
  }

  private HoodieTimeline getArchivedReadTimeline(HoodieTableMetaClient metaClient, String startInstant) {
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline(startInstant, false);
    HoodieTimeline archivedCompleteTimeline = archivedTimeline.getCommitsTimeline().filterCompletedInstants();
    return filterInstantsAsPerUserConfigs(archivedCompleteTimeline);
  }

  /**
   * Returns the instants with a given issuedInstant to start from.
   *
   * @param commitTimeline The completed commits timeline
   * @param issuedInstant  The last issued instant that has already been delivered to downstream
   *
   * @return the filtered hoodie instants
   */
  @VisibleForTesting
  public List<HoodieInstant> filterInstantsWithRange(
      HoodieTimeline commitTimeline,
      @Nullable final String issuedInstant) {
    HoodieTimeline completedTimeline = commitTimeline.filterCompletedInstants();
    if (issuedInstant != null) {
      // returns early for streaming mode
      return completedTimeline
          .getInstantsAsStream()
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN, issuedInstant))
          .collect(Collectors.toList());
    }

    Stream<HoodieInstant> instantStream = completedTimeline.getInstantsAsStream();

    if (OptionsResolver.hasNoSpecificReadCommits(this.conf)) {
      // by default read from the latest commit
      return completedTimeline.lastInstant().map(Collections::singletonList).orElseGet(Collections::emptyList);
    }

    if (OptionsResolver.isSpecificStartCommit(this.conf)) {
      final String startCommit = this.conf.get(FlinkOptions.READ_START_COMMIT);
      instantStream = instantStream
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, startCommit));
    }
    if (this.conf.getOptional(FlinkOptions.READ_END_COMMIT).isPresent()) {
      final String endCommit = this.conf.get(FlinkOptions.READ_END_COMMIT);
      instantStream = instantStream.filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, endCommit));
    }
    return instantStream.collect(Collectors.toList());
  }

  /**
   * Filters out the unnecessary instants as per user specified configs.
   *
   * @param timeline The timeline
   *
   * @return the filtered timeline
   */
  @VisibleForTesting
  public HoodieTimeline filterInstantsAsPerUserConfigs(HoodieTimeline timeline) {
    final HoodieTimeline oriTimeline = timeline;
    if (OptionsResolver.isMorTable(this.conf) & this.skipCompaction) {
      // the compaction commit uses 'commit' as action which is tricky
      timeline = timeline.filter(instant -> !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    }
    if (this.skipClustering) {
      timeline = timeline.filter(instant -> !ClusteringUtil.isClusteringInstant(instant, oriTimeline));
    }
    return timeline;
  }

  private static <T> List<T> mergeList(List<T> list1, List<T> list2) {
    if (list1.isEmpty()) {
      return list2;
    }
    if (list2.isEmpty()) {
      return list1;
    }
    List<T> merged = new ArrayList<>(list1);
    merged.addAll(list2);
    return merged;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Represents a result of calling {@link #inputSplits}.
   */
  public static class Result {
    private final List<MergeOnReadInputSplit> inputSplits; // input splits
    private final String endInstant; // end instant to consume to
    private final String offset;     // monotonic increasing consumption offset

    public static final Result EMPTY = instance(Collections.emptyList(), "");

    public boolean isEmpty() {
      return this.inputSplits.size() == 0;
    }

    public List<MergeOnReadInputSplit> getInputSplits() {
      return this.inputSplits;
    }

    public String getEndInstant() {
      return this.endInstant;
    }

    @Nullable
    public String getOffset() {
      return offset;
    }

    private Result(List<MergeOnReadInputSplit> inputSplits, String endInstant, @Nullable String offset) {
      this.inputSplits = inputSplits;
      this.endInstant = endInstant;
      this.offset = offset;
    }

    public static Result instance(List<MergeOnReadInputSplit> inputSplits, String endInstant) {
      return new Result(inputSplits, endInstant, null);
    }

    public static Result instance(List<MergeOnReadInputSplit> inputSplits, String endInstant, String offset) {
      return new Result(inputSplits, endInstant, offset);
    }
  }

  /**
   * Builder for {@link IncrementalInputSplits}.
   */
  public static class Builder {
    private Configuration conf;
    private Path path;
    private RowType rowType;
    private long maxCompactionMemoryInBytes;
    // for partition pruning
    private PartitionPruners.PartitionPruner partitionPruner;
    // skip compaction
    private boolean skipCompaction = false;
    // skip clustering
    private boolean skipClustering = false;

    public Builder() {
    }

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder path(Path path) {
      this.path = path;
      return this;
    }

    public Builder rowType(RowType rowType) {
      this.rowType = rowType;
      return this;
    }

    public Builder maxCompactionMemoryInBytes(long maxCompactionMemoryInBytes) {
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      return this;
    }

    public Builder partitionPruner(@Nullable PartitionPruners.PartitionPruner partitionPruner) {
      this.partitionPruner = partitionPruner;
      return this;
    }

    public Builder skipCompaction(boolean skipCompaction) {
      this.skipCompaction = skipCompaction;
      return this;
    }

    public Builder skipClustering(boolean skipClustering) {
      this.skipClustering = skipClustering;
      return this;
    }

    public IncrementalInputSplits build() {
      return new IncrementalInputSplits(
          Objects.requireNonNull(this.conf), Objects.requireNonNull(this.path), Objects.requireNonNull(this.rowType),
          this.maxCompactionMemoryInBytes, this.partitionPruner, this.skipCompaction, this.skipClustering);
    }
  }
}
