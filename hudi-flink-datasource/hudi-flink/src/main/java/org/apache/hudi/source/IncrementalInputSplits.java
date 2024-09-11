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
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.format.cdc.CdcInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  // skip insert overwrite
  private final boolean skipInsertOverwrite;

  private IncrementalInputSplits(
      Configuration conf,
      Path path,
      RowType rowType,
      long maxCompactionMemoryInBytes,
      @Nullable PartitionPruners.PartitionPruner partitionPruner,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite) {
    this.conf = conf;
    this.path = path;
    this.rowType = rowType;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.partitionPruner = partitionPruner;
    this.skipCompaction = skipCompaction;
    this.skipClustering = skipClustering;
    this.skipInsertOverwrite = skipInsertOverwrite;
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

    IncrementalQueryAnalyzer analyzer = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startTime(this.conf.getString(FlinkOptions.READ_START_COMMIT))
        .endTime(this.conf.getString(FlinkOptions.READ_END_COMMIT))
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .readCdcFromChangelog(this.conf.getBoolean(FlinkOptions.READ_CDC_FROM_CHANGELOG))
        .build();

    IncrementalQueryAnalyzer.QueryContext analyzingResult = analyzer.analyze();

    if (analyzingResult.isEmpty()) {
      LOG.info("No new instant found for the table under path " + path + ", skip reading");
      return Result.EMPTY;
    }
    final HoodieTimeline commitTimeline = analyzingResult.getActiveTimeline();
    final boolean startFromEarliest = analyzingResult.isConsumingFromEarliest();
    final boolean hasArchivedInstants = !analyzingResult.getArchivedInstants().isEmpty();
    // We better add another premise: whether the endCommit is cleaned.
    boolean fullTableScan = startFromEarliest || hasArchivedInstants;

    // Step1: generates the instant range
    // if the specified end commit is archived, still uses the specified timestamp,
    // else uses the latest filtered instant time
    // (would be the latest instant time if the specified end commit is greater than the latest instant time)

    final InstantRange instantRange = analyzingResult.getInstantRange().orElse(null);

    // Step2: decides the read end commit
    final String endInstant = analyzingResult.getLastInstant();

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
      if (cdcEnabled) {
        // case1: cdc change log enabled
        List<MergeOnReadInputSplit> inputSplits = getCdcInputSplits(metaClient, instantRange);
        return Result.instance(inputSplits, endInstant);
      }
      // case2: normal incremental read
      String tableName = conf.getString(FlinkOptions.TABLE_NAME);
      List<HoodieInstant> instants = analyzingResult.getActiveInstants();
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
        fileInfoList, readPartitions, endInstant, analyzingResult.getMaxCompletionTime(), instantRange, false);

    return Result.instance(inputSplits, endInstant);
  }

  /**
   * Returns the incremental input splits.
   *
   * @param metaClient    The meta client
   * @param issuedOffset  The last issued offset, only valid in streaming read
   * @param cdcEnabled    Whether cdc is enabled
   *
   * @return The list of incremental input splits or empty if there are no new instants
   */
  public Result inputSplits(
      HoodieTableMetaClient metaClient,
      @Nullable String issuedOffset,
      boolean cdcEnabled) {
    metaClient.reloadActiveTimeline();
    IncrementalQueryAnalyzer analyzer = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startTime(issuedOffset != null ? issuedOffset : this.conf.getString(FlinkOptions.READ_START_COMMIT))
        .endTime(this.conf.getString(FlinkOptions.READ_END_COMMIT))
        .rangeType(issuedOffset != null ? InstantRange.RangeType.OPEN_CLOSED : InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .readCdcFromChangelog(this.conf.getBoolean(FlinkOptions.READ_CDC_FROM_CHANGELOG))
        .limit(OptionsResolver.getReadCommitsLimit(conf))
        .build();

    IncrementalQueryAnalyzer.QueryContext queryContext = analyzer.analyze();

    if (queryContext.isEmpty()) {
      LOG.info("No new instant found for the table under path " + path + ", skip reading");
      return Result.EMPTY;
    }

    HoodieTimeline commitTimeline = queryContext.getActiveTimeline();
    // get the latest instant that satisfies condition
    final String endInstant = queryContext.getLastInstant();
    final Option<InstantRange> instantRange = queryContext.getInstantRange();

    // version number should be monotonically increasing
    // fetch the instant offset by completion time
    String offsetToIssue = queryContext.getMaxCompletionTime();

    if (instantRange.isEmpty()) {
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
          pathInfoList, readPartitions, endInstant, offsetToIssue, null, false);

      return Result.instance(inputSplits, endInstant, offsetToIssue);
    } else {
      List<MergeOnReadInputSplit> inputSplits = getIncInputSplits(
          metaClient, (org.apache.hadoop.conf.Configuration) metaClient.getStorageConf().unwrap(),
          commitTimeline, queryContext, instantRange.get(), endInstant, cdcEnabled);
      return Result.instance(inputSplits, endInstant, offsetToIssue);
    }
  }

  /**
   * Returns the input splits for streaming incremental read.
   */
  private List<MergeOnReadInputSplit> getIncInputSplits(
      HoodieTableMetaClient metaClient,
      org.apache.hadoop.conf.Configuration hadoopConf,
      HoodieTimeline commitTimeline,
      IncrementalQueryAnalyzer.QueryContext queryContext,
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
    List<HoodieCommitMetadata> activeMetadataList = queryContext.getActiveInstants().stream()
        .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, commitTimeline)).collect(Collectors.toList());
    List<HoodieCommitMetadata> archivedMetadataList = queryContext.getArchivedInstants().stream()
        .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, queryContext.getArchivedTimeline())).collect(Collectors.toList());
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
        pathInfoList, readPartitions, endInstant, queryContext.getMaxCompletionTime(), instantRange, skipCompaction);
  }

  private List<MergeOnReadInputSplit> getInputSplits(
      HoodieTableMetaClient metaClient,
      HoodieTimeline commitTimeline,
      List<StoragePathInfo> pathInfoList,
      Set<String> readPartitions,
      String endInstant,
      String maxCompletionTime,
      InstantRange instantRange,
      boolean skipBaseFiles) {
    final HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, commitTimeline, pathInfoList);
    final AtomicInteger cnt = new AtomicInteger(0);
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    return readPartitions.stream()
        .map(relPartitionPath -> getFileSlices(fsView, relPartitionPath, maxCompletionTime, skipBaseFiles)
            .map(fileSlice -> {
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .filter(logPath -> !logPath.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
                  .collect(Collectors.toList()));
              String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
              // the latest commit is used as the limit of the log reader instant upper threshold,
              // it must be at least the latest instant time of the file slice to avoid data loss.
              String latestCommit = HoodieTimeline.minInstant(fileSlice.getLatestInstantTime(), endInstant);
              return new MergeOnReadInputSplit(cnt.getAndAdd(1),
                  basePath, logPaths, latestCommit,
                  metaClient.getBasePath().toString(), maxCompactionMemoryInBytes, mergeType, instantRange, fileSlice.getFileId());
            }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .sorted(Comparator.comparing(MergeOnReadInputSplit::getLatestCommit))
        .collect(Collectors.toList());
  }

  private List<MergeOnReadInputSplit> getCdcInputSplits(
      HoodieTableMetaClient metaClient,
      InstantRange instantRange) {
    HoodieCDCExtractor extractor = new HoodieCDCExtractor(metaClient, instantRange, OptionsResolver.readCDCFromChangelog(this.conf));
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
        .path(new StoragePath(path.toUri()))
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
    // skip insert overwrite
    private boolean skipInsertOverwrite = false;

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

    public Builder skipInsertOverwrite(boolean skipInsertOverwrite) {
      this.skipInsertOverwrite = skipInsertOverwrite;
      return this;
    }

    public IncrementalInputSplits build() {
      return new IncrementalInputSplits(
          Objects.requireNonNull(this.conf), Objects.requireNonNull(this.path), Objects.requireNonNull(this.rowType),
          this.maxCompactionMemoryInBytes, this.partitionPruner, this.skipCompaction, this.skipClustering, this.skipInsertOverwrite);
    }
  }
}
