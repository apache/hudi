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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Serializable;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
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
  private final long maxCompactionMemoryInBytes;
  // for partition pruning
  private final Set<String> requiredPartitions;
  // skip compaction
  private final boolean skipCompaction;

  private IncrementalInputSplits(
      Configuration conf,
      Path path,
      long maxCompactionMemoryInBytes,
      @Nullable Set<String> requiredPartitions,
      boolean skipCompaction) {
    this.conf = conf;
    this.path = path;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.requiredPartitions = requiredPartitions;
    this.skipCompaction = skipCompaction;
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
   * @param hadoopConf The hadoop configuration
   * @return The list of incremental input splits or empty if there are no new instants
   */
  public Result inputSplits(
      HoodieTableMetaClient metaClient,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    return inputSplits(metaClient, hadoopConf, null);
  }

  /**
   * Returns the incremental input splits.
   *
   * @param metaClient    The meta client
   * @param hadoopConf    The hadoop configuration
   * @param issuedInstant The last issued instant, only valid in streaming read
   * @return The list of incremental input splits or empty if there are no new instants
   */
  public Result inputSplits(
      HoodieTableMetaClient metaClient,
      org.apache.hadoop.conf.Configuration hadoopConf,
      String issuedInstant) {
    metaClient.reloadActiveTimeline();
    HoodieTimeline commitTimeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    if (commitTimeline.empty()) {
      LOG.warn("No splits found for the table under path " + path);
      return Result.EMPTY;
    }
    List<HoodieInstant> instants = filterInstantsWithRange(commitTimeline, issuedInstant);
    // get the latest instant that satisfies condition
    final HoodieInstant instantToIssue = instants.size() == 0 ? null : instants.get(instants.size() - 1);
    final InstantRange instantRange;
    if (instantToIssue != null) {
      if (issuedInstant != null) {
        // the streaming reader may record the last issued instant, if the issued instant is present,
        // the instant range should be: (issued instant, the latest instant].
        instantRange = InstantRange.getInstance(issuedInstant, instantToIssue.getTimestamp(),
            InstantRange.RangeType.OPEN_CLOSE);
      } else if (this.conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent()) {
        // first time consume and has a start commit
        final String startCommit = this.conf.getString(FlinkOptions.READ_START_COMMIT);
        instantRange = startCommit.equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)
            ? null
            : InstantRange.getInstance(startCommit, instantToIssue.getTimestamp(), InstantRange.RangeType.CLOSE_CLOSE);
      } else {
        // first time consume and no start commit, consumes the latest incremental data set.
        instantRange = InstantRange.getInstance(instantToIssue.getTimestamp(), instantToIssue.getTimestamp(),
            InstantRange.RangeType.CLOSE_CLOSE);
      }
    } else {
      LOG.info("No new instant found for the table under path " + path + ", skip reading");
      return Result.EMPTY;
    }

    String tableName = conf.getString(FlinkOptions.TABLE_NAME);

    Set<String> writePartitions;
    final FileStatus[] fileStatuses;

    if (instantRange == null) {
      // reading from the earliest, scans the partitions and files directly.
      FileIndex fileIndex = FileIndex.instance(new org.apache.hadoop.fs.Path(path.toUri()), conf);
      if (this.requiredPartitions != null) {
        // apply partition push down
        fileIndex.setPartitionPaths(this.requiredPartitions);
      }
      writePartitions = new HashSet<>(fileIndex.getOrBuildPartitionPaths());
      if (writePartitions.size() == 0) {
        LOG.warn("No partitions found for reading in user provided path.");
        return Result.EMPTY;
      }
      fileStatuses = fileIndex.getFilesInPartitions();
    } else {
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
      List<HoodieCommitMetadata> metadataList = archivedMetadataList.size() > 0
          // IMPORTANT: the merged metadata list must be in ascending order by instant time
          ? mergeList(archivedMetadataList, activeMetadataList)
          : activeMetadataList;

      writePartitions = HoodieInputFormatUtils.getWritePartitionPaths(metadataList);
      // apply partition push down
      if (this.requiredPartitions != null) {
        writePartitions = writePartitions.stream()
            .filter(this.requiredPartitions::contains).collect(Collectors.toSet());
      }
      if (writePartitions.size() == 0) {
        LOG.warn("No partitions found for reading in user provided path.");
        return Result.EMPTY;
      }
      fileStatuses = WriteProfiles.getWritePathsOfInstants(path, hadoopConf, metadataList, metaClient.getTableType());
    }

    if (fileStatuses.length == 0) {
      LOG.warn("No files found for reading in user provided path.");
      return Result.EMPTY;
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, commitTimeline, fileStatuses);
    final String endInstant = instantToIssue.getTimestamp();
    final AtomicInteger cnt = new AtomicInteger(0);
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    List<MergeOnReadInputSplit> inputSplits = writePartitions.stream()
        .map(relPartitionPath -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, endInstant)
            .map(fileSlice -> {
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
              String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
              return new MergeOnReadInputSplit(cnt.getAndAdd(1),
                  basePath, logPaths, endInstant,
                  metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, instantRange);
            }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
    return Result.instance(inputSplits, endInstant);
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
    if (instantRange == null || commitTimeline.isBeforeTimelineStarts(instantRange.getStartInstant())) {
      // read the archived metadata if:
      // 1. the start commit is 'earliest';
      // 2. the start instant is archived.
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      HoodieTimeline archivedCompleteTimeline = archivedTimeline.getCommitsTimeline().filterCompletedInstants();
      if (!archivedCompleteTimeline.empty()) {
        final String endTs = archivedCompleteTimeline.lastInstant().get().getTimestamp();
        Stream<HoodieInstant> instantStream = archivedCompleteTimeline.getInstants();
        if (instantRange != null) {
          archivedTimeline.loadInstantDetailsInMemory(instantRange.getStartInstant(), endTs);
          instantStream = instantStream.filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, instantRange.getStartInstant()));
        } else {
          final String startTs = archivedCompleteTimeline.firstInstant().get().getTimestamp();
          archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
        }
        return maySkipCompaction(instantStream)
            .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, archivedTimeline)).collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }

  /**
   * Returns the instants with a given issuedInstant to start from.
   *
   * @param commitTimeline The completed commits timeline
   * @param issuedInstant  The last issued instant that has already been delivered to downstream
   * @return the filtered hoodie instants
   */
  private List<HoodieInstant> filterInstantsWithRange(
      HoodieTimeline commitTimeline,
      final String issuedInstant) {
    HoodieTimeline completedTimeline = commitTimeline.filterCompletedInstants();
    if (issuedInstant != null) {
      // returns early for streaming mode
      return completedTimeline.getInstants()
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN, issuedInstant))
          .collect(Collectors.toList());
    }

    Stream<HoodieInstant> instantStream = completedTimeline.getInstants();

    if (this.conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent()
        && !this.conf.get(FlinkOptions.READ_START_COMMIT).equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)) {
      final String startCommit = this.conf.get(FlinkOptions.READ_START_COMMIT);
      instantStream = instantStream
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, startCommit));
    }
    if (this.conf.getOptional(FlinkOptions.READ_END_COMMIT).isPresent()) {
      final String endCommit = this.conf.get(FlinkOptions.READ_END_COMMIT);
      instantStream = instantStream.filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, endCommit));
    }
    return maySkipCompaction(instantStream).collect(Collectors.toList());
  }

  private Stream<HoodieInstant> maySkipCompaction(Stream<HoodieInstant> instants) {
    return this.skipCompaction
        ? instants.filter(instant -> !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
        : instants;
  }

  private static <T> List<T> mergeList(List<T> list1, List<T> list2) {
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

    private Result(List<MergeOnReadInputSplit> inputSplits, String endInstant) {
      this.inputSplits = inputSplits;
      this.endInstant = endInstant;
    }

    public static Result instance(List<MergeOnReadInputSplit> inputSplits, String endInstant) {
      return new Result(inputSplits, endInstant);
    }
  }

  /**
   * Builder for {@link IncrementalInputSplits}.
   */
  public static class Builder {
    private Configuration conf;
    private Path path;
    private long maxCompactionMemoryInBytes;
    // for partition pruning
    private Set<String> requiredPartitions;
    // skip compaction
    private boolean skipCompaction = false;

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

    public Builder maxCompactionMemoryInBytes(long maxCompactionMemoryInBytes) {
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      return this;
    }

    public Builder requiredPartitions(@Nullable Set<String> requiredPartitions) {
      this.requiredPartitions = requiredPartitions;
      return this;
    }

    public Builder skipCompaction(boolean skipCompaction) {
      this.skipCompaction = skipCompaction;
      return this;
    }

    public IncrementalInputSplits build() {
      return new IncrementalInputSplits(Objects.requireNonNull(this.conf), Objects.requireNonNull(this.path),
          this.maxCompactionMemoryInBytes, this.requiredPartitions, this.skipCompaction);
    }
  }
}
