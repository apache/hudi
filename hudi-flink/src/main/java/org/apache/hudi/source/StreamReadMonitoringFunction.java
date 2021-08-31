/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link MergeOnReadInputSplit}
 * , it is responsible for:
 *
 * <ol>
 *     <li>Monitoring a user-provided hoodie table path.</li>
 *     <li>Deciding which files(or split) should be further read and processed.</li>
 *     <li>Creating the {@link MergeOnReadInputSplit splits} corresponding to those files.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link StreamReadOperator}
 * which can have parallelism greater than one.
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending instant commits time order,
 * in each downstream task, the splits are also read in receiving sequence. We do not ensure split consuming sequence
 * among the downstream tasks.
 */
public class StreamReadMonitoringFunction
    extends RichSourceFunction<MergeOnReadInputSplit> implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(StreamReadMonitoringFunction.class);

  private static final long serialVersionUID = 1L;

  /**
   * The path to monitor.
   */
  private final Path path;

  /**
   * The interval between consecutive path scans.
   */
  private final long interval;

  private transient Object checkpointLock;

  private volatile boolean isRunning = true;

  private String issuedInstant;

  private transient ListState<String> instantState;

  private final Configuration conf;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private HoodieTableMetaClient metaClient;

  private final long maxCompactionMemoryInBytes;

  // for partition pruning
  private final Set<String> requiredPartitionPaths;

  public StreamReadMonitoringFunction(
      Configuration conf,
      Path path,
      long maxCompactionMemoryInBytes,
      Set<String> requiredPartitionPaths) {
    this.conf = conf;
    this.path = path;
    this.interval = conf.getInteger(FlinkOptions.READ_STREAMING_CHECK_INTERVAL);
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.requiredPartitionPaths = requiredPartitionPaths;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

    ValidationUtils.checkState(this.instantState == null,
        "The " + getClass().getSimpleName() + " has already been initialized.");

    this.instantState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "file-monitoring-state",
            StringSerializer.INSTANCE
        )
    );

    if (context.isRestored()) {
      LOG.info("Restoring state for the class {} with table {} and base path {}.",
          getClass().getSimpleName(), conf.getString(FlinkOptions.TABLE_NAME), path);

      List<String> retrievedStates = new ArrayList<>();
      for (String entry : this.instantState.get()) {
        retrievedStates.add(entry);
      }

      ValidationUtils.checkArgument(retrievedStates.size() <= 1,
          getClass().getSimpleName() + " retrieved invalid state.");

      if (retrievedStates.size() == 1 && issuedInstant != null) {
        // this is the case where we have both legacy and new state.
        // the two should be mutually exclusive for the operator, thus we throw the exception.

        throw new IllegalArgumentException(
            "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

      } else if (retrievedStates.size() == 1) {
        this.issuedInstant = retrievedStates.get(0);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} retrieved a issued instant of time {} for table {} with path {}.",
              getClass().getSimpleName(), issuedInstant, conf.get(FlinkOptions.TABLE_NAME), path);
        }
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.hadoopConf = StreamerUtil.getHadoopConf();
  }

  @Override
  public void run(SourceFunction.SourceContext<MergeOnReadInputSplit> context) throws Exception {
    checkpointLock = context.getCheckpointLock();
    while (isRunning) {
      synchronized (checkpointLock) {
        monitorDirAndForwardSplits(context);
      }
      TimeUnit.SECONDS.sleep(interval);
    }
  }

  @Nullable
  private HoodieTableMetaClient getOrCreateMetaClient() {
    if (this.metaClient != null) {
      return this.metaClient;
    }
    if (StreamerUtil.tableExists(this.path.toString(), hadoopConf)) {
      this.metaClient = StreamerUtil.createMetaClient(this.path.toString(), hadoopConf);
      return this.metaClient;
    }
    // fallback
    return null;
  }

  @VisibleForTesting
  public void monitorDirAndForwardSplits(SourceContext<MergeOnReadInputSplit> context) {
    HoodieTableMetaClient metaClient = getOrCreateMetaClient();
    if (metaClient == null) {
      // table does not exist
      return;
    }
    metaClient.reloadActiveTimeline();
    HoodieTimeline commitTimeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    if (commitTimeline.empty()) {
      LOG.warn("No splits found for the table under path " + path);
      return;
    }
    List<HoodieInstant> instants = filterInstantsWithStart(commitTimeline, this.issuedInstant);
    // get the latest instant that satisfies condition
    final HoodieInstant instantToIssue = instants.size() == 0 ? null : instants.get(instants.size() - 1);
    final InstantRange instantRange;
    if (instantToIssue != null) {
      if (this.issuedInstant != null) {
        // had already consumed an instant
        instantRange = InstantRange.getInstance(this.issuedInstant, instantToIssue.getTimestamp(),
            InstantRange.RangeType.OPEN_CLOSE);
      } else if (this.conf.getOptional(FlinkOptions.READ_STREAMING_START_COMMIT).isPresent()) {
        // first time consume and has a start commit
        final String specifiedStart = this.conf.getString(FlinkOptions.READ_STREAMING_START_COMMIT);
        instantRange = specifiedStart.equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)
            ? null
            : InstantRange.getInstance(specifiedStart, instantToIssue.getTimestamp(), InstantRange.RangeType.CLOSE_CLOSE);
      } else {
        // first time consume and no start commit, consumes the latest incremental data set.
        instantRange = InstantRange.getInstance(instantToIssue.getTimestamp(), instantToIssue.getTimestamp(),
            InstantRange.RangeType.CLOSE_CLOSE);
      }
    } else {
      LOG.info("No new instant found for the table under path " + path + ", skip reading");
      return;
    }
    // generate input split:
    // 1. first fetch all the commit metadata for the incremental instants;
    // 2. filter the relative partition paths
    // 3. filter the full file paths
    // 4. use the file paths from #step 3 as the back-up of the filesystem view

    String tableName = conf.getString(FlinkOptions.TABLE_NAME);
    List<HoodieCommitMetadata> activeMetadataList = instants.stream()
        .map(instant -> WriteProfiles.getCommitMetadata(tableName, path, instant, commitTimeline)).collect(Collectors.toList());
    List<HoodieCommitMetadata> archivedMetadataList = getArchivedMetadata(instantRange, commitTimeline, tableName);
    List<HoodieCommitMetadata> metadataList = archivedMetadataList.size() > 0
        ? mergeList(activeMetadataList, archivedMetadataList)
        : activeMetadataList;

    Set<String> writePartitions = getWritePartitionPaths(metadataList);
    // apply partition push down
    if (this.requiredPartitionPaths.size() > 0) {
      writePartitions = writePartitions.stream()
          .filter(this.requiredPartitionPaths::contains).collect(Collectors.toSet());
    }
    FileStatus[] fileStatuses = WriteProfiles.getWritePathsOfInstants(path, hadoopConf, metadataList);
    if (fileStatuses.length == 0) {
      LOG.warn("No files found for reading in user provided path.");
      return;
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, commitTimeline, fileStatuses);
    final String commitToIssue = instantToIssue.getTimestamp();
    final AtomicInteger cnt = new AtomicInteger(0);
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    List<MergeOnReadInputSplit> inputSplits = writePartitions.stream()
        .map(relPartitionPath -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, commitToIssue)
        .map(fileSlice -> {
          Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
              .sorted(HoodieLogFile.getLogFileComparator())
              .map(logFile -> logFile.getPath().toString())
              .collect(Collectors.toList()));
          String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
          return new MergeOnReadInputSplit(cnt.getAndAdd(1),
              basePath, logPaths, commitToIssue,
              metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, instantRange);
        }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    for (MergeOnReadInputSplit split : inputSplits) {
      context.collect(split);
    }
    // update the issues instant time
    this.issuedInstant = commitToIssue;
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (checkpointLock != null) {
      synchronized (checkpointLock) {
        issuedInstant = null;
        isRunning = false;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed File Monitoring Source for path: " + path + ".");
    }
  }

  @Override
  public void cancel() {
    if (checkpointLock != null) {
      // this is to cover the case where cancel() is called before the run()
      synchronized (checkpointLock) {
        issuedInstant = null;
        isRunning = false;
      }
    } else {
      issuedInstant = null;
      isRunning = false;
    }
  }

  // -------------------------------------------------------------------------
  //  Checkpointing
  // -------------------------------------------------------------------------

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    this.instantState.clear();
    if (this.issuedInstant != null) {
      this.instantState.add(this.issuedInstant);
    }
  }

  /**
   * Returns the archived metadata in case the reader consumes untimely or it wants
   * to read from the earliest.
   *
   * <p>Note: should improve it with metadata table when the metadata table is stable enough.
   *
   * @param instantRange   The instant range to filter the timeline instants
   * @param commitTimeline The commit timeline
   * @param tableName      The table name
   * @return the list of archived metadata, or empty if there is no need to read the archived timeline
   */
  private List<HoodieCommitMetadata> getArchivedMetadata(
      InstantRange instantRange,
      HoodieTimeline commitTimeline,
      String tableName) {
    if (instantRange == null || commitTimeline.isBeforeTimelineStarts(instantRange.getStartInstant())) {
      // read the archived metadata if:
      // 1. the start commit is 'earliest';
      // 2. the start instant is archived.
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      if (!metaClient.getArchivedTimeline().empty()) {
        Stream<HoodieInstant> instantStream = archivedTimeline.getCommitsTimeline().filterCompletedInstants().getInstants();
        if (instantRange != null) {
          instantStream = instantStream.filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, instantRange.getStartInstant()));
        }
        return instantStream
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
  private List<HoodieInstant> filterInstantsWithStart(
      HoodieTimeline commitTimeline,
      final String issuedInstant) {
    HoodieTimeline completedTimeline = commitTimeline.filterCompletedInstants();
    if (issuedInstant != null) {
      return completedTimeline.getInstants()
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN, issuedInstant))
          .collect(Collectors.toList());
    } else if (this.conf.getOptional(FlinkOptions.READ_STREAMING_START_COMMIT).isPresent()
        && !this.conf.get(FlinkOptions.READ_STREAMING_START_COMMIT).equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)) {
      String definedStartCommit = this.conf.get(FlinkOptions.READ_STREAMING_START_COMMIT);
      return completedTimeline.getInstants()
          .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, definedStartCommit))
          .collect(Collectors.toList());
    } else {
      return completedTimeline.getInstants().collect(Collectors.toList());
    }
  }

  /**
   * Returns all the incremental write partition paths as a set with the given commits metadata.
   *
   * @param metadataList The commits metadata
   * @return the partition path set
   */
  private Set<String> getWritePartitionPaths(List<HoodieCommitMetadata> metadataList) {
    return metadataList.stream()
        .map(HoodieCommitMetadata::getWritePartitionPaths)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private static <T> List<T> mergeList(List<T> list1, List<T> list2) {
    List<T> merged = new ArrayList<>(list1);
    merged.addAll(list2);
    return merged;
  }
}
