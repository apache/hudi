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

package org.apache.hudi.sink.clustering;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.hudi.table.action.cluster.strategy.LsmBaseClusteringPlanStrategy;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.FsCacheCleanUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusteringPlanGenerateOperator extends AbstractStreamOperator<Object>
    implements OneInputStreamOperator<ClusteringFileEvent, Object> {

  protected static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanGenerateOperator.class);

  private final Configuration conf;
  private final RowType rowType;
  private final long interval;
  private final long step;
  private long checkLatency = 0;
  private long lastCheckTs = -1;
  private HoodieWriteConfig hoodieConfig;
  private NonThrownExecutor executor;
  // <Pair<PartitionPath, FileID>, <Logs>>
  private final Map<Pair<String, String>, TreeSet<HoodieLSMLogFile>> files = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<HoodieClusteringGroup> groups = new ConcurrentLinkedQueue<>();
  // <Pair<PartitionPath, FileID>, max-level>
  private final Map<Pair<String, String>, Integer> flags = new ConcurrentHashMap<>();
  private final Set<String> missingInstants = new HashSet<>();
  private final Set<String> completedInstants = new HashSet<>();
  private final Set<String> missingPartitions = new HashSet<>();
  private int numGroups;
  private Schema schema;
  private HoodieActiveTimeline activeTimeline;
  private boolean skipOP;

  public ClusteringPlanGenerateOperator(Configuration conf, RowType rowType) {
    this.conf = conf;
    this.interval = conf.getLong(FlinkOptions.LSM_CLUSTERING_SCHEDULE_INTERVAL) * 1000;
    this.step = (long) (Math.ceil(this.interval * 1.0) / 5);
    this.rowType = BulkInsertWriterHelper.addMetadataFields(rowType, false);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.hoodieConfig = FlinkWriteClients.getHoodieClientConfig(conf, true);
    this.executor = NonThrownExecutor.builder(LOG).build();
    this.numGroups = hoodieConfig.getLsmClusteringMinNumGroups();
    this.schema = AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(rowType));
    this.activeTimeline = FlinkTables.createTable(conf, getRuntimeContext()).getActiveTimeline();
    this.skipOP = !this.hoodieConfig.isLsmReadFooterEnabled();
  }

  @Override
  public void processElement(StreamRecord<ClusteringFileEvent> record) {
    if (lastCheckTs == -1) {
      this.lastCheckTs = System.currentTimeMillis();
    }
    executor.execute(() -> {
      long currentTs = System.currentTimeMillis();
      if (currentTs - lastCheckTs > interval + checkLatency) {
        LOG.info("Start to schedule clustering plan " + " flags " + flags + ". groups: " + groups);
        if (tryScheduleClusteringPlan()) {
          lastCheckTs = currentTs;
          checkLatency = 0;
          LOG.info("Schedule clustering successful.");
        } else {
          checkLatency += step;
          LOG.info("No clustering group to save. Wait " + step + " ms to save again.");
        }
        flags.clear();
      } else {
        LOG.info("Still wait for interval " + (interval + checkLatency) + ". Gap " + (currentTs - lastCheckTs));
      }

      // event 可能来自于不同分区下的不同FileSlice
      ClusteringFileEvent event = record.getValue();
      String fileID = event.getFileID();
      String partitionPath = event.getPartitionPath();
      Pair<String, String> key = Pair.of(partitionPath, fileID);

      if (event instanceof ClusteringEndFileEvent && files.containsKey(key)) {
        TreeSet<HoodieLSMLogFile> logs = files.remove(key);
        // 当前Key下的其他slice都不再接收
        flags.put(key, 1);
        HoodieFlinkTable<?> table = FlinkTables.createTable(conf, getRuntimeContext());
        LsmBaseClusteringPlanStrategy strategy = new LsmBaseClusteringPlanStrategy(table, table.getContext(), hoodieConfig, schema);
        missingInstants.addAll(event.getMissingAndCompletedInstants().getLeft());
        completedInstants.addAll(event.getMissingAndCompletedInstants().getRight());
        buildAndCacheClusteringGroup(table, strategy, logs, key);
      } else if (!flags.containsKey(key)) {
        HoodieLSMLogFile logFile = event.getLogFile();
        logFile.setFileStatus(event.getFileStatus());
        if (files.containsKey(key)) {
          TreeSet<HoodieLSMLogFile> tree = files.get(key);
          tree.add(logFile);
        } else {
          TreeSet<HoodieLSMLogFile> tree = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
          tree.add(logFile);
          files.put(key, tree);
        }
      }
      // drop current record directly, it doesn't mater will come again in next loop.
    }, "Try to schedule clustering plan");
  }

  private boolean tryScheduleClusteringPlan() {
    if (groups.size() > 0) {
      Option<HoodieClusteringPlan> plan = LsmBaseClusteringPlanStrategy.buildClusteringPlan(FlinkTables.createTable(conf, getRuntimeContext()).getMetaClient(),
          groups.stream().collect(Collectors.toList()), hoodieConfig, missingInstants, completedInstants, missingPartitions,
          new HashMap<>(), new HashMap<>(), ClusteringPlanStrategy.CLUSTERING_PLAN_VERSION_1);
      saveClusteringPlan(plan);
      groups.clear();
      missingPartitions.clear();
      missingInstants.clear();
      completedInstants.clear();
      return true;
    }
    return false;
  }

  private void buildAndCacheClusteringGroup(HoodieFlinkTable<?> table, LsmBaseClusteringPlanStrategy strategy, TreeSet<HoodieLSMLogFile> logs, Pair<String, String> partitionPathToFileId) {
    String partitionPath = partitionPathToFileId.getLeft();
    String fileId = partitionPathToFileId.getRight();
    Pair<List<HoodieLogFile>, Pair<Boolean, Boolean>> res = getEligibleLogFiles(table, strategy, logs, partitionPath, fileId);
    Boolean level1Exists = res.getRight().getLeft();
    Boolean level1ExistsInPending = res.getRight().getRight();
    Pair<Boolean, Stream<HoodieClusteringGroup>> clusteringGroups =
        (Pair<Boolean, Stream<HoodieClusteringGroup>>) strategy.buildClusteringGroups(res.getLeft(), level1Exists, level1ExistsInPending, partitionPath, skipOP);
    if (clusteringGroups.getLeft()) {
      missingPartitions.add(partitionPath);
    }
    groups.addAll(clusteringGroups.getRight().collect(Collectors.toList()));
  }

  /**
   * Get Eligible Log Files for Clutering Schedule
   *
   * @param table
   * @param logs
   * @param partitionPath
   * @param fileId
   * @return Pair<List logfiles, Pair<boolean level1Exists, boolean level1ExistInPendings>>
   */
  private Pair<List<HoodieLogFile>, Pair<Boolean, Boolean>> getEligibleLogFiles(HoodieFlinkTable<?> table, LsmBaseClusteringPlanStrategy strategy,
                                                                                TreeSet<HoodieLSMLogFile> logs, String partitionPath, String fileId) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    FileStatus[] fileStatuses = logs.stream().map(HoodieLogFile::getFileStatus).toArray(FileStatus[]::new);

    // build fsView based on log files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedAndReplaceInstants(),
        fileStatuses);
    // use latest fileslice to drop pending and replaced files
    Option<FileSlice> fileSlice = fsView.getLatestFileSlice(partitionPath, fileId);
    Set<String> pendingClusteringFiles = fsView.getFileGroupsInPendingClustering()
        .map(Pair::getKey).map(HoodieFileGroupId::getFileId).collect(Collectors.toSet());
    Set<String> level1InPendingClustering = fsView.getLevel1FileIdInPendingClustering(partitionPath);

    if (fileSlice.isPresent() && !fileSlice.get().isFileSliceEmpty()) {
      HashSet<String> level1 = new HashSet<>();
      List finalLogs = strategy.dropPending(fileSlice.get().getLogFiles(), level1, pendingClusteringFiles);
      // <logFiles, levelExists>
      return Pair.of(finalLogs, Pair.of(!level1.isEmpty(), level1InPendingClustering.contains(fileId)));
    }
    return Pair.of(Collections.EMPTY_LIST, Pair.of(false, false));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // no op
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<Object>> output) {
    this.output = output;
  }

  private void saveClusteringPlan(Option<HoodieClusteringPlan> planOption) {
    if (planOption.isPresent()) {
      String instantTime = HoodieActiveTimeline.createNewInstantTime();
      HoodieInstant clusteringInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
      try {
        HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
            .setOperationType(WriteOperationType.CLUSTER.name())
            .setExtraMetadata(Collections.emptyMap())
            .setClusteringPlan(planOption.get())
            .build();
        activeTimeline.saveToPendingReplaceCommit(clusteringInstant,
            TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
        LOG.info("Generate clustering plan at " + instantTime);
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling clustering", ioe);
      }
    }
  }

  @Override
  public void close() throws Exception {
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }
}
