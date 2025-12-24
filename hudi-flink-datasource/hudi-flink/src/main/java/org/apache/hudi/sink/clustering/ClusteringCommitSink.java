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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkClusteringMetrics;
import org.apache.hudi.metrics.enums.FlinkClusteringMetricEnum;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Function to check and commit the clustering action.
 *
 * <p> Each time after receiving a clustering commit event {@link ClusteringCommitEvent},
 * it loads and checks the clustering plan {@link org.apache.hudi.avro.model.HoodieClusteringPlan},
 * if all the clustering operations {@link org.apache.hudi.common.model.ClusteringOperation}
 * of the plan are finished, tries to commit the clustering action.
 *
 * <p>It also inherits the {@link CleanFunction} cleaning ability. This is needed because
 * the SQL API does not allow multiple sinks in one table sink provider.
 */
public class ClusteringCommitSink extends CleanFunction<ClusteringCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(ClusteringCommitSink.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  private transient HoodieFlinkTable<?> table;

  /**
   * Buffer to collect the event from each clustering task {@code ClusteringFunction}.
   * The key is the instant time.
   */
  private transient Map<String, List<ClusteringCommitEvent>> commitBuffer;

  /**
   * Cache to store clustering plan for each instant.
   * Stores the mapping of instant_time -> clusteringPlan.
   */
  private transient Map<String, HoodieClusteringPlan> clusteringPlanCache;

  /**
   * clustering metric
   */
  private transient FlinkClusteringMetrics clusteringMetrics;

  public ClusteringCommitSink(Configuration conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (writeClient == null) {
      this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    }
    registerMetrics();
    this.commitBuffer = new HashMap<>();
    this.clusteringPlanCache = new HashMap<>();
    this.table = writeClient.getHoodieTable();
  }

  @Override
  public void invoke(ClusteringCommitEvent event, Context context) throws Exception {
    final String instant = event.getInstant();
    commitBuffer.computeIfAbsent(instant, k -> new ArrayList<>())
        .add(event);
    commitIfNecessary(instant, commitBuffer.get(instant));
  }

  /**
   * Condition to commit: the commit buffer has equal size with the clustering plan operations
   * and all the clustering commit event {@link ClusteringCommitEvent} has the same clustering instant time.
   *
   * @param instant Clustering commit instant time
   * @param events  Commit events ever received for the instant
   */
  private void commitIfNecessary(String instant, List<ClusteringCommitEvent> events) {
    HoodieClusteringPlan clusteringPlan = clusteringPlanCache.computeIfAbsent(instant, k -> {
      try {
        Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
            this.writeClient.getHoodieTable().getMetaClient(), HoodieTimeline.getReplaceCommitInflightInstant(instant));
        return clusteringPlanOption.get().getRight();
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    });
    boolean isReady = clusteringPlan.getInputGroups().size() == events.size();
    if (!isReady) {
      return;
    }

    if (events.stream().anyMatch(ClusteringCommitEvent::isFailed)) {
      try {
        // handle failure case
        ClusteringUtil.rollbackClustering(table, writeClient, instant);
      } finally {
        clusteringMetrics.markClusteringRolledBack();
        // remove commitBuffer to avoid obsolete metadata commit
        reset(instant);
      }
      return;
    }

    try {
      doCommit(instant, clusteringPlan, events);
    } catch (Throwable throwable) {
      // make it fail-safe
      LOG.error("Error while committing clustering instant: " + instant, throwable);
    } finally {
      // reset the status
      reset(instant);
    }
  }

  private void doCommit(String instant, HoodieClusteringPlan clusteringPlan, List<ClusteringCommitEvent> events) {
    List<WriteStatus> statuses = events.stream()
        .map(ClusteringCommitEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    long numErrorRecords = statuses.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);

    if (numErrorRecords > 0 && !this.conf.getBoolean(FlinkOptions.IGNORE_FAILED)) {
      // handle failure case
      LOG.error("Got {} error records during clustering of instant {},\n"
          + "option '{}' is configured as false,"
          + "rolls back the clustering", numErrorRecords, instant, FlinkOptions.IGNORE_FAILED.key());
      clusteringMetrics.markClusteringRolledBack();
      ClusteringUtil.rollbackClustering(table, writeClient, instant);
      return;
    }

    HoodieWriteMetadata<List<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(statuses);
    writeMetadata.setWriteStats(statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()));
    writeMetadata.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(clusteringPlan, writeMetadata));
    validateWriteResult(clusteringPlan, instant, writeMetadata);
    if (!writeMetadata.getCommitMetadata().isPresent()) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(
          writeMetadata.getWriteStats().get(),
          writeMetadata.getPartitionToReplaceFileIds(),
          Option.empty(),
          WriteOperationType.CLUSTER,
          this.writeClient.getConfig().getSchema(),
          HoodieTimeline.REPLACE_COMMIT_ACTION);
      writeMetadata.setCommitMetadata(Option.of(commitMetadata));
    }
    // commit the clustering
    this.table.getMetaClient().reloadActiveTimeline();
    this.writeClient.completeTableService(
        TableServiceType.CLUSTER, writeMetadata.getCommitMetadata().get(), table, instant);

    // if clustering successful then update metric
    this.clusteringMetrics.updateCommitMetrics(instant, writeMetadata.getCommitMetadata().get());
    this.clusteringMetrics.markClusteringCompleted();

    // whether to clean up the input base parquet files used for clustering
    if (!conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED) && !isCleaning) {
      LOG.info("Running inline clean");
      this.writeClient.clean();
    }
  }

  private void reset(String instant) {
    this.commitBuffer.remove(instant);
    this.clusteringPlanCache.remove(instant);
  }

  /**
   * Validate actions taken by clustering. In the first implementation, we validate at least one new file is written.
   * But we can extend this to add more validation. E.g. number of records read = number of records written etc.
   * We can also make these validations in BaseCommitActionExecutor to reuse pre-commit hooks for multiple actions.
   */
  private static void validateWriteResult(HoodieClusteringPlan clusteringPlan, String instantTime, HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    if (writeMetadata.getWriteStatuses().isEmpty()) {
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + instantTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(
      HoodieClusteringPlan clusteringPlan,
      HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    Set<HoodieFileGroupId> newFilesWritten = writeMetadata.getWriteStats().get().stream()
        .map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
    return ClusteringUtils.getFileGroupsFromClusteringPlan(clusteringPlan)
        .filter(fg -> !newFilesWritten.contains(fg))
        .collect(Collectors.groupingBy(HoodieFileGroupId::getPartitionPath, Collectors.mapping(HoodieFileGroupId::getFileId, Collectors.toList())));
  }

  private void registerMetrics() {
    this.clusteringMetrics = new FlinkClusteringMetrics(FlinkWriteClients.getHoodieClientConfig(this.conf));
    this.clusteringMetrics.registerMetrics(FlinkClusteringMetricEnum.notClusteringPlanOperator.value, FlinkClusteringMetricEnum.notClusteringOperator.value);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (this.clusteringMetrics != null) {
      this.clusteringMetrics.shutDown();
    }
  }
}
