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

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;
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

import java.util.List;
import java.util.Set;

public class ClusteringPlanPartitionFindOperator extends AbstractStreamOperator<ClusteringPartitionEvent>
    implements OneInputStreamOperator<Object, ClusteringPartitionEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanPartitionFindOperator.class);
  private final Configuration conf;

  private final long interval;
  private final RowType rowType;
  private long lastCheckTs;
  private transient NonThrownExecutor executor;
  private HoodieWriteConfig hoodieConfig;
  private Schema schema;
  private transient StreamRecordCollector<ClusteringPartitionEvent> collector;

  public ClusteringPlanPartitionFindOperator(Configuration conf, RowType rowType) {
    this.conf = conf;
    this.interval = conf.getLong(FlinkOptions.LSM_CLUSTERING_SCHEDULE_INTERVAL) * 1000;
    this.lastCheckTs = System.currentTimeMillis();
    this.rowType = BulkInsertWriterHelper.addMetadataFields(rowType, false);
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ClusteringPartitionEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.executor = NonThrownExecutor.builder(LOG).build();
    this.hoodieConfig = FlinkWriteClients.getHoodieClientConfig(conf, true);
    this.schema = AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(rowType));
    this.collector = new StreamRecordCollector<>(output);
  }

  @Override
  public void processElement(StreamRecord<Object> record) {
    // no op
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    executor.execute(() -> {
      try {
        HoodieFlinkTable<?> table = FlinkTables.createTable(conf, getRuntimeContext());
        long currentTs = System.currentTimeMillis();
        if (currentTs - lastCheckTs > interval) {
          if (scheduleClustering(table)) {
            lastCheckTs = currentTs;
          }
        } else {
          LOG.info("Skip current schedule lastCheckTs " + lastCheckTs + ", interval " + interval);
        }
      } catch (Throwable throwable) {
        // make it fail-safe
        LOG.error("Error while scheduling clustering plan for checkpoint: " + checkpointId, throwable);
      }
    }, "Get Clustering partitions");
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringPartitionEvent>> output) {
    this.output = output;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // no op
  }

  private boolean scheduleClustering(HoodieFlinkTable<?> table) {
    LsmBaseClusteringPlanStrategy clusteringPlanStrategy = new LsmBaseClusteringPlanStrategy(table, table.getContext(), hoodieConfig, schema);
    List<String> backtrackInstances = table.getMetaClient().getBacktrackInstances();

    // get the pending insert overwrite instant
    List<String> pendingBackTrackInstances = table.getMetaClient().getPendingBacktrackInstances();
    int pendingClusteringNum  = table.getActiveTimeline()
        .filter(s -> s.getAction().equalsIgnoreCase(HoodieTimeline.REPLACE_COMMIT_ACTION)
            && !backtrackInstances.contains(s.getTimestamp())
            && !pendingBackTrackInstances.contains(s.getTimestamp())
            && !s.isCompleted())
        .countInstants();
    if (pendingClusteringNum >= hoodieConfig.getLsmMaxOfPendingClustering()) {
      LOG.info(String.format("The num of pending clustering is %s >= %s.", pendingClusteringNum, hoodieConfig.getMaxOfPendingClustering()));
      return false;
    }

    Option<HoodieInstant> lastClusteringInstant = table.getActiveTimeline()
        .filter(s -> s.getAction().equalsIgnoreCase(HoodieTimeline.REPLACE_COMMIT_ACTION)
            && !backtrackInstances.contains(s.getTimestamp())
            && !pendingBackTrackInstances.contains(s.getTimestamp())).lastInstant();

    int commitsSinceLastClustering = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .findInstantsAfter(lastClusteringInstant.map(HoodieInstant::getTimestamp).orElse("0"), Integer.MAX_VALUE)
        .countInstants();

    if (commitsSinceLastClustering < 1) {
      LOG.info("Not scheduling clustering as only " + commitsSinceLastClustering
          + " commits was found since last clustering " + lastClusteringInstant + ". Waiting for at least 1 delta commit.");
      return false;
    }

    Pair<List<String>, Pair<Set<String>, Set<String>>> partitionsAndInstantsPair = clusteringPlanStrategy.getPartitionPathsToCluster();
    List<String> partitions = partitionsAndInstantsPair.getLeft();
    LOG.info("Try to schedule clustering for partitions : " + partitions);
    for (String partition : partitions) {
      collector.collect(new ClusteringPartitionEvent(partition, partitionsAndInstantsPair.getRight()));
    }
    return true;
  }

  @Override
  public void close() throws Exception {
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }
}
