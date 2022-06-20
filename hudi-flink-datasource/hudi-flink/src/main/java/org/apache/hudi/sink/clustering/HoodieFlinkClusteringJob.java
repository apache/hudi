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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink hudi clustering program that can be executed manually.
 */
public class HoodieFlinkClusteringJob {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkClusteringJob.class);

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set table type
    conf.setString(FlinkOptions.TABLE_TYPE, metaClient.getTableConfig().getTableType().name());

    // set record key field
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());

    // set partition field
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();

    // judge whether have operation
    // to compute the clustering instant time and do cluster.
    if (cfg.schedule) {
      String clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();
      boolean scheduled = writeClient.scheduleClusteringAtInstant(clusteringInstantTime, Option.empty());
      if (!scheduled) {
        // do nothing.
        LOG.info("No clustering plan for this job ");
        return;
      }
    }

    table.getMetaClient().reloadActiveTimeline();

    // fetch the instant based on the configured execution sequence
    HoodieTimeline timeline = table.getActiveTimeline().filterPendingReplaceTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    Option<HoodieInstant> requested = CompactionUtil.isLIFO(cfg.clusteringSeq) ? timeline.lastInstant() : timeline.firstInstant();
    if (!requested.isPresent()) {
      // do nothing.
      LOG.info("No clustering plan scheduled, turns on the clustering plan schedule with --schedule option");
      return;
    }

    HoodieInstant clusteringInstant = requested.get();

    HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringInstant.getTimestamp());
    if (timeline.containsInstant(inflightInstant)) {
      LOG.info("Rollback inflight clustering instant: [" + clusteringInstant + "]");
      table.rollbackInflightClustering(inflightInstant,
          commitToRollback -> writeClient.getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
    }

    // generate clustering plan
    // should support configurable commit metadata
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), clusteringInstant);

    if (!clusteringPlanOption.isPresent()) {
      // do nothing.
      LOG.info("No clustering plan scheduled, turns on the clustering plan schedule with --schedule option");
      return;
    }

    HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

    if (clusteringPlan == null || (clusteringPlan.getInputGroups() == null)
        || (clusteringPlan.getInputGroups().isEmpty())) {
      // No clustering plan, do nothing and return.
      LOG.info("No clustering plan for instant " + clusteringInstant.getTimestamp());
      return;
    }

    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(clusteringInstant.getTimestamp());
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
    if (!pendingClusteringTimeline.containsInstant(instant)) {
      // this means that the clustering plan was written to auxiliary path(.tmp)
      // but not the meta path(.hoodie), this usually happens when the job crush
      // exceptionally.

      // clean the clustering plan in auxiliary path and cancels the clustering.

      LOG.warn("The clustering plan was fetched through the auxiliary path(.tmp) but not the meta path(.hoodie).\n"
          + "Clean the clustering plan in auxiliary path and cancels the clustering");
      CompactionUtil.cleanInstant(table.getMetaClient(), instant);
      return;
    }

    // get clusteringParallelism.
    int clusteringParallelism = conf.getInteger(FlinkOptions.CLUSTERING_TASKS) == -1
        ? clusteringPlan.getInputGroups().size() : conf.getInteger(FlinkOptions.CLUSTERING_TASKS);

    // Mark instant as clustering inflight
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());

    final Schema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();

    // setup configuration
    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

    DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(timeline.lastInstant().get(), clusteringPlan))
        .name("clustering_source")
        .uid("uid_clustering_source")
        .rebalance()
        .transform("clustering_task",
            TypeInformation.of(ClusteringCommitEvent.class),
            new ClusteringOperator(conf, rowType))
        .setParallelism(clusteringPlan.getInputGroups().size());

    ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
        conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

    dataStream
        .addSink(new ClusteringCommitSink(conf))
        .name("clustering_commit")
        .uid("uid_clustering_commit")
        .setParallelism(1);

    env.execute("flink_hudi_clustering");
  }
}
