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

package org.apache.hudi.sink.cluster;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanSourceFunction;
import org.apache.hudi.sink.clustering.FlinkClusteringConfig;
import org.apache.hudi.sink.clustering.HoodieFlinkClusteringJob;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for {@link HoodieFlinkClusteringJob}.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestHoodieFlinkClustering {

  private static final Map<String, String> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", "[id1,par1,id1,Danny,23,1000,par1, id2,par1,id2,Stephen,33,2000,par1]");
    EXPECTED.put("par2", "[id3,par2,id3,Julian,53,3000,par2, id4,par2,id4,Fabian,31,4000,par2]");
    EXPECTED.put("par3", "[id5,par3,id5,Sophia,18,5000,par3, id6,par3,id6,Emma,20,6000,par3]");
    EXPECTED.put("par4", "[id7,par4,id7,Bob,44,7000,par4, id8,par4,id8,Han,56,8000,par4]");
  }

  @TempDir
  File tempFile;

  @Test
  public void testHoodieFlinkClustering() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.targetPartitions = 4;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.
    String clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();

    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf);
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();

    boolean scheduled = writeClient.scheduleClusteringAtInstant(clusteringInstantTime, Option.empty());

    assertTrue(scheduled, "The clustering plan should be scheduled");

    // fetch the instant based on the configured execution sequence
    table.getMetaClient().reloadActiveTimeline();
    HoodieTimeline timeline = table.getActiveTimeline().filterPendingReplaceTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);

    // generate clustering plan
    // should support configurable commit metadata
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), timeline.lastInstant().get());

    HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

    // Mark instant as clustering inflight
    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(clusteringInstantTime);
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());

    final Schema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();

    DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstantTime, clusteringPlan))
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
    TestData.checkWrittenData(tempFile, EXPECTED, 4);
  }

  @Test
  public void testHoodieFlinkClusteringService() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.minClusteringIntervalSeconds = 3;
    cfg.schedule = true;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    HoodieFlinkClusteringJob.AsyncClusteringService asyncClusteringService = new HoodieFlinkClusteringJob.AsyncClusteringService(cfg, conf);
    asyncClusteringService.start(null);

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(5);

    asyncClusteringService.shutDown();

    TestData.checkWrittenData(tempFile, EXPECTED, 4);
  }

  @Test
  public void testHoodieFlinkClusteringSchedule() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 2);
    conf.setBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // To compute the clustering instant time.
    String clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();

    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf);

    boolean scheduled = writeClient.scheduleClusteringAtInstant(clusteringInstantTime, Option.empty());

    assertFalse(scheduled, "1 delta commit, the clustering plan should not be scheduled");

    tableEnv.executeSql(TestSQL.INSERT_T1).await();
    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();

    scheduled = writeClient.scheduleClusteringAtInstant(clusteringInstantTime, Option.empty());

    assertTrue(scheduled, "2 delta commits, the clustering plan should be scheduled");
  }

  @Test
  public void testHoodieFlinkClusteringScheduleAfterArchive() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.targetPartitions = 4;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set archive commits
    conf.setInteger(FlinkOptions.ARCHIVE_MAX_COMMITS.key(), 2);
    conf.setInteger(FlinkOptions.ARCHIVE_MIN_COMMITS.key(), 1);
    conf.setInteger(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), 0);

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.
    String firstClusteringInstant = HoodieActiveTimeline.createNewInstantTime();

    HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf);
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();

    boolean scheduled = writeClient.scheduleClusteringAtInstant(firstClusteringInstant, Option.empty());

    assertTrue(scheduled, "The clustering plan should be scheduled");

    // fetch the instant based on the configured execution sequence
    table.getMetaClient().reloadActiveTimeline();
    HoodieTimeline timeline = table.getActiveTimeline().filterPendingReplaceTimeline()
        .filter(i -> i.getState() == HoodieInstant.State.REQUESTED);

    // generate clustering plan
    // should support configurable commit metadata
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), timeline.lastInstant().get());

    HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

    // Mark instant as clustering inflight
    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(firstClusteringInstant);
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());

    final Schema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();

    DataStream<ClusteringCommitEvent> dataStream =
        env.addSource(new ClusteringPlanSourceFunction(firstClusteringInstant, clusteringPlan))
            .name("clustering_source")
            .uid("uid_clustering_source")
            .rebalance()
            .transform(
                "clustering_task",
                TypeInformation.of(ClusteringCommitEvent.class),
                new ClusteringOperator(conf, rowType))
            .setParallelism(clusteringPlan.getInputGroups().size());

    ExecNodeUtil.setManagedMemoryWeight(
        dataStream.getTransformation(),
        conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

    // keep pending clustering, not committing clustering
    dataStream
        .addSink(new DiscardingSink<>())
        .name("clustering_commit")
        .uid("uid_clustering_commit")
        .setParallelism(1);

    env.execute("flink_hudi_clustering");

    tableEnv.executeSql(TestSQL.INSERT_T1).await();
    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // archive the first commit, retain the second commit before the inflight replacecommit
    writeClient.archive();

    scheduled = writeClient.scheduleClusteringAtInstant(HoodieActiveTimeline.createNewInstantTime(), Option.empty());

    assertTrue(scheduled, "The clustering plan should be scheduled");
    table.getMetaClient().reloadActiveTimeline();
    timeline = table.getActiveTimeline().filterPendingReplaceTimeline()
        .filter(i -> i.getState() == HoodieInstant.State.REQUESTED);

    HoodieInstant secondClusteringInstant = timeline.lastInstant().get();
    List<HoodieClusteringGroup> inputFileGroups = ClusteringUtils.getClusteringPlan(table.getMetaClient(), secondClusteringInstant).get().getRight().getInputGroups();
    // clustering plan has no previous file slice generated by previous pending clustering
    assertFalse(inputFileGroups
        .stream().anyMatch(fg -> fg.getSlices()
            .stream().anyMatch(s -> s.getDataFilePath().contains(firstClusteringInstant))));
  }
}
