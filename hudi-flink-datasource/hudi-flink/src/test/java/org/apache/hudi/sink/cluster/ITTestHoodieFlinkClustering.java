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

import org.apache.hudi.adapter.SinkFunctionAdapter;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
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
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());

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
    cfg.sortMemory = 256;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);
    assertEquals(256, conf.get(FlinkOptions.WRITE_SORT_MEMORY));

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.
    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();

      Option<String> clusteringInstantTime = writeClient.scheduleClustering(Option.empty());

      assertTrue(clusteringInstantTime.isPresent(), "The clustering plan should be scheduled");

      // fetch the instant based on the configured execution sequence
      table.getMetaClient().reloadActiveTimeline();
      HoodieTimeline timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);

      // generate clustering plan
      // should support configurable commit metadata
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          table.getMetaClient(), timeline.lastInstant().get());

      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

      // Mark instant as clustering inflight
      HoodieInstant instant = INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstantTime.get());
      table.getActiveTimeline().transitionClusterRequestedToInflight(instant, Option.empty());

      final HoodieSchema tableSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
      final DataType rowDataType = HoodieSchemaConverter.convertToDataType(tableSchema);
      final RowType rowType = (RowType) rowDataType.getLogicalType();

      DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstantTime.get(), clusteringPlan, conf))
          .name("clustering_source")
          .uid("uid_clustering_source")
          .rebalance()
          .transform("clustering_task",
              TypeInformation.of(ClusteringCommitEvent.class),
              new ClusteringOperator(conf, rowType))
          .setParallelism(clusteringPlan.getInputGroups().size());

      ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      dataStream
          .addSink(new ClusteringCommitSink(conf))
          .name("clustering_commit")
          .uid("uid_clustering_commit")
          .setParallelism(1);

      env.execute("flink_hudi_clustering");
      TestData.checkWrittenData(tempFile, EXPECTED, 4);
    }
  }

  @Test
  public void testHoodieFlinkClusteringService() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());

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
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    conf.set(FlinkOptions.CLUSTERING_DELTA_COMMITS, 2);
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      // To compute the clustering instant time.
      Option<String> clusteringInstantTime = writeClient.scheduleClustering(Option.empty());

      assertFalse(clusteringInstantTime.isPresent(), "1 delta commit, the clustering plan should not be scheduled");

      tableEnv.executeSql(TestSQL.INSERT_T1).await();
      // wait for the asynchronous commit to finish
      TimeUnit.SECONDS.sleep(3);

      clusteringInstantTime = writeClient.scheduleClustering(Option.empty());

      assertTrue(clusteringInstantTime.isPresent(), "2 delta commits, the clustering plan should be scheduled");
    }
  }

  @Test
  public void testHoodieFlinkClusteringScheduleAfterArchive() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());

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
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set archive commits
    conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, 2);
    conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, 1);
    conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 0);

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.

    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      Option<String> firstClusteringInstant = writeClient.scheduleClustering(Option.empty());

      assertTrue(firstClusteringInstant.isPresent(), "The clustering plan should be scheduled");

      // fetch the instant based on the configured execution sequence
      table.getMetaClient().reloadActiveTimeline();
      HoodieTimeline timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(i -> i.getState() == HoodieInstant.State.REQUESTED);

      // generate clustering plan
      // should support configurable commit metadata
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          table.getMetaClient(), timeline.lastInstant().get());

      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

      // Mark instant as clustering inflight
      HoodieInstant instant = INSTANT_GENERATOR.getClusteringCommitRequestedInstant(firstClusteringInstant.get());
      table.getActiveTimeline().transitionClusterRequestedToInflight(instant, Option.empty());

      final HoodieSchema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
      final DataType rowDataType = HoodieSchemaConverter.convertToDataType(tableAvroSchema);
      final RowType rowType = (RowType) rowDataType.getLogicalType();

      DataStream<ClusteringCommitEvent> dataStream =
          env.addSource(new ClusteringPlanSourceFunction(firstClusteringInstant.get(), clusteringPlan, conf))
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
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      // keep pending clustering, not committing clustering
      dataStream
          .addSink(new DiscardingSink<>())
          .name("discarding-sink")
          .uid("uid_discarding-sink")
          .setParallelism(1);

      env.execute("flink_hudi_clustering");

      tableEnv.executeSql(TestSQL.INSERT_T1).await();
      // wait for the asynchronous commit to finish
      TimeUnit.SECONDS.sleep(3);

      // archive the first commit, retain the second commit before the inflight cluster commit
      writeClient.archive();

      assertTrue(writeClient.scheduleClustering(Option.empty()).isPresent(), "The clustering plan should be scheduled");
      table.getMetaClient().reloadActiveTimeline();
      timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(i -> i.getState() == HoodieInstant.State.REQUESTED);

      HoodieInstant secondClusteringInstant = timeline.lastInstant().get();
      List<HoodieClusteringGroup> inputFileGroups = ClusteringUtils.getClusteringPlan(table.getMetaClient(), secondClusteringInstant).get().getRight().getInputGroups();
      // clustering plan has no previous file slice generated by previous pending clustering
      assertFalse(inputFileGroups
          .stream().anyMatch(fg -> fg.getSlices()
              .stream().anyMatch(s -> s.getDataFilePath().contains(firstClusteringInstant.get()))));
    }
  }

  /**
   * Test to ensure that creating a table with a column of TIMESTAMP(9) will throw errors
   * @throws Exception
   */
  @Test
  public void testHoodieFlinkClusteringWithTimestampNanos() {
    // create hoodie table and insert into data
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");

    // row schema
    final DataType dataType = DataTypes.ROW(
            DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
            DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
            DataTypes.FIELD("age", DataTypes.INT()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(9)), // precombine field
            DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
        .notNull();

    final RowType rowType = (RowType) dataType.getLogicalType();
    final List<String> fields = rowType.getFields().stream()
        .map(RowType.RowField::asSummaryString).collect(Collectors.toList());

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL(
        "t1", fields, options, true, "uuid", "partition");
    TableResult tableResult = tableEnv.executeSql(hoodieTableDDL);

    // insert rows with timestamp of microseconds precision; timestamp(6)
    final String insertSql = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01.100001001','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02.100001001','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03.100001001','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04.100001001','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05.100001001','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06.100001001','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07.100001001','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08.100001001','par4')";

    assertThrows(ValidationException.class, () -> tableEnv.executeSql(insertSql),
        "Avro does not support TIMESTAMP type with precision: 9, it only support precisions <= 6.");
  }

  @Test
  public void testHoodieFlinkClusteringWithTimestampMicros() throws Exception {
    // create hoodie table and insert into data
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());

    // row schema
    final DataType dataType = DataTypes.ROW(
            DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
            DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
            DataTypes.FIELD("age", DataTypes.INT()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)), // precombine field
            DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
        .notNull();
    final RowType rowType = (RowType) dataType.getLogicalType();
    final List<String> fields = rowType.getFields().stream()
        .map(RowType.RowField::asSummaryString).collect(Collectors.toList());

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL(
        "t1", fields, options, true, "uuid", "partition");
    tableEnv.executeSql(hoodieTableDDL);

    // insert rows with timestamp of microseconds precision; timestamp(6)
    final String insertSql = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01.100001','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02.100001','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03.100001','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04.100001','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05.100001','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06.100001','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07.100001','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08.100001','par4')";
    tableEnv.executeSql(insertSql).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    runCluster(rowType);

    // test output
    final Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1100001,par1, id2,par1,id2,Stephen,33,2100001,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3100001,par2, id4,par2,id4,Fabian,31,4100001,par2]");
    expected.put("par3", "[id5,par3,id5,Sophia,18,5100001,par3, id6,par3,id6,Emma,20,6100001,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7100001,par4, id8,par4,id8,Han,56,8100001,par4]");
    TestData.checkWrittenData(tempFile, expected, 4);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInsertWithDifferentRecordKeyNullabilityAndClustering(boolean withPk) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

    // if create a table without primary key, the nullability of the record key field is nullable
    // otherwise, the nullability is not nullable.
    String pkConstraint = withPk ? ",  primary key (uuid) not enforced\n" : "";
    String tblWithoutPkDDL = "create table t1(\n"
        + "  `uuid` VARCHAR(20)\n"
        + ",  `name` VARCHAR(10)\n"
        + ",  `age` INT\n"
        + ",  `ts` TIMESTAMP(3)\n"
        + ",  `partition` VARCHAR(10)\n"
        + pkConstraint
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'hoodie.datasource.write.recordkey.field' = 'uuid',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "'\n"
        + ")";
    tableEnv.executeSql(tblWithoutPkDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    final RowType rowType = (RowType) DataTypes.ROW(
            DataTypes.FIELD("uuid", DataTypes.VARCHAR(20).notNull()), // primary key set as not null
            DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
            DataTypes.FIELD("age", DataTypes.INT()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
        .notNull().getLogicalType();

    // run cluster with row type
    runCluster(rowType);

    final Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1000,par1, id2,par1,id2,Stephen,33,2000,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3000,par2, id4,par2,id4,Fabian,31,4000,par2]");
    expected.put("par3", "[id5,par3,id5,Sophia,18,5000,par3, id6,par3,id6,Emma,20,6000,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7000,par4, id8,par4,id8,Han,56,8000,par4]");
    TestData.checkWrittenData(tempFile, expected, 4);
  }

  @Test
  public void testOfflineClusterFailoverAfterCommit() throws Exception {
    StreamTableEnvironment tableEnv = prepareEnvAndTable();

    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.targetPartitions = 4;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);
    assertDoesNotThrow(() -> runOfflineCluster(tableEnv, conf));

    Table result = tableEnv.sqlQuery("select count(*) from t1");
    assertEquals(16L, tableEnv.toDataStream(result, Row.class).executeAndCollect(1).get(0).getField(0));
  }

  /**
   * schedule clustering, run clustering.
   */
  private void runCluster(RowType rowType) throws Exception {
    // make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.targetPartitions = 4;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      Option<String> clusteringInstantTime = writeClient.scheduleClustering(Option.empty());

      assertTrue(clusteringInstantTime.isPresent(), "The clustering plan should be scheduled");

      // fetch the instant based on the configured execution sequence
      table.getMetaClient().reloadActiveTimeline();
      HoodieTimeline timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);

      // generate clustering plan
      // should support configurable commit metadata
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          table.getMetaClient(), timeline.lastInstant().get());

      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

      // Mark instant as clustering inflight
      HoodieInstant instant = INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstantTime.get());
      table.getActiveTimeline().transitionClusterRequestedToInflight(instant, Option.empty());

      DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstantTime.get(), clusteringPlan, conf))
          .name("clustering_source")
          .uid("uid_clustering_source")
          .rebalance()
          .transform("clustering_task",
              TypeInformation.of(ClusteringCommitEvent.class),
              new ClusteringOperator(conf, rowType))
          .setParallelism(clusteringPlan.getInputGroups().size());

      ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      dataStream
          .addSink(new ClusteringCommitSink(conf))
          .name("clustering_commit")
          .uid("uid_clustering_commit")
          .setParallelism(1);

      env.execute("flink_hudi_clustering");
    }
  }

  private StreamTableEnvironment prepareEnvAndTable() {
    // Create hoodie table and insert into data.
    Configuration conf = new org.apache.flink.configuration.Configuration();
    conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DML_SYNC, true);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());

    // use append mode
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.INSERT_CLUSTER.key(), "false");
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tEnv.executeSql(hoodieTableDDL);
    tEnv.executeSql(TestSQL.INSERT_T1);
    return tEnv;
  }

  /**
   * schedule clustering, insert another batch, run clustering.
   */
  private void runOfflineCluster(TableEnvironment tableEnv, Configuration conf) throws Exception {
    // Make configuration and setAvroSchema.
    Configuration envConf = new Configuration();
    envConf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    envConf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    envConf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(1));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(envConf);

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set record key field
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    // set partition field
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, metaClient.getTableConfig().getPartitionFieldProp());

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the clustering instant time and do clustering.

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      Option<String> clusteringInstantTime = writeClient.scheduleClustering(Option.empty());

      assertTrue(clusteringInstantTime.isPresent(), "The clustering plan should be scheduled");

      tableEnv.executeSql(TestSQL.INSERT_T1);

      // fetch the instant based on the configured execution sequence
      table.getMetaClient().reloadActiveTimeline();
      HoodieTimeline timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);

      // generate clustering plan
      // should support configurable commit metadata
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          table.getMetaClient(), timeline.lastInstant().get());

      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

      // Mark instant as clustering inflight
      HoodieInstant instant = INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstantTime.get());
      table.getActiveTimeline().transitionClusterRequestedToInflight(instant, Option.empty());

      final HoodieSchema tableAvroSchema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
      final DataType rowDataType = HoodieSchemaConverter.convertToDataType(tableAvroSchema);
      final RowType rowType = (RowType) rowDataType.getLogicalType();

      DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstantTime.get(), clusteringPlan, conf))
          .name("clustering_source")
          .uid("uid_clustering_source")
          .rebalance()
          .transform("clustering_task",
              TypeInformation.of(ClusteringCommitEvent.class),
              new ClusteringOperator(conf, rowType))
          .setParallelism(clusteringPlan.getInputGroups().size());

      ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      dataStream
          .addSink(new ClusteringCommitTestSink(conf))
          .name("clustering_commit")
          .uid("uid_clustering_commit")
          .setParallelism(1);

      env.execute("flink_hudi_clustering");
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private static final class DiscardingSink<T> implements SinkFunctionAdapter<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(T value) {
      // do nothing
    }
  }
}
