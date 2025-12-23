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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for {@link org.apache.hudi.common.model.HoodieRecord}.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestHoodieFlinkCompactor {

  protected static final Logger LOG = LoggerFactory.getLogger(ITTestHoodieFlinkCompactor.class);

  private static final Map<String, List<String>> EXPECTED1 = new HashMap<>();

  private static final Map<String, List<String>> EXPECTED2 = new HashMap<>();

  private static final Map<String, List<String>> EXPECTED3 = new HashMap<>();

  static {
    EXPECTED1.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED1.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED1.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED1.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    EXPECTED2.put("par1", Arrays.asList("id1,par1,id1,Danny,24,1000,par1", "id2,par1,id2,Stephen,34,2000,par1"));
    EXPECTED2.put("par2", Arrays.asList("id3,par2,id3,Julian,54,3000,par2", "id4,par2,id4,Fabian,32,4000,par2"));
    EXPECTED2.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3", "id9,par3,id9,Jane,19,6000,par3"));
    EXPECTED2.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4", "id10,par4,id10,Ella,38,7000,par4", "id11,par4,id11,Phoebe,52,8000,par4"));

    EXPECTED3.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED3.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED3.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED3.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));
    EXPECTED3.put("par5", Arrays.asList("id12,par5,id12,Tony,27,9000,par5", "id13,par5,id13,Jenny,72,10000,par5"));
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @MethodSource("changedlogAndLogBlockParams")
  public void testHoodieFlinkCompactor(boolean enableChangelog, String logBlockFormat) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "false");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    options.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logBlockFormat);
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), enableChangelog + "");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set the partition fields
    CompactionUtil.setPartitionField(conf, metaClient);

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // infer changelog mode
    CompactionUtil.inferChangelogMode(conf, metaClient);

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();

      String compactionInstantTime = scheduleCompactionPlan(writeClient);

      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime);

      HoodieInstant instant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

      env.addSource(new CompactionPlanSourceFunction(Collections.singletonList(Pair.of(compactionInstantTime, compactionPlan)), conf))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(FlinkMiniCluster.DEFAULT_PARALLELISM)
          .addSink(new CompactionCommitSink(conf))
          .name("compaction_commit")
          .uid("uid_compaction_commit")
          .setParallelism(1);

      env.execute("flink_hudi_compaction");
      TestData.checkWrittenDataCOW(tempFile, EXPECTED1);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHoodieFlinkCompactorWithUpgradeAndDowngrade(boolean upgrade) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "false");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // infer changelog mode
    CompactionUtil.inferChangelogMode(conf, metaClient);

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {

      String compactionInstantTime = scheduleCompactionPlan(writeClient);

      // try to upgrade or downgrade
      if (upgrade) {
        metaClient.getTableConfig().setTableVersion(HoodieTableVersion.SIX);
        HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
        new UpgradeDowngrade(metaClient, writeClient.getConfig(), writeClient.getEngineContext(),
            FlinkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.current(), "none");
      } else {
        metaClient.getTableConfig().setTableVersion(HoodieTableVersion.current());
        new UpgradeDowngrade(metaClient, writeClient.getConfig(), writeClient.getEngineContext(),
            FlinkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.SIX, "none");
        // set table version
        conf.setString("hoodie.write.table.version", "6");
      }
      // Refresh the meta client
      metaClient.reloadTableConfig();
      metaClient.reloadActiveTimeline();

      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          metaClient, compactionInstantTime);

      HoodieInstant instant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

      conf.set(FlinkOptions.WRITE_TABLE_VERSION, upgrade ? HoodieTableVersion.EIGHT.versionCode() : HoodieTableVersion.SIX.versionCode());
      env.addSource(new CompactionPlanSourceFunction(Collections.singletonList(Pair.of(compactionInstantTime, compactionPlan)), conf))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(FlinkMiniCluster.DEFAULT_PARALLELISM)
          .addSink(new CompactionCommitSink(conf))
          .name("compaction_commit")
          .uid("uid_compaction_commit")
          .setParallelism(1);

      env.execute("flink_hudi_compaction");
      TestData.checkWrittenDataCOW(tempFile, EXPECTED1);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieFlinkCompactorService(boolean enableChangelog) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), enableChangelog + "");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);

    // insert dataset
    tableEnv.executeSql(TestSQL.INSERT_T1).await();
    // update the dataset
    tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await();

    // Make configuration and setAvroSchema.
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.minCompactionIntervalSeconds = 3;
    cfg.schedule = true;
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
    conf.set(FlinkOptions.COMPACTION_TASKS, FlinkMiniCluster.DEFAULT_PARALLELISM);

    HoodieFlinkCompactor.AsyncCompactionService asyncCompactionService = new HoodieFlinkCompactor.AsyncCompactionService(cfg, conf);
    asyncCompactionService.start(null);

    TestUtils.waitUntil(() -> TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath(), HoodieTimeline.COMMIT_ACTION) != null, 20);
    asyncCompactionService.shutDown();

    TestData.checkWrittenDataCOW(tempFile, EXPECTED2);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieFlinkCompactorWithPlanSelectStrategy(boolean enableChangelog) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), enableChangelog + "");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
    CompactionUtil.setAvroSchema(conf, metaClient);
    CompactionUtil.inferChangelogMode(conf, metaClient);

    List<String> compactionInstantTimeList = new ArrayList<>(2);

    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf);

    compactionInstantTimeList.add(scheduleCompactionPlan(writeClient));

    // insert a new record to new partition, so that we can generate a new compaction plan
    String insertT1ForNewPartition = "insert into t1 values\n"
        + "('id12','Tony',27,TIMESTAMP '1970-01-01 00:00:09','par5'),\n"
        + "('id13','Jenny',72,TIMESTAMP '1970-01-01 00:00:10','par5')";
    tableEnv.executeSql(insertT1ForNewPartition).await();

    writeClient.close();
    // re-create the write client/fs view server
    // or there is low probability that connection refused occurs then
    // the reader metadata view is not complete
    writeClient = FlinkWriteClients.createWriteClient(conf);

    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    compactionInstantTimeList.add(scheduleCompactionPlan(writeClient));

    List<Pair<String, HoodieCompactionPlan>> compactionPlans = new ArrayList<>(2);
    for (String compactionInstantTime : compactionInstantTimeList) {
      HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(table.getMetaClient(), compactionInstantTime);
      compactionPlans.add(Pair.of(compactionInstantTime, plan));
    }

    // Mark instant as compaction inflight
    for (String compactionInstantTime : compactionInstantTimeList) {
      HoodieInstant hoodieInstant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime);
      table.getActiveTimeline().transitionCompactionRequestedToInflight(hoodieInstant);
    }
    table.getMetaClient().reloadActiveTimeline();

    env.addSource(new CompactionPlanSourceFunction(compactionPlans, conf))
        .name("compaction_source")
        .uid("uid_compaction_source")
        .rebalance()
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new CompactOperator(conf))
        .setParallelism(1)
        .addSink(new CompactionCommitSink(conf))
        .name("compaction_commit")
        .uid("uid_compaction_commit")
        .setParallelism(1);

    env.execute("flink_hudi_compaction");
    writeClient.close();
    TestData.checkWrittenDataCOW(tempFile, EXPECTED3);
  }

  @Test
  public void testCompactionInBatchExecutionMode() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "2");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();
    tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await();
    TestData.checkWrittenDataCOW(tempFile, EXPECTED2);
  }

  @Test
  public void testOfflineCompactFailoverAfterCommit() {
    TableEnvironment tableEnv = prepareEnvAndTable();

    tableEnv.executeSql(TestSQL.INSERT_T1);

    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    assertDoesNotThrow(() -> runOfflineCompact(tableEnv, conf));
    assertNoDuplicateFile(conf);
  }

  private void assertNoDuplicateFile(Configuration conf) {
    Set<Pair<String, String>> fileIdCommitTimeSet = new HashSet<>();
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    HoodieStorage storage = metaClient.getStorage();
    FSUtils.getAllPartitionPaths(HoodieFlinkEngineContext.DEFAULT, metaClient, false).forEach(
        partition -> {
          try {
            storage.listDirectEntries(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partition))
                .stream()
                .filter(f -> FSUtils.isBaseFile(f.getPath()))
                .forEach(f -> {
                  HoodieBaseFile baseFile = new HoodieBaseFile(f);
                  assertFalse(fileIdCommitTimeSet.contains(
                      Pair.of(baseFile.getFileId(), baseFile.getCommitTime())));
                  fileIdCommitTimeSet.add(Pair.of(baseFile.getFileId(), baseFile.getCommitTime()));
                });
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    assertFalse(fileIdCommitTimeSet.isEmpty());
  }

  private TableEnvironment prepareEnvAndTable() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    tableEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DML_SYNC, true);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.METADATA_ENABLED.key(), "false"); // to archive compaction instant
    options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "false");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    return tableEnv;
  }

  /**
   * schedule compact, insert another batch, run compact.
   */
  private void runOfflineCompact(TableEnvironment tableEnv, Configuration conf) throws Exception {
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // infer changelog mode
    CompactionUtil.inferChangelogMode(conf, metaClient);

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();

      String compactionInstantTime = scheduleCompactionPlan(writeClient);

      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime);

      HoodieInstant instant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

      tableEnv.executeSql(TestSQL.INSERT_T1);

      // Make configuration and setAvroSchema.
      Configuration envConf = new Configuration();
      envConf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      envConf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      envConf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(1));
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(envConf);

      env.addSource(new CompactionPlanSourceFunction(Collections.singletonList(Pair.of(compactionInstantTime, compactionPlan)), conf))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(1)
          .addSink(new CompactionCommitTestSink(conf))
          .name("compaction_commit")
          .uid("uid_compaction_commit")
          .setParallelism(1);

      env.execute("flink_hudi_compaction");
    }
  }

  private String scheduleCompactionPlan(HoodieFlinkWriteClient<?> writeClient) {
    Option<String> compactionInstant = writeClient.scheduleCompaction(Option.empty());
    assertTrue(compactionInstant.isPresent(), "The compaction plan should be scheduled");
    return compactionInstant.get();
  }

  /**
   * Return test params => (enableChangelog, log block format).
   */
  private static Stream<Arguments> changedlogAndLogBlockParams() {
    Object[][] data =
        new Object[][] {
            {true, "parquet"},
            {true, "avro"},
            {false, "parquet"},
            {false, "avro"}};
    return Stream.of(data).map(Arguments::of);
  }
}
