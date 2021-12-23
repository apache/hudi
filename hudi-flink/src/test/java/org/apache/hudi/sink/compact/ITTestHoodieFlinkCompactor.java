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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for {@link org.apache.hudi.common.model.HoodieRecord}.
 */
public class ITTestHoodieFlinkCompactor {

  protected static final Logger LOG = LoggerFactory.getLogger(ITTestHoodieFlinkCompactor.class);

  private static final Map<String, List<String>> EXPECTED1 = new HashMap<>();

  private static final Map<String, List<String>> EXPECTED2 = new HashMap<>();

  static {
    EXPECTED1.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED1.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED1.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED1.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    EXPECTED2.put("par1", Arrays.asList("id1,par1,id1,Danny,24,1000,par1", "id2,par1,id2,Stephen,34,2000,par1"));
    EXPECTED2.put("par2", Arrays.asList("id3,par2,id3,Julian,54,3000,par2", "id4,par2,id4,Fabian,32,4000,par2"));
    EXPECTED2.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3", "id9,par3,id9,Jane,19,6000,par3"));
    EXPECTED2.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4", "id10,par4,id10,Ella,38,7000,par4", "id11,par4,id11,Phoebe,52,8000,par4"));
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieFlinkCompactor(boolean enableChangelog) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
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
    conf.setString(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");

    // create metaClient
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

    // set the table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // infer changelog mode
    CompactionUtil.inferChangelogMode(conf, metaClient);

    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);

    boolean scheduled = false;
    // judge whether have operation
    // To compute the compaction instant time and do compaction.
    Option<String> compactionInstantTimeOption = CompactionUtil.getCompactionInstantTime(metaClient);
    if (compactionInstantTimeOption.isPresent()) {
      scheduled = writeClient.scheduleCompactionAtInstant(compactionInstantTimeOption.get(), Option.empty());
    }

    String compactionInstantTime = compactionInstantTimeOption.get();

    assertTrue(scheduled, "The compaction plan should be scheduled");

    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    // Mark instant as compaction inflight
    table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

    env.addSource(new CompactionPlanSourceFunction(compactionPlan, compactionInstantTime))
        .name("compaction_source")
        .uid("uid_compaction_source")
        .rebalance()
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(conf)))
        .setParallelism(compactionPlan.getOperations().size())
        .addSink(new CompactionCommitSink(conf))
        .name("clean_commits")
        .uid("uid_clean_commits")
        .setParallelism(1);

    env.execute("flink_hudi_compaction");
    writeClient.close();
    TestData.checkWrittenFullData(tempFile, EXPECTED1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieFlinkCompactorService(boolean enableChangelog) throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    options.put(FlinkOptions.CHANGELOG_ENABLED.key(), enableChangelog + "");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(5);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.minCompactionIntervalSeconds = 3;
    cfg.schedule = true;
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.setString(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");

    HoodieFlinkCompactor.AsyncCompactionService asyncCompactionService = new HoodieFlinkCompactor.AsyncCompactionService(cfg, conf, env);
    asyncCompactionService.start(null);

    tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(5);

    asyncCompactionService.shutDown();

    TestData.checkWrittenFullData(tempFile, EXPECTED2);
  }
}
