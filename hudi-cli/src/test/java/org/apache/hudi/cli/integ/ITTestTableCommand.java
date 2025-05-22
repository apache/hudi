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

package org.apache.hudi.cli.integ;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.cli.commands.CompactionCommand.COMPACTION_SCH_SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link TableCommand#changeTableType}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestTableCommand extends HoodieCLIIntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ITTestTableCommand.class);

  @Autowired
  private Shell shell;
  private String tablePath;
  private String tableName = "test_table";

  @Test
  public void testChangeTableCOW2MOR() throws IOException {
    tablePath = basePath + StoragePath.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTestDataGenerator.createCommitFile(
        tablePath, "100", HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    Object result = shell.evaluate(() -> "table change-table-type --target-type MOR");

    assertEquals(String.class, result.getClass());
    assertTrue(((String) result).matches("(?s).*║ hoodie.table.type +│ COPY_ON_WRITE +│ MERGE_ON_READ +║.*"));
    assertEquals(HoodieTableType.MERGE_ON_READ, HoodieCLI.getTableMetaClient().getTableType());
  }

  @Test
  public void testChangeTableMOR2COW() throws IOException {
    tablePath = basePath + StoragePath.SEPARATOR + tableName + "_mor2cow";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    Object result = shell.evaluate(() -> "table change-table-type --target-type COW");

    assertEquals(String.class, result.getClass());
    assertTrue(((String) result).matches("(?s).*║ hoodie.table.type +│ MERGE_ON_READ +│ COPY_ON_WRITE +║.*"));
    assertEquals(HoodieTableType.COPY_ON_WRITE, HoodieCLI.getTableMetaClient().getTableType());
  }

  @Test
  public void testChangeTableMOR2COW_withPendingCompactions() throws Exception {
    tablePath = basePath + StoragePath.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    generateCommits();
    // schedule a compaction
    Object scheduleResult = shell.evaluate(() -> "compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1 --sparkMaster local");
    assertEquals(String.class, scheduleResult.getClass());
    assertTrue(((String) scheduleResult).startsWith(COMPACTION_SCH_SUCCESSFUL));
    Option<HoodieInstant> lastInstant = HoodieCLI.getTableMetaClient().getActiveTimeline().lastInstant();
    assertTrue(lastInstant.isPresent());
    assertEquals(HoodieTimeline.COMPACTION_ACTION, lastInstant.get().getAction());
    assertTrue(lastInstant.get().isRequested());

    generateCommits();
    Object result = shell.evaluate(() -> "table change-table-type --target-type COW");

    LOG.info("change to cow result \n{}", result);
    assertEquals(String.class, result.getClass());
    assertTrue(((String) result).matches("(?s).*║ hoodie.table.type +│ MERGE_ON_READ +│ COPY_ON_WRITE +║.*"));
    // table change to cow type successfully
    assertEquals(HoodieTableType.COPY_ON_WRITE, HoodieCLI.getTableMetaClient().getTableType());
    lastInstant = HoodieCLI.getTableMetaClient().getActiveTimeline().lastInstant();
    assertTrue(lastInstant.isPresent());
    assertEquals(HoodieTimeline.COMMIT_ACTION, lastInstant.get().getAction());
    assertTrue(lastInstant.get().isCompleted());
  }

  @Test
  public void testChangeTableMOR2COW_withFullCompaction() throws Exception {
    tablePath = basePath + StoragePath.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    generateCommits();
    Object result = shell.evaluate(() -> "table change-table-type --target-type COW");

    LOG.info("change to cow result \n{}", result);
    assertEquals(String.class, result.getClass());
    assertTrue(((String) result).matches("(?s).*║ hoodie.table.type +│ MERGE_ON_READ +│ COPY_ON_WRITE +║.*"));
    // table change to cow type successfully
    assertEquals(HoodieTableType.COPY_ON_WRITE, HoodieCLI.getTableMetaClient().getTableType());
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    // no compaction left
    assertTrue(activeTimeline.filter(instant -> instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)).empty());
    Option<HoodieInstant> lastInstant = activeTimeline.lastInstant();
    assertTrue(lastInstant.isPresent());
    assertEquals(HoodieTimeline.COMMIT_ACTION, lastInstant.get().getAction());
    assertTrue(lastInstant.get().isCompleted());
  }

  @Test
  public void testChangeTableMOR2COW_withoutCompaction() throws Exception {
    tablePath = basePath + StoragePath.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    generateCommits();
    Object result = shell.evaluate(() -> "table change-table-type --target-type COW --enable-compaction false");

    LOG.info("change to cow result \n{}", result);
    assertEquals(HoodieException.class, result.getClass());
    assertTrue(((HoodieException) result).getMessage().startsWith("The last action must be a completed compaction"));
    // table change to cow type failed
    assertEquals(HoodieTableType.MERGE_ON_READ, HoodieCLI.getTableMetaClient().getTableType());
  }

  private void generateCommits() throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // Create the write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();

    try (SparkRDDWriteClient<HoodieAvroPayload> client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg)) {
      String instantTime = client.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 2);
      upsert(jsc, client, records, instantTime);

      instantTime = client.createNewInstantTime();
      List<HoodieRecord> recordsToUpdate = dataGen.generateUpdates(instantTime, 2);
      records.addAll(recordsToUpdate);
      upsert(jsc, client, records, instantTime);
    }
  }

  private void upsert(JavaSparkContext jsc, SparkRDDWriteClient<HoodieAvroPayload> client,
                      List<HoodieRecord> records, String newCommitTime) throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    operateFunc(SparkRDDWriteClient::upsert, client, writeRecords, newCommitTime);
  }

  private void operateFunc(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      SparkRDDWriteClient<HoodieAvroPayload> client, JavaRDD<HoodieRecord> writeRecords, String commitTime)
      throws IOException {
    writeFn.apply(client, writeRecords, commitTime);
  }
}
