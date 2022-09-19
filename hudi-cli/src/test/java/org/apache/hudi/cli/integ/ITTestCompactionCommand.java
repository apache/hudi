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
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.CompactionAdminClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.TestCompactionAdminClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.CompactionCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestCompactionCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;
  @BeforeEach
  public void init() throws IOException {
    tableName = "test_table_" + ITTestCompactionCommand.class.getName();
    basePath = Paths.get(basePath, tableName).toString();

    HoodieCLI.conf = jsc.hadoopConfiguration();
    // Create table and connect
    new TableCommand().createTable(
        basePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    initMetaClient();
  }

  /**
   * Test case for command 'compaction schedule'.
   */
  @Test
  public void testScheduleCompact() throws IOException {
    // generate commits
    generateCommits();

    Object result = shell.evaluate(() ->
            String.format("compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1 --sparkMaster %s",
            "local"));
    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(
                result.toString().startsWith("Attempted to schedule compaction for")));

    // there is 1 requested compaction
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.filterPendingCompactionTimeline().countInstants());
  }

  /**
   * Test case for command 'compaction run'.
   */
  @Test
  public void testCompact() throws IOException {
    // generate commits
    generateCommits();

    String instance = prepareScheduleCompaction();

    String schemaPath = Paths.get(basePath, "compaction.schema").toString();
    writeSchemaToTmpFile(schemaPath);

    Object result2 = shell.evaluate(() ->
            String.format("compaction run --parallelism %s --schemaFilePath %s --sparkMaster %s",
            2, schemaPath, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result2)),
        () -> assertTrue(
            result2.toString().startsWith("Compaction successfully completed for")));

    // assert compaction complete
    assertTrue(HoodieCLI.getTableMetaClient().getActiveTimeline().reload()
        .filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList()).contains(instance),
        "Pending compaction must be completed");
  }

  /**
   * Test case for command 'compaction scheduleAndExecute'.
   */
  @Test
  public void testCompactScheduleAndExecute() throws IOException {
    // generate commits
    generateCommits();

    String schemaPath = Paths.get(basePath, "compaction.schema").toString();
    writeSchemaToTmpFile(schemaPath);

    Object result = shell.evaluate(() ->
            String.format("compaction scheduleAndExecute --parallelism %s --schemaFilePath %s --sparkMaster %s "
                    + "--hoodieConfigs hoodie.compact.inline.max.delta.commits=1",
            2, schemaPath, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(
                result.toString().startsWith("Schedule and execute compaction successfully completed")));

    // assert compaction complete
    assertTrue(HoodieCLI.getTableMetaClient().getActiveTimeline().reload()
            .filterCompletedInstants().getInstants()
            .map(HoodieInstant::getTimestamp).count() > 0,
        "Completed compaction couldn't be 0");
  }

  /**
   * Test case for command 'compaction validate'.
   */
  @Test
  public void testValidateCompaction() throws IOException {
    // generate commits
    generateCommits();

    String instance = prepareScheduleCompaction();

    Object result = shell.evaluate(() ->
            String.format("compaction validate --instant %s --sparkMaster %s", instance, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(
            // compaction requested should be valid
            result.toString().contains("COMPACTION PLAN VALID")));
  }

  /**
   * This function mainly tests the workflow of 'compaction unschedule' command.
   * The real test of {@link org.apache.hudi.client.CompactionAdminClient#unscheduleCompactionPlan}
   * is {@link TestCompactionAdminClient#testUnscheduleCompactionPlan()}.
   */
  @Test
  public void testUnscheduleCompaction() throws Exception {
    // generate commits
    generateCommits();

    String instance = prepareScheduleCompaction();

    Object result = shell.evaluate(() ->
            String.format("compaction unschedule --instant %s --sparkMaster %s", instance, "local"));

    // Always has no file
    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertEquals("No File renames needed to unschedule pending compaction. Operation successful.",
           result.toString()));
  }

  /**
   * This function mainly tests the workflow of 'compaction unscheduleFileId' command.
   * The real test of {@link org.apache.hudi.client.CompactionAdminClient#unscheduleCompactionFileId}
   * is {@link TestCompactionAdminClient#testUnscheduleCompactionFileId}.
   */
  @Test
  public void testUnscheduleCompactFile() throws IOException {
    int numEntriesPerInstant = 10;
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);

    CompactionOperation op = CompactionOperation.convertFromAvroRecordInstance(
        CompactionUtils.getCompactionPlan(metaClient, "001").getOperations().stream().findFirst().get());

    Object result = shell.evaluate(() ->
            String.format("compaction unscheduleFileId --fileId %s --partitionPath %s --sparkMaster %s",
            op.getFileGroupId().getFileId(), op.getFileGroupId().getPartitionPath(), "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(removeNonWordAndStripSpace(result.toString()).contains("true")),
        () -> assertFalse(removeNonWordAndStripSpace(result.toString()).contains("false")));
  }

  /**
   * This function mainly tests the workflow of 'compaction repair' command.
   * The real test of {@link org.apache.hudi.client.CompactionAdminClient#repairCompaction}
   * is {@link TestCompactionAdminClient#testRepairCompactionPlan}.
   */
  @Test
  public void testRepairCompaction() throws Exception {
    int numEntriesPerInstant = 10;
    String compactionInstant = "001";
    CompactionTestUtils.setupAndValidateCompactionOperations(metaClient, false, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant, numEntriesPerInstant);

    metaClient.reloadActiveTimeline();
    CompactionAdminClient client = new CompactionAdminClient(new HoodieSparkEngineContext(jsc), metaClient.getBasePath());
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        client.getRenamingActionsForUnschedulingCompactionPlan(metaClient, compactionInstant, 1, Option.empty(), false);

    renameFiles.forEach(lfPair -> {
      try {
        metaClient.getFs().rename(lfPair.getLeft().getPath(), lfPair.getRight().getPath());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });

    client.unscheduleCompactionPlan(compactionInstant, false, 1, false);

    Object result = shell.evaluate(() ->
            String.format("compaction repair --instant %s --sparkMaster %s", compactionInstant, "local"));

    // All Executes is succeeded, result contains true and has no false
    // Expected:
    // ║ File Id │ Source File Path │ Destination File Path │ Rename Executed? │ Rename Succeeded? │ Error ║
    // ║ *       │     *            │        *              │    true          │     true          │       ║
    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(removeNonWordAndStripSpace(result.toString()).contains("true")),
        () -> assertFalse(removeNonWordAndStripSpace(result.toString()).contains("false")));
  }

  private String prepareScheduleCompaction() {
    // generate requested compaction
    Object result = shell.evaluate(() ->
            String.format("compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1 --sparkMaster %s",
            "local"));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get compaction instance
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Option<String> instance =
        timeline.filterPendingCompactionTimeline().firstInstant().map(HoodieInstant::getTimestamp);
    assertTrue(instance.isPresent(), "Must have pending compaction.");
    return instance.get();
  }

  private void writeSchemaToTmpFile(String schemaPath) throws IOException {
    try (BufferedWriter out = new BufferedWriter(new FileWriter(schemaPath))) {
      out.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    }
  }

  private void generateCommits() throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // Create the write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2).forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();

    SparkRDDWriteClient<HoodieAvroPayload> client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg);

    List<HoodieRecord> records = insert(jsc, client, dataGen);
    upsert(jsc, client, dataGen, records);
    delete(jsc, client, records);
  }

  private List<HoodieRecord> insert(JavaSparkContext jsc, SparkRDDWriteClient<HoodieAvroPayload> client,
      HoodieTestDataGenerator dataGen) throws IOException {
    // inserts
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    operateFunc(SparkRDDWriteClient::insert, client, writeRecords, newCommitTime);
    return records;
  }

  private void upsert(JavaSparkContext jsc, SparkRDDWriteClient<HoodieAvroPayload> client,
      HoodieTestDataGenerator dataGen, List<HoodieRecord> records)
      throws IOException {
    // updates
    String newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
    records.addAll(toBeUpdated);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    operateFunc(SparkRDDWriteClient::upsert, client, writeRecords, newCommitTime);
  }

  private void delete(JavaSparkContext jsc, SparkRDDWriteClient<HoodieAvroPayload> client,
       List<HoodieRecord> records) {
    // Delete
    String newCommitTime = "003";
    client.startCommitWithTime(newCommitTime);

    // just delete half of the records
    int numToDelete = records.size() / 2;
    List<HoodieKey> toBeDeleted = records.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
    JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(toBeDeleted, 1);
    client.delete(deleteRecords, newCommitTime);
  }

  private JavaRDD<WriteStatus> operateFunc(
      HoodieClientTestBase.Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      SparkRDDWriteClient<HoodieAvroPayload> client, JavaRDD<HoodieRecord> writeRecords, String commitTime)
      throws IOException {
    return writeFn.apply(client, writeRecords, commitTime);
  }
}
