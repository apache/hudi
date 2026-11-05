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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.client.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchiverV2;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link CompactionCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestCompactionCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private static final String PENDING_COMPACTION_INSTANT = "001";

  private String tableName;
  private String tablePath;

  @BeforeEach
  public void init() {
    tableName = tableName();
    tablePath = tablePath(tableName);
  }

  @Test
  public void testVerifyTableType() throws IOException {
    // create COW table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", HoodieTableVersion.current().versionCode(), HoodieAvroPayload.class.getName());

    // expect HoodieException for COPY_ON_WRITE table.
    assertThrows(HoodieException.class,
        () -> new CompactionCommand().compactionsAll(false, -1, "", false, false));
  }

  /**
   * Test case for command 'compactions show all'.
   */
  @Test
  public void testCompactionsAll() throws IOException {
    // create MOR table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), false, 3, 4, 3, 3);

    HoodieCLI.getTableMetaClient().reloadActiveTimeline();

    Object result = shell.evaluate(() -> "compactions show all");
    assertNotNull(result);

    TableHeader header = new TableHeader().addTableHeaderField("Compaction Instant Time").addTableHeaderField("State")
        .addTableHeaderField("Total FileIds to be Compacted");
    Map<String, Integer> fileIds = new HashMap<>();
    fileIds.put("001", 3);
    fileIds.put("003", 4);
    fileIds.put("005", 3);
    fileIds.put("007", 3);
    List<Comparable[]> rows = new ArrayList<>();
    Arrays.asList("001", "003", "005", "007").stream().sorted(Comparator.reverseOrder()).forEach(instant -> {
      rows.add(new Comparable[] {instant, "REQUESTED", fileIds.get(instant)});
    });
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    assertEquals(expected, result.toString());
  }

  /**
   * Test case for command 'compaction show'.
   */
  @Test
  public void testCompactionShow() throws IOException {
    // create MOR table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), false, 3, 4, 3, 3);

    HoodieCLI.getTableMetaClient().reloadActiveTimeline();

    Object result = shell.evaluate(() -> "compaction show --instant 001");
    assertNotNull(result);
  }

  /**
   * Test case of the compaction validation entry point of {@link SparkMain}, which the
   * 'compaction validate' command reaches through a spark-submit of its own.
   */
  @Test
  public void testSparkMainCompactValidate() throws Exception {
    createPendingCompactions();
    String outputPath = outputPath("validate");

    SparkMain.doCompactValidate(jsc(), tablePath, PENDING_COMPACTION_INSTANT, outputPath, 2);

    List<ValidationOpResult> results = readOperationResults(outputPath);
    assertEquals(operationsOf(PENDING_COMPACTION_INSTANT).size(), results.size());
    assertTrue(results.stream().allMatch(ValidationOpResult::isSuccess), results.toString());
    assertEquals(fileIdsOf(PENDING_COMPACTION_INSTANT),
        results.stream().map(result -> result.getOperation().getFileId()).collect(Collectors.toSet()));
  }

  @Test
  public void testSparkMainCompactValidateReportsMissingLogFile() throws Exception {
    createPendingCompactions();
    HoodieCompactionOperation broken = operationsOf(PENDING_COMPACTION_INSTANT).get(0);
    // a log file the plan reads is gone, so that operation can no longer be compacted
    Files.delete(Paths.get(tablePath, broken.getPartitionPath(), broken.getDeltaFilePaths().get(0)));
    String outputPath = outputPath("validate-broken");

    SparkMain.doCompactValidate(jsc(), tablePath, PENDING_COMPACTION_INSTANT, outputPath, 2);

    List<ValidationOpResult> results = readOperationResults(outputPath);
    assertEquals(operationsOf(PENDING_COMPACTION_INSTANT).size(), results.size());
    List<ValidationOpResult> failed = results.stream().filter(result -> !result.isSuccess()).collect(Collectors.toList());
    assertEquals(1, failed.size(), results.toString());
    assertEquals(broken.getFileId(), failed.get(0).getOperation().getFileId());
    assertTrue(failed.get(0).getException().isPresent());
  }

  /**
   * Repair runs the plan validation and returns an empty result: the log file renaming it was
   * written for is gone from the admin client, which leaves the plan untouched and never reads
   * the dry run flag, so there is only one arm to exercise. See
   * https://github.com/apache/hudi/issues/19881.
   */
  @Test
  public void testSparkMainCompactRepair() throws Exception {
    createPendingCompactions();
    Set<String> fileIdsBefore = fileIdsOf(PENDING_COMPACTION_INSTANT);
    String outputPath = outputPath("repair");

    SparkMain.doCompactRepair(jsc(), tablePath, PENDING_COMPACTION_INSTANT, outputPath, 2, false);

    assertTrue(readOperationResults(outputPath).isEmpty());
    assertTrue(pendingCompactionInstants().contains(PENDING_COMPACTION_INSTANT));
    assertEquals(fileIdsBefore, fileIdsOf(PENDING_COMPACTION_INSTANT));
  }

  /**
   * Unscheduling a plan takes the requested compaction instant off the timeline, unless this is a
   * dry run. The other pending plans are left alone either way. Skip validation is held at false:
   * the admin client takes the flag but never reads it, so toggling it repeats the same run.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSparkMainCompactUnschedulePlan(boolean dryRun) throws Exception {
    createPendingCompactions();
    Set<String> pendingBefore = pendingCompactionInstants();
    String outputPath = outputPath("unschedule-" + dryRun);

    SparkMain.doCompactUnschedule(jsc(), tablePath, PENDING_COMPACTION_INSTANT, outputPath, 2, false, dryRun);

    assertTrue(readOperationResults(outputPath).isEmpty());
    Set<String> pendingAfter = pendingCompactionInstants();
    if (dryRun) {
      assertEquals(pendingBefore, pendingAfter);
    } else {
      assertFalse(pendingAfter.contains(PENDING_COMPACTION_INSTANT), pendingAfter.toString());
      pendingBefore.remove(PENDING_COMPACTION_INSTANT);
      assertEquals(pendingBefore, pendingAfter);
    }
  }

  /**
   * Unscheduling a single file group rewrites the plan without it, unless this is a dry run. Skip
   * validation is held at false: the admin client takes the flag but never reads it, so toggling
   * it repeats the same run.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSparkMainCompactUnscheduleFile(boolean dryRun) throws Exception {
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> pendingOperations = createPendingCompactions();
    HoodieFileGroupId unscheduled = pendingOperations.entrySet().stream()
        .filter(entry -> entry.getValue().getKey().equals(PENDING_COMPACTION_INSTANT))
        .map(Map.Entry::getKey).findFirst().get();
    String outputPath = outputPath("unschedule-file-" + dryRun);
    // both operations of this plan sit in the same partition, so there is a sibling to keep
    Set<String> fileIdsBefore = fileIdsOf(PENDING_COMPACTION_INSTANT);
    assertEquals(2, fileIdsBefore.size());
    assertTrue(fileIdsBefore.contains(unscheduled.getFileId()));

    SparkMain.doCompactUnscheduleFile(jsc(), tablePath, unscheduled.getFileId(), unscheduled.getPartitionPath(),
        outputPath, 2, false, dryRun);

    assertTrue(readOperationResults(outputPath).isEmpty());
    // the plan itself stays pending either way, only its operations change
    assertTrue(pendingCompactionInstants().contains(PENDING_COMPACTION_INSTANT));
    if (dryRun) {
      assertEquals(fileIdsBefore, fileIdsOf(PENDING_COMPACTION_INSTANT));
    } else {
      // The admin client keeps the operations that differ from the unscheduled one in file id AND
      // in partition path, so the sibling operation goes with it and the plan is left with no
      // operations at all (https://github.com/apache/hudi/issues/19881). When that is fixed this
      // expectation has to become the sibling on its own:
      // fileIdsBefore minus the unscheduled file id.
      assertEquals(Collections.emptySet(), fileIdsOf(PENDING_COMPACTION_INSTANT));
    }
  }

  /**
   * A MOR table with four pending compaction plans, of which {@link #PENDING_COMPACTION_INSTANT}
   * holds more than one operation.
   *
   * @return The pending compaction operations, by file group.
   */
  private Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> createPendingCompactions() throws IOException {
    createTableAndConnect(tablePath, tableName, HoodieTableType.MERGE_ON_READ, HoodieAvroPayload.class.getName());
    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> operations =
        CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), false, 2, 1, 1, 1);
    HoodieCLI.getTableMetaClient().reloadActiveTimeline();
    return operations;
  }

  private String outputPath(String name) {
    return Paths.get(basePath(), "compaction-admin-" + name).toString();
  }

  private Set<String> pendingCompactionInstants() {
    return HoodieCLI.getTableMetaClient().reloadActiveTimeline().filterPendingCompactionTimeline()
        .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
  }

  private List<HoodieCompactionOperation> operationsOf(String compactionInstant) throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    metaClient.reloadActiveTimeline();
    return CompactionUtils.getCompactionPlan(metaClient, compactionInstant).getOperations();
  }

  private Set<String> fileIdsOf(String compactionInstant) throws IOException {
    return operationsOf(compactionInstant).stream()
        .map(HoodieCompactionOperation::getFileId).collect(Collectors.toSet());
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> readOperationResults(String outputPath) throws Exception {
    try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(Paths.get(outputPath)))) {
      return (List<T>) in.readObject();
    }
  }

  /**
   * Test case of the compaction entry point of {@link SparkMain}, which the 'compaction run' and
   * 'compaction scheduleAndExecute' commands reach through a spark-submit of their own.
   */
  @Test
  public void testSparkMainCompact() throws Exception {
    writeDeltaCommits();
    assertEquals(0, HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient())
        .getActiveTimeline().filterPendingCompactionTimeline().countInstants());

    int returnCode = SparkMain.compact(jsc(), tablePath, tableName, null, 1, "", 0,
        UtilHelpers.SCHEDULE_AND_EXECUTE, null,
        Collections.singletonList("hoodie.compact.inline.max.delta.commits=1"));

    assertEquals(0, returnCode);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    // the plan it scheduled ran to completion, leaving a commit and nothing pending
    assertEquals(0, metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants());
    assertEquals(1, metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(instant -> COMMIT_ACTION.equals(instant.getAction())).countInstants());
  }

  /**
   * Writes two delta commits into a new MOR table at {@link #tablePath}, the second one updating
   * the records of the first so that the file groups have log files to compact.
   */
  private void writeDeltaCommits() throws IOException {
    createTableAndConnect(tablePath, tableName, HoodieTableType.MERGE_ON_READ, HoodieAvroPayload.class.getName());

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(1, 1).forTable(tableName).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      String firstCommit = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(firstCommit, 10);
      writeAndCommit(client, records, firstCommit);

      String secondCommit = client.startCommit();
      writeAndCommit(client, dataGen.generateUpdates(secondCommit, 5), secondCommit);
    }
  }

  private void writeAndCommit(SparkRDDWriteClient client, List<HoodieRecord> records, String commitTime) {
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> result = client.upsert(writeRecords, commitTime).collect();
    client.commit(commitTime, jsc().parallelize(result));
    Assertions.assertNoWriteErrors(result);
  }

  private void generateCompactionInstances() throws IOException {
    // create MOR table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", HoodieTableVersion.current().versionCode(), HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), true, 1, 2, 3, 4);

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().reloadActiveTimeline();
    // Create six commits
    Arrays.asList("001", "003", "005", "007").forEach(timestamp -> {
      activeTimeline.transitionCompactionInflightToComplete(true,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, timestamp), new HoodieCommitMetadata());
    });
    // Simulate a compaction commit in metadata table timeline
    // so the archival in data table can happen
    HoodieTestUtils.createCompactionCommitInMetadataTable(storageConf(), tablePath, "007");
  }

  private void generateArchive() throws IOException {
    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .forTable("test-trip-table").build();
    // archive
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new TimelineArchiverV2(cfg, table);
    archiver.archiveIfRequired(context());
  }

  /**
   * Test case for command 'compactions showarchived'.
   */
  @Test
  public void testCompactionsShowArchived() throws IOException {
    generateCompactionInstances();

    generateArchive();

    Object result = shell.evaluate(() -> "compactions showarchived --startTs 001 --endTs 005");

    // generate result
    Map<String, Integer> fileMap = new HashMap<>();
    fileMap.put("001", 1);
    fileMap.put("003", 2);
    fileMap.put("005", 3);
    List<Comparable[]> rows = Arrays.asList("005", "003", "001").stream().map(i ->
        new Comparable[] {i, HoodieInstant.State.COMPLETED, fileMap.get(i)}).collect(Collectors.toList());
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader().addTableHeaderField("Compaction Instant Time").addTableHeaderField("State")
        .addTableHeaderField("Total FileIds to be Compacted");
    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);

    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for command 'compaction showarchived'.
   */
  @Test
  public void testCompactionShowArchived() throws IOException {
    generateCompactionInstances();

    String instance = "001";
    // get compaction plan before compaction
    HoodieCompactionPlan plan = HoodieCLI.getTableMetaClient().reloadActiveTimeline().readCompactionPlan(
            INSTANT_GENERATOR.getCompactionRequestedInstant(instance));

    generateArchive();

    Object result = shell.evaluate(() -> "compaction showarchived --instant " + instance);

    // generate expected
    String expected = CompactionCommand.printCompaction(plan, "", false, -1, false, null);

    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }
}
