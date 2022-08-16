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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.client.HoodieTimelineArchiver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test Cases for {@link CompactionCommand}.
 */
@Tag("functional")
public class TestCompactionCommand extends CLIFunctionalTestHarness {

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
        "", TimelineLayoutVersion.VERSION_1, HoodieAvroPayload.class.getName());

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
        "", TimelineLayoutVersion.VERSION_1, HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), false, 3, 4, 3, 3);

    HoodieCLI.getTableMetaClient().reloadActiveTimeline();

    CommandResult cr = shell().executeCommand("compactions show all");
    System.out.println(cr.getResult().toString());

    TableHeader header = new TableHeader().addTableHeaderField("Compaction Instant Time").addTableHeaderField("State")
        .addTableHeaderField("Total FileIds to be Compacted");
    Map<String, Integer> fileIds = new HashMap();
    fileIds.put("001", 3);
    fileIds.put("003", 4);
    fileIds.put("005", 3);
    fileIds.put("007", 3);
    List<Comparable[]> rows = new ArrayList<>();
    Arrays.asList("001", "003", "005", "007").stream().sorted(Comparator.reverseOrder()).forEach(instant -> {
      rows.add(new Comparable[] {instant, "REQUESTED", fileIds.get(instant)});
    });
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    assertEquals(expected, cr.getResult().toString());
  }

  /**
   * Test case for command 'compaction show'.
   */
  @Test
  public void testCompactionShow() throws IOException {
    // create MOR table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), false, 3, 4, 3, 3);

    HoodieCLI.getTableMetaClient().reloadActiveTimeline();

    CommandResult cr = shell().executeCommand("compaction show --instant 001");
    System.out.println(cr.getResult().toString());
  }

  private void generateCompactionInstances() throws IOException {
    // create MOR table.
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, HoodieAvroPayload.class.getName());

    CompactionTestUtils.setupAndValidateCompactionOperations(HoodieCLI.getTableMetaClient(), true, 1, 2, 3, 4);

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().reloadActiveTimeline();
    // Create six commits
    Arrays.asList("001", "003", "005", "007").forEach(timestamp -> {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, timestamp), Option.empty());
    });
    // Simulate a compaction commit in metadata table timeline
    // so the archival in data table can happen
    HoodieTestUtils.createCompactionCommitInMetadataTable(hadoopConf(),
        new HoodieWrapperFileSystem(
            FSUtils.getFs(tablePath, hadoopConf()), new NoOpConsistencyGuard()), tablePath, "007");
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
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    archiver.archiveIfRequired(context());
  }

  /**
   * Test case for command 'compactions showarchived'.
   */
  @Test
  public void testCompactionsShowArchived() throws IOException {
    generateCompactionInstances();

    generateArchive();

    CommandResult cr = shell().executeCommand("compactions showarchived --startTs 001 --endTs 005");

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
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
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
    HoodieCompactionPlan plan = TimelineMetadataUtils.deserializeCompactionPlan(
        HoodieCLI.getTableMetaClient().reloadActiveTimeline().readCompactionPlanAsBytes(
            HoodieTimeline.getCompactionRequestedInstant(instance)).get());

    generateArchive();

    CommandResult cr = shell().executeCommand("compaction showarchived --instant " + instance);

    // generate expected
    String expected = new CompactionCommand().printCompaction(plan, "", false, -1, false, null);

    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }
}
