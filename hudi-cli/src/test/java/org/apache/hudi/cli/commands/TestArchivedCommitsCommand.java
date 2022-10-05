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

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.HoodieTestCommitUtilities;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link ArchivedCommitsCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestArchivedCommitsCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String tablePath;

  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = hadoopConf();

    // Create table and connect
    String tableName = tableName();
    tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .forTable("test-trip-table").build();

    // Create six commits
    for (int i = 100; i < 106; i++) {
      String timestamp = String.valueOf(i);
      // Requested Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp), hadoopConf());
      // Inflight Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp), hadoopConf());
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, hadoopConf());
    }

    // Simulate a compaction commit in metadata table timeline
    // so the archival in data table can happen
    HoodieTestUtils.createCompactionCommitInMetadataTable(
        hadoopConf(), metaClient.getFs(), tablePath, "105");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // reload the timeline and get all the commits before archive
    metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();

    // archive
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    archiver.archiveIfRequired(context());
  }

  /**
   * Test for command: show archived commit stats.
   */
  @Test
  public void testShowArchivedCommits() {
    Object result = shell.evaluate(() -> "show archived commit stats");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    TableHeader header = new TableHeader().addTableHeaderField("action").addTableHeaderField("instant")
        .addTableHeaderField("partition").addTableHeaderField("file_id").addTableHeaderField("prev_instant")
        .addTableHeaderField("num_writes").addTableHeaderField("num_inserts").addTableHeaderField("num_deletes")
        .addTableHeaderField("num_update_writes").addTableHeaderField("total_log_files")
        .addTableHeaderField("total_log_blocks").addTableHeaderField("total_corrupt_log_blocks")
        .addTableHeaderField("total_rollback_blocks").addTableHeaderField("total_log_records")
        .addTableHeaderField("total_updated_records_compacted").addTableHeaderField("total_write_bytes")
        .addTableHeaderField("total_write_errors");

    // Generate expected data
    final List<Comparable[]> rows = new ArrayList<>();
    for (int i = 100; i < 104; i++) {
      String instant = String.valueOf(i);
      for (int j = 0; j < 3; j++) {
        Comparable[] defaultComp = new Comparable[] {"commit", instant,
            HoodieTestCommitMetadataGenerator.DEFAULT_SECOND_PARTITION_PATH,
            HoodieTestCommitMetadataGenerator.DEFAULT_FILEID,
            HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT,
            HoodieTestCommitMetadataGenerator.DEFAULT_NUM_WRITES,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_NUM_UPDATE_WRITES,
            HoodieTestCommitMetadataGenerator.DEFAULT_NULL_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_BLOCKS,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_RECORDS,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE,
            HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
            HoodieTestCommitMetadataGenerator.DEFAULT_OTHER_VALUE};
        rows.add(defaultComp.clone());
        defaultComp[2] = HoodieTestCommitMetadataGenerator.DEFAULT_FIRST_PARTITION_PATH;
        rows.add(defaultComp);
      }
    }

    String expectedResult = HoodiePrintHelper.print(
        header, new HashMap<>(), "", false, -1, false, rows);
    expectedResult = removeNonWordAndStripSpace(expectedResult);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expectedResult, got);
  }

  /**
   * Test for command: show archived commits.
   */
  @Test
  public void testShowCommits() throws Exception {
    Object cmdResult = shell.evaluate(() -> "show archived commits");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));
    final List<Comparable[]> rows = new ArrayList<>();

    // Test default skipMetadata and limit 10
    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType");
    for (int i = 100; i < 103; i++) {
      String instant = String.valueOf(i);
      Comparable[] result = new Comparable[] {instant, "commit"};
      rows.add(result);
      rows.add(result);
      rows.add(result);
    }
    rows.add(new Comparable[] {"103", "commit"});
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, 10, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cmdResult.toString());
    assertEquals(expected, got);

    // Test with Metadata and no limit
    cmdResult = shell.evaluate(() -> "show archived commits --skipMetadata false --limit 0");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));

    rows.clear();

    for (int i = 100; i < 104; i++) {
      String instant = String.valueOf(i);
      // Since HoodiePrintHelper order data by default, need to order commitMetadata
      HoodieCommitMetadata metadata = HoodieTestCommitMetadataGenerator.generateCommitMetadata(tablePath, instant);
      Comparable[] result = new Comparable[] {
          instant, "commit", HoodieTestCommitUtilities.convertAndOrderCommitMetadata(metadata)};
      rows.add(result);
      rows.add(result);
      rows.add(result);
    }
    header = header.addTableHeaderField("CommitDetails");
    expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, 0, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    got = removeNonWordAndStripSpace(cmdResult.toString());
    assertEquals(expected, got);
  }
}
