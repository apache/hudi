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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.HoodieTestCommitUtilities;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchiverV1;
import org.apache.hudi.client.timeline.TimelineArchiverV2;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link ArchivedCommitsCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestArchivedCommitsCommand extends CLIFunctionalTestHarness {

  /**
   * The clean and the replace commit that the fixture archives along with the plain commits.
   * The clean has to be older than the newest archived write instant for TimelineArchiverV2 to
   * pick it up, and its earliest instant to retain has to be past the replace commit so that the
   * clustering guard of the archiver does not hold the replace commit back.
   */
  private static final String CLEAN_INSTANT = "101";
  private static final String REPLACE_COMMIT_INSTANT = "102";
  private static final String EARLIEST_COMMIT_TO_RETAIN = "103";

  @Autowired
  private Shell shell;

  private String tablePath;
  private HoodieCleanMetadata cleanMetadata;
  private HoodieReplaceCommitMetadata replaceCommitMetadata;

  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = storageConf();

    // Create table and connect
    String tableName = tableName();
    tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", HoodieTableVersion.current().versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable("test-trip-table").build();

    // Create seven write instants (a commit, a replace commit and five more commits) plus one
    // clean, so that archival keeps the four newest write instants and archives 100, 101, 102
    // and 103
    HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, "100", storageConf());
    cleanMetadata = addClean(metaClient, CLEAN_INSTANT, EARLIEST_COMMIT_TO_RETAIN);
    replaceCommitMetadata = addReplaceCommit(metaClient, REPLACE_COMMIT_INSTANT);
    for (int i = 103; i < 108; i++) {
      String timestamp = String.valueOf(i);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, storageConf());
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // reload the timeline and get all the commits before archive
    metaClient.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();

    // archive
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new TimelineArchiverV2(cfg, table);
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

    // Generate expected data: the stats only cover the archived commit and delta commit
    // instants, so the archived clean and replace commit contribute no row. The archived
    // timeline holds one entry per instant, with one write stat for each of the two
    // partitions, ordered by partition path
    final List<Comparable[]> rows = new ArrayList<>();
    for (String instant : Arrays.asList("100", "103")) {
      rows.addAll(writeStatRows("commit", instant));
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
    Object cmdResult = shell.evaluate(() -> "show archived commits --limit 5");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));
    final List<Comparable[]> rows = new ArrayList<>();

    // Test default skipMetadata and limit 5. The archived timeline holds one entry
    // per instant, so each instant shows up as a single row
    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType");
    rows.add(new Comparable[] {"100", "commit"});
    rows.add(new Comparable[] {CLEAN_INSTANT, "clean"});
    rows.add(new Comparable[] {REPLACE_COMMIT_INSTANT, "replacecommit"});
    rows.add(new Comparable[] {"103", "commit"});
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, 5, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cmdResult.toString());
    assertEquals(expected, got);

    // Test with Metadata and no limit
    cmdResult = shell.evaluate(() -> "show archived commits --skipMetadata false --limit 0");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));

    rows.clear();

    // Since HoodiePrintHelper order data by default, need to order commitMetadata
    rows.add(new Comparable[] {"100", "commit", commitDetails("100")});
    rows.add(new Comparable[] {CLEAN_INSTANT, "clean", cleanMetadata});
    rows.add(new Comparable[] {REPLACE_COMMIT_INSTANT, "replacecommit", orderedAvroReplaceCommitMetadata()});
    rows.add(new Comparable[] {"103", "commit", commitDetails("103")});
    header = header.addTableHeaderField("CommitDetails");
    expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, 0, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    got = removeNonWordAndStripSpace(cmdResult.toString());
    assertEquals(expected, got);
  }

  /**
   * Test for both show archived commands against a table created at version 6, whose archived
   * instants live in the legacy log format under the archive folder instead of in an LSM
   * timeline.
   */
  @Test
  public void testShowArchivedCommitsOnLegacyArchive() throws Exception {
    String legacyTableName = tableName("_legacy_table");
    String legacyTablePath = tablePath(legacyTableName);

    // createTable also connects the CLI to the new table
    new TableCommand().createTable(
        legacyTablePath, legacyTableName, "COPY_ON_WRITE", "",
        HoodieTableVersion.SIX.versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(legacyTablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .withAutoUpgradeVersion(false)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable(legacyTableName).build();

    // Six commits, of which archival keeps the four newest and archives 100 and 101
    List<HoodieCommitMetadata> commitMetadata = new ArrayList<>();
    for (int i = 100; i < 106; i++) {
      commitMetadata.add(createLegacyCommit(metaClient, legacyTablePath, String.valueOf(i)));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new TimelineArchiverV1(cfg, table);
    archiver.archiveIfRequired(context());

    TableHeader statsHeader = new TableHeader().addTableHeaderField("action").addTableHeaderField("instant")
        .addTableHeaderField("partition").addTableHeaderField("file_id").addTableHeaderField("prev_instant")
        .addTableHeaderField("num_writes").addTableHeaderField("num_inserts").addTableHeaderField("num_deletes")
        .addTableHeaderField("num_update_writes").addTableHeaderField("total_log_files")
        .addTableHeaderField("total_log_blocks").addTableHeaderField("total_corrupt_log_blocks")
        .addTableHeaderField("total_rollback_blocks").addTableHeaderField("total_log_records")
        .addTableHeaderField("total_updated_records_compacted").addTableHeaderField("total_write_bytes")
        .addTableHeaderField("total_write_errors");

    // The pre table version 8 archive keeps one entry per instant state. The requested entry
    // of a commit carries no metadata and the inflight one only the in-progress stats, so the
    // legacy stats reader renders the completed entry of each archived instant
    final List<Comparable[]> statsRows = new ArrayList<>();
    for (String instant : Arrays.asList("100", "101")) {
      statsRows.addAll(writeStatRows("commit", instant));
    }
    String expectedStats = removeNonWordAndStripSpace(
        HoodiePrintHelper.print(statsHeader, new HashMap<>(), "", false, 0, false, statsRows));

    Object result = shell.evaluate(() -> "show archived commit stats");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(expectedStats, removeNonWordAndStripSpace(result.toString()));

    // the explicit folder pattern reads the very same archive files
    result = shell.evaluate(() -> "show archived commit stats --archiveFolderPattern archived/.commits_.archive*");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(expectedStats, removeNonWordAndStripSpace(result.toString()));

    // show archived commits reads the same archive through ArchivedTimelineV1, which keeps only
    // the completed entry of each instant and caches its metadata as the archived JSON rendering
    TableHeader header = new TableHeader().addTableHeaderField("CommitTime").addTableHeaderField("CommitType")
        .addTableHeaderField("CommitDetails");
    final List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[] {"100", "commit",
        HoodieTestCommitUtilities.convertAndOrderCommitMetadata(commitMetadata.get(0))});
    rows.add(new Comparable[] {"101", "commit",
        HoodieTestCommitUtilities.convertAndOrderCommitMetadata(commitMetadata.get(1))});
    String expected = removeNonWordAndStripSpace(
        HoodiePrintHelper.print(header, new HashMap<>(), "", false, 0, false, rows));

    result = shell.evaluate(() -> "show archived commits --skipMetadata false");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(expected, removeNonWordAndStripSpace(result.toString()));
  }

  /**
   * The metadata of one of the plain commits of the fixture, rendered the way the command
   * renders it, with the partition keyed map ordered by partition path.
   */
  private Comparable commitDetails(String instantTime) throws Exception {
    return HoodieTestCommitUtilities.convertAndOrderCommitMetadata(
        HoodieTestCommitMetadataGenerator.generateCommitMetadata(tablePath, instantTime));
  }

  /**
   * The archived replace commit rendered the way the command renders it, with both partition
   * keyed maps ordered by partition path.
   */
  private org.apache.hudi.avro.model.HoodieReplaceCommitMetadata orderedAvroReplaceCommitMetadata() {
    org.apache.hudi.avro.model.HoodieReplaceCommitMetadata avroMetadata =
        MetadataConversionUtils.convertCommitMetadataToAvro(replaceCommitMetadata);
    avroMetadata.setPartitionToWriteStats(new TreeMap<>(avroMetadata.getPartitionToWriteStats()));
    avroMetadata.setPartitionToReplaceFileIds(new TreeMap<>(avroMetadata.getPartitionToReplaceFileIds()));
    return avroMetadata;
  }

  /**
   * The stat rows of a commit written by {@link HoodieTestCommitMetadataGenerator}, ordered by
   * partition path.
   */
  private static List<Comparable[]> writeStatRows(String action, String instant) {
    return Arrays.asList(
        writeStatRow(action, instant, DEFAULT_SECOND_PARTITION_PATH),
        writeStatRow(action, instant, DEFAULT_FIRST_PARTITION_PATH));
  }

  private static Comparable[] writeStatRow(String action, String instant, String partitionPath) {
    return new Comparable[] {action, instant, partitionPath,
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
  }

  /**
   * Writes a completed clean whose plan retains from {@code earliestCommitToRetain}.
   */
  private static HoodieCleanMetadata addClean(HoodieTableMetaClient metaClient, String instantTime,
                                              String earliestCommitToRetain) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(
        new HoodieActionInstant(earliestCommitToRetain, HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()),
        "", HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(), new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
    HoodieCleanStat cleanStat = HoodieCleanStat.builder()
        .withPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
        .withPartitionPath(DEFAULT_FIRST_PARTITION_PATH)
        .withEarliestCommitToRetain(earliestCommitToRetain)
        .withLastCompletedCommitTimestamp("")
        .build();
    HoodieCleanMetadata metadata = convertCleanMetadata(
        instantTime, Option.of(0L), Collections.singletonList(cleanStat), Collections.emptyMap());
    HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, metadata);
    return metadata;
  }

  /**
   * Writes a completed insert overwrite replace commit over the two default partitions.
   */
  private static HoodieReplaceCommitMetadata addReplaceCommit(HoodieTableMetaClient metaClient,
                                                              String instantTime) throws Exception {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    metadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    for (String partitionPath : Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH)) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partitionPath);
      writeStat.setPath(HoodieTestCommitMetadataGenerator.DEFAULT_PATH);
      writeStat.setFileId(HoodieTestCommitMetadataGenerator.DEFAULT_FILEID);
      writeStat.setTotalWriteBytes(HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES);
      writeStat.setPrevCommit(HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT);
      writeStat.setNumWrites(HoodieTestCommitMetadataGenerator.DEFAULT_NUM_WRITES);
      writeStat.setNumUpdateWrites(HoodieTestCommitMetadataGenerator.DEFAULT_NUM_UPDATE_WRITES);
      writeStat.setTotalLogBlocks(HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_BLOCKS);
      writeStat.setTotalLogRecords(HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_RECORDS);
      metadata.addWriteStat(partitionPath, writeStat);
      metadata.addReplaceFileId(partitionPath, HoodieTestCommitMetadataGenerator.DEFAULT_FILEID);
    }
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.INSERT_OVERWRITE.name())
        .setVersion(1)
        .setExtraMetadata(Collections.emptyMap())
        .build();
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, Option.of(requestedReplaceMetadata), Option.empty(), metadata);
    return metadata;
  }

  /**
   * Writes the requested, the inflight and the completed file of one commit through the meta
   * client of the table, so that the files follow the layout of the table version and look like
   * what a writer leaves behind: an empty requested file, and the commit metadata in the inflight
   * and the completed file.
   */
  private static HoodieCommitMetadata createLegacyCommit(HoodieTableMetaClient metaClient, String basePath,
                                                         String instantTime) throws Exception {
    HoodieCommitMetadata metadata = HoodieTestCommitMetadataGenerator.generateCommitMetadata(basePath, instantTime);
    // the archiver drops the commit metadata of an entry whose operation type is UNKNOWN
    metadata.setOperationType(WriteOperationType.INSERT);
    metaClient.getStorage().create(new StoragePath(metaClient.getTimelinePath(),
        metaClient.getInstantFileNameGenerator().makeRequestedCommitFileName(instantTime)), true).close();
    List<String> fileNames = Arrays.asList(
        metaClient.getInstantFileNameGenerator().makeInflightCommitFileName(instantTime),
        metaClient.getInstantFileNameGenerator().makeCommitFileName(instantTime));
    for (String fileName : fileNames) {
      StoragePath path = new StoragePath(metaClient.getTimelinePath(), fileName);
      try (OutputStream os = metaClient.getStorage().create(path, true)) {
        metaClient.getCommitMetadataSerDe().getInstantWriter(metadata).get().writeToStream(os);
      }
    }
    return metadata;
  }
}
