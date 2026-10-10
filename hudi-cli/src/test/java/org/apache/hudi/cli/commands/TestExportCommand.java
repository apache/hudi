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

import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchiverV1;
import org.apache.hudi.client.timeline.TimelineArchiverV2;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ExportCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestExportCommand extends CLIFunctionalTestHarness {

  private static final String[] COMMIT_TIMES = new String[] {"101", "102", "103"};
  private static final Set<String> WRITTEN_PARTITIONS = new HashSet<>(
      Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH));

  @Autowired
  private Shell shell;

  private String tablePath;
  private Path exportFolder;

  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = storageConf();
    String tableName = tableName();
    tablePath = tablePath(tableName);
    exportFolder = Files.createDirectories(Paths.get(basePath(), "exported-instants"));

    createTableAndConnect(tablePath, tableName, HoodieTableType.COPY_ON_WRITE, HoodieAvroPayload.class.getName());
    for (String commitTime : COMMIT_TIMES) {
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, commitTime, storageConf());
    }
    HoodieCLI.refreshTableMetadata();
  }

  /**
   * Exports the whole timeline with the default options, which walk the archived timeline first,
   * the LSM timeline history of a table of version eight or above, even when it holds nothing.
   */
  @Test
  public void testExportInstants() throws Exception {
    Object result = shell.evaluate(() -> "export instants --localFolder " + exportFolder);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported " + COMMIT_TIMES.length + " Instants to " + exportFolder, result.toString());

    // one file per completed instant, named after the instant file it was read from
    assertEquals(instantFileNames(COMMIT_TIMES), exportedFiles(exportFolder));

    // The export copies the instant file off the timeline as it stands, in whatever format the
    // table writes its commit metadata in, so it is read back through the table's own serde and
    // compared with what the fixture wrote.
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    for (HoodieInstant instant : metaClient.getActiveTimeline().filterCompletedInstants().getInstants()) {
      Map<String, List<HoodieWriteStat>> writeStats = readExportedCommit(metaClient, instant).getPartitionToWriteStats();
      assertEquals(WRITTEN_PARTITIONS, writeStats.keySet());
      for (List<HoodieWriteStat> partitionStats : writeStats.values()) {
        assertEquals(1, partitionStats.size());
        assertEquals(HoodieTestCommitMetadataGenerator.DEFAULT_NUM_WRITES, partitionStats.get(0).getNumWrites());
        assertEquals(HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT, partitionStats.get(0).getPrevCommit());
      }
    }
  }

  /**
   * The limit caps the number of active instants exported, from the oldest one in the default
   * ascending order and from the latest one in descending order.
   */
  @Test
  public void testExportInstantsWithLimit() throws Exception {
    Path ascending = Files.createDirectories(exportFolder.resolve("ascending"));
    Object result = shell.evaluate(() -> "export instants --limit 2 --localFolder " + ascending);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 2 Instants to " + ascending, result.toString());
    assertEquals(instantFileNames("101", "102"), exportedFiles(ascending));

    Path descending = Files.createDirectories(exportFolder.resolve("descending"));
    result = shell.evaluate(() -> "export instants --limit 2 --desc true --localFolder " + descending);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 2 Instants to " + descending, result.toString());
    assertEquals(instantFileNames("102", "103"), exportedFiles(descending));
  }

  /**
   * A completed restore is one of the actions the default action filter selects, and is exported
   * with its restore metadata.
   */
  @Test
  public void testExportRestoreInstant() throws Exception {
    String restoreTime = "104";
    HoodieInstant restore = addRestore(restoreTime);

    Object result = shell.evaluate(() -> "export instants --localFolder " + exportFolder);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported " + (COMMIT_TIMES.length + 1) + " Instants to " + exportFolder, result.toString());

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    String restoreFileName = metaClient.getInstantFileNameGenerator().getFileName(restore);
    Set<String> expected = instantFileNames(COMMIT_TIMES);
    expected.add(restoreFileName);
    assertEquals(expected, exportedFiles(exportFolder));

    JsonNode restoreMetadata = readJson(exportFolder.resolve(restoreFileName));
    assertEquals(restoreTime, restoreMetadata.get("startRestoreTime").asText());
  }

  /**
   * Exports a table of the current version whose oldest commits were archived to the LSM
   * timeline: the archived instants come from the timeline history and are written as json,
   * named after their requested time and action, ahead of the active ones in ascending order and
   * after them in descending order.
   */
  @Test
  public void testExportArchivedInstants() throws Exception {
    for (int i = 104; i < 109; i++) {
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, String.valueOf(i), storageConf());
    }
    archive(HoodieTableVersion.current());

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    // archival keeps the four newest of the eight commits
    assertEquals(Arrays.asList("101", "102", "103", "104"), archivedInstantTimes(metaClient));

    Object result = shell.evaluate(() -> "export instants --localFolder " + exportFolder);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 8 Instants to " + exportFolder, result.toString());
    Set<String> expected = instantFileNames("105", "106", "107", "108");
    expected.addAll(archivedFileNames("101", "102", "103", "104"));
    assertEquals(expected, exportedFiles(exportFolder));
    for (String archived : Arrays.asList("101", "102", "103", "104")) {
      assertArchivedCommitExported(exportFolder.resolve(archived + "." + HoodieTimeline.COMMIT_ACTION));
    }

    // the oldest instants are archived ones
    Path oldest = Files.createDirectories(exportFolder.resolve("oldest"));
    result = shell.evaluate(() -> "export instants --limit 2 --localFolder " + oldest);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 2 Instants to " + oldest, result.toString());
    assertEquals(archivedFileNames("101", "102"), exportedFiles(oldest));

    // the latest instants run past the active timeline into the latest archived instants
    Path latest = Files.createDirectories(exportFolder.resolve("latest"));
    result = shell.evaluate(() -> "export instants --limit 6 --desc true --localFolder " + latest);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 6 Instants to " + latest, result.toString());
    expected = instantFileNames("105", "106", "107", "108");
    expected.addAll(archivedFileNames("103", "104"));
    assertEquals(expected, exportedFiles(latest));
  }

  /**
   * Exports a table created at version 6, whose archived instants live in the legacy log format
   * under the archive folder. The archive keeps an entry per instant state, and only the
   * completed one of each archived instant is exported and counted against the limit.
   */
  @Test
  public void testExportInstantsOnLegacyArchive() throws Exception {
    String legacyTableName = tableName("_legacy_table");
    String legacyTablePath = tablePath(legacyTableName);
    // createTable also connects the CLI to the new table
    new TableCommand().createTable(
        legacyTablePath, legacyTableName, "COPY_ON_WRITE", "",
        HoodieTableVersion.SIX.versionCode(), HoodieAvroPayload.class.getName());
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    for (int i = 100; i < 106; i++) {
      createLegacyCommit(metaClient, legacyTablePath, String.valueOf(i));
    }
    archive(HoodieTableVersion.SIX);

    metaClient = HoodieCLI.getTableMetaClient();
    // archival keeps the four newest of the six commits
    assertEquals(Arrays.asList("100", "101"), archivedInstantTimes(metaClient));

    Object result = shell.evaluate(() -> "export instants --localFolder " + exportFolder);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 6 Instants to " + exportFolder, result.toString());
    Set<String> expected = instantFileNames("102", "103", "104", "105");
    expected.addAll(archivedFileNames("100", "101"));
    assertEquals(expected, exportedFiles(exportFolder));
    for (String archived : Arrays.asList("100", "101")) {
      assertArchivedCommitExported(exportFolder.resolve(archived + "." + HoodieTimeline.COMMIT_ACTION));
    }

    Path oldest = Files.createDirectories(exportFolder.resolve("oldest"));
    result = shell.evaluate(() -> "export instants --limit 1 --localFolder " + oldest);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 1 Instants to " + oldest, result.toString());
    assertEquals(archivedFileNames("100"), exportedFiles(oldest));

    // both archived instants are in one archive file, the latest of them comes first in descending order
    Path latest = Files.createDirectories(exportFolder.resolve("latest"));
    result = shell.evaluate(() -> "export instants --limit 5 --desc true --localFolder " + latest);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported 5 Instants to " + latest, result.toString());
    expected = instantFileNames("102", "103", "104", "105");
    expected.addAll(archivedFileNames("101"));
    assertEquals(expected, exportedFiles(latest));
  }

  @Test
  public void testExportInstantsToInvalidFolder() {
    String missingFolder = Paths.get(basePath(), "no-such-folder").toString();
    Object result = shell.evaluate(() -> "export instants --localFolder " + missingFolder);
    assertFalse(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(HoodieException.class, result.getClass());
    assertTrue(result.toString().contains(missingFolder + " is not a valid local directory"), result.toString());
    assertFalse(Files.exists(Paths.get(missingFolder)));
  }

  /**
   * Reads back the file the export wrote for one instant.
   *
   * @param metaClient Meta client of the exported table.
   * @param instant    Instant whose exported file to read.
   * @return The commit metadata the file holds.
   */
  private HoodieCommitMetadata readExportedCommit(HoodieTableMetaClient metaClient, HoodieInstant instant)
      throws IOException {
    String fileName = metaClient.getInstantFileNameGenerator().getFileName(instant);
    try (InputStream exported = Files.newInputStream(exportFolder.resolve(fileName))) {
      return metaClient.getCommitMetadataSerDe().deserialize(instant, exported, () -> false, HoodieCommitMetadata.class);
    }
  }

  /**
   * Checks that an archived commit was exported as the json rendering of its Avro commit
   * metadata, with the single write stat the fixture wrote to each partition.
   */
  private static void assertArchivedCommitExported(Path exported) throws IOException {
    // the Avro json encoding wraps the value of the nullable map in its branch name
    JsonNode partitionToWriteStats = readJson(exported).get("partitionToWriteStats").get("map");
    Set<String> partitions = new HashSet<>();
    for (Iterator<String> names = partitionToWriteStats.fieldNames(); names.hasNext(); ) {
      String partition = names.next();
      partitions.add(partition);
      assertEquals(1, partitionToWriteStats.get(partition).size(), exported.toString());
    }
    assertEquals(WRITTEN_PARTITIONS, partitions, exported.toString());
  }

  private static JsonNode readJson(Path file) throws IOException {
    return JsonUtils.getObjectMapper().readTree(Files.readAllBytes(file));
  }

  /**
   * Archives the table the CLI is connected to, keeping its four newest commits active.
   *
   * @param tableVersion Version the table was created at, which picks the archiver.
   */
  private void archive(HoodieTableVersion tableVersion) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(metaClient.getBasePath().toString())
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable(metaClient.getTableConfig().getTableName());
    boolean legacy = tableVersion.lesserThan(HoodieTableVersion.EIGHT);
    if (legacy) {
      builder.withWriteTableVersion(tableVersion.versionCode()).withAutoUpgradeVersion(false);
    }
    HoodieWriteConfig cfg = builder.build();
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = legacy ? new TimelineArchiverV1(cfg, table) : new TimelineArchiverV2(cfg, table);
    archiver.archiveIfRequired(context());
    HoodieCLI.refreshTableMetadata();
  }

  private static List<String> archivedInstantTimes(HoodieTableMetaClient metaClient) {
    return metaClient.getArchivedTimeline(StringUtils.EMPTY_STRING, false).getInstantsAsStream()
        .filter(HoodieInstant::isCompleted)
        .map(HoodieInstant::requestedTime)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Completes a restore at the given instant time on the timeline of the table the CLI is
   * connected to.
   *
   * @param instantTime Requested time of the restore.
   * @return The completed restore instant.
   */
  private HoodieInstant addRestore(String instantTime) throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant requested = metaClient.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.RESTORE_ACTION, instantTime);
    timeline.saveToRestoreRequested(requested, HoodieRestorePlan.newBuilder().build());
    HoodieInstant inflight = timeline.transitionRestoreRequestedToInflight(requested);
    timeline.saveAsComplete(inflight, Option.of(TimelineMetadataUtils.convertRestoreMetadata(
        instantTime, 0L, Collections.emptyList(), Collections.emptyMap())));
    HoodieCLI.refreshTableMetadata();
    return HoodieCLI.getTableMetaClient().getActiveTimeline().getRestoreTimeline().filterCompletedInstants()
        .lastInstant().get();
  }

  /**
   * Writes the requested, the inflight and the completed file of one commit through the meta
   * client of a table created at version 6, the way a writer of that version leaves them.
   */
  private static void createLegacyCommit(HoodieTableMetaClient metaClient, String basePath,
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
  }

  private static Set<String> exportedFiles(Path folder) {
    try (Stream<Path> files = Files.list(folder)) {
      return files.filter(Files::isRegularFile)
          .map(file -> file.getFileName().toString()).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The names of the files the export writes for the given archived commits.
   *
   * @param commitTimes Requested times of the archived commits.
   * @return The exported file names.
   */
  private static Set<String> archivedFileNames(String... commitTimes) {
    return Arrays.stream(commitTimes)
        .map(commitTime -> commitTime + "." + HoodieTimeline.COMMIT_ACTION)
        .collect(Collectors.toSet());
  }

  /**
   * The names of the instant files of the given commits, which are the names the export uses.
   *
   * @param commitTimes Requested times of the commits.
   * @return The instant file names.
   */
  private Set<String> instantFileNames(String... commitTimes) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    List<String> wanted = Arrays.asList(commitTimes);
    return metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(instant -> wanted.contains(instant.requestedTime()))
        .map(instant -> metaClient.getInstantFileNameGenerator().getFileName(instant))
        .collect(Collectors.toSet());
  }
}
