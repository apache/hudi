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
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimeGeneratorType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link TableCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestTableCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String tableName;
  private String tablePath;
  private String metaPath;
  private String archivePath;

  /**
   * Init path after Mini hdfs init.
   */
  @BeforeEach
  public void init() {
    HoodieCLI.conf = storageConf();
    tableName = tableName();
    tablePath = tablePath(tableName);
    metaPath = Paths.get(tablePath, METAFOLDER_NAME).toString();
    archivePath = Paths.get(metaPath, HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue()).toString();
  }

  /**
   * Method to create a table for connect or desc.
   */
  private boolean prepareTable() {
    Object result = shell.evaluate(() -> "create --path " + tablePath + " --tableName " + tableName);
    return ShellEvaluationResultUtil.isSuccess(result);
  }

  /**
   * Test Case for connect table.
   */
  @Test
  public void testConnectTable() {
    // Prepare table
    assertTrue(prepareTable());

    // Test connect with specified values
    Object result = shell.evaluate(() -> "connect --path " + tablePath + " --initialCheckIntervalMs 3000 "
            + "--maxWaitIntervalMs 40000 --maxCheckIntervalMs 8 --maxExpectedClockSkewMs 888 --useDefaultLockProvider true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // Check specified values
    ConsistencyGuardConfig conf = HoodieCLI.consistencyGuardConfig;
    assertEquals(3000, conf.getInitialConsistencyCheckIntervalMs());
    assertEquals(40000, conf.getMaxConsistencyCheckIntervalMs());
    assertEquals(8, conf.getMaxConsistencyChecks());
    HoodieTimeGeneratorConfig timeGeneratorConfig = HoodieCLI.timeGeneratorConfig;
    assertEquals(tablePath, timeGeneratorConfig.getBasePath());
    assertEquals(888L, timeGeneratorConfig.getMaxExpectedClockSkewMs());
    assertEquals("org.apache.hudi.core.transaction.lock.InProcessLockProvider",
        timeGeneratorConfig.getLockConfiguration().getConfig().getString(HoodieTimeGeneratorConfig.LOCK_PROVIDER_KEY));

    // Check default values
    assertFalse(conf.isConsistencyCheckEnabled());
    assertEquals(TimeGeneratorType.valueOf("WAIT_TO_ADJUST_SKEW"), timeGeneratorConfig.getTimeGeneratorType());
  }

  /**
   * Test Cases for create table with default values.
   */
  @Test
  public void testDefaultCreate() {
    // Create table
    assertTrue(prepareTable());

    // Test meta
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(archivePath, client.getArchivePath());
    assertEquals(tablePath, client.getBasePath().toString());
    assertEquals(metaPath, client.getMetaPath().toString());
    assertEquals(HoodieTableType.COPY_ON_WRITE, client.getTableType());
    assertEquals(new Integer(1), client.getTimelineLayoutVersion().getVersion());

    HoodieTimeGeneratorConfig timeGeneratorConfig = HoodieCLI.timeGeneratorConfig;
    assertEquals(tablePath, timeGeneratorConfig.getBasePath());
    assertEquals(200L, timeGeneratorConfig.getMaxExpectedClockSkewMs());
    assertEquals("org.apache.hudi.core.transaction.lock.InProcessLockProvider",
        timeGeneratorConfig.getLockConfiguration().getConfig().getString(HoodieTimeGeneratorConfig.LOCK_PROVIDER_KEY));
    assertEquals(TimeGeneratorType.valueOf("WAIT_TO_ADJUST_SKEW"), timeGeneratorConfig.getTimeGeneratorType());
  }

  /**
   * Test Cases for create table with specified values.
   */
  @Test
  public void testCreateWithSpecifiedValues() {
    // Test create with specified values
    Object result = shell.evaluate(() -> "create --path " + tablePath + " --tableName " + tableName
            + " --tableType MERGE_ON_READ --archiveLogFolder archive --tableVersion 6");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals("Metadata for table " + tableName + " loaded", result.toString());
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(new StoragePath(metaPath, "archive"), client.getArchivePath());
    assertEquals(tablePath, client.getBasePath().toString());
    assertEquals(metaPath, client.getMetaPath().toString());
    assertEquals(HoodieTableVersion.SIX, client.getTableConfig().getTableVersion());
    assertEquals(HoodieTableType.MERGE_ON_READ, client.getTableType());
  }

  /**
   * Test Case for desc table.
   */
  @Test
  public void testDescTable() {
    // Prepare table
    assertTrue(prepareTable());

    // Test desc table
    Object result = shell.evaluate(() -> "desc");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // check table's basePath metaPath and type
    assertTrue(result.toString().contains(tablePath));
    assertTrue(result.toString().contains(metaPath));
    assertTrue(result.toString().contains("COPY_ON_WRITE"));
  }

  /**
   * Test case of command 'refresh'.
   */
  @Test
  public void testRefresh() throws IOException {
    List<String> refreshCommands = Arrays.asList("refresh", "metadata refresh",
        "commits refresh", "cleans refresh", "savepoints refresh");
    for (String command : refreshCommands) {
      testRefreshCommand(command);
    }
  }

  private void testRefreshCommand(String command) throws IOException {
    // clean table matedata
    FileSystem fs = FileSystem.get(storageConf().unwrap());
    fs.delete(new Path(tablePath + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME), true);

    // Create table
    assertTrue(prepareTable());

    HoodieTimeline timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "There should have no instant at first");

    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(tablePath, instantTime, storageConf());
    }

    // Before refresh, no instant
    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "there should have no instant");

    Object result = shell.evaluate(() -> command);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();

    // After refresh, there are 4 instants
    assertEquals(4, timeline.countInstants(), "there should have 4 instants");
  }

  @Test
  public void testFetchTableSchema() throws Exception {
    // Create table and connect
    HoodieCLI.conf = storageConf();
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", HoodieTableVersion.current().versionCode(),
        "org.apache.hudi.common.model.HoodieAvroPayload");

    String schemaStr = "{\n"
        + "         \"type\" : \"record\",\n"
        + "         \"name\" : \"SchemaName\",\n"
        + "         \"namespace\" : \"SchemaNS\",\n"
        + "         \"fields\" : [ {\n"
        + "           \"name\" : \"key\",\n"
        + "           \"type\" : \"int\"\n"
        + "         }, {\n"
        + "           \"name\" : \"val\",\n"
        + "           \"type\" : [ \"null\", \"string\" ],\n"
        + "           \"default\" : null\n"
        + "         }]};";

    generateData(schemaStr);

    Object result = shell.evaluate(() -> "fetch table schema");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    String actualSchemaStr = result.toString().substring(result.toString().indexOf("{"));
    HoodieSchema actualSchema = HoodieSchema.parse(actualSchemaStr);

    HoodieSchema expectedSchema = HoodieSchema.parse(schemaStr);
    expectedSchema = HoodieSchemaUtils.addMetadataFields(expectedSchema);
    assertEquals(actualSchema, expectedSchema);

    File file = File.createTempFile("temp", null);
    result = shell.evaluate(() -> "fetch table schema --outputFilePath " + file.getAbsolutePath());
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    actualSchemaStr = getFileContent(file.getAbsolutePath());
    actualSchema = HoodieSchema.parse(actualSchemaStr);
    assertEquals(actualSchema, expectedSchema);
  }

  private LinkedHashMap<String, Integer[]> generateData(String schemaStr) throws Exception {
    // generate data and metadata
    LinkedHashMap<String, Integer[]> data = new LinkedHashMap<>();
    data.put("102", new Integer[] {15, 10});
    data.put("101", new Integer[] {20, 10});
    data.put("100", new Integer[] {15, 15});
    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, key, HoodieCLI.conf,
          Option.of(value[0]), Option.of(value[1]), Collections.singletonMap(HoodieCommitMetadata.SCHEMA_KEY, schemaStr));
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should have 3 commits");
    return data;
  }

  private String getFileContent(String fileToReadStr) throws IOException {
    File fileToRead = new File(fileToReadStr);
    if (!fileToRead.exists()) {
      throw new IllegalStateException("Outfile " + fileToReadStr + "not found ");
    }
    FileInputStream fis = new FileInputStream(fileToRead);
    byte[] data = new byte[(int) fileToRead.length()];
    fis.read(data);
    fis.close();
    return fromUTF8Bytes(data);
  }

  // ---------------------------------------------------------------------------
  // set-meta-fields-mode
  // ---------------------------------------------------------------------------

  @Test
  public void testSetMetaFieldsModeOnFreshTableToCommitTimeOnly() {
    assertTrue(prepareTable());
    // Default table is ALL — no commits yet, so the safety check must let this through.
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    // Rendered diff must surface the changed properties so the operator can confirm the write.
    String rendered = result.toString();
    assertTrue(rendered.contains(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "expected rendered diff to mention populate.meta.fields, got: " + rendered);
    assertTrue(rendered.contains(HoodieTableConfig.META_FIELDS_MODE.key()),
        "expected rendered diff to mention meta.fields.mode, got: " + rendered);
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, client.getTableConfig().getMetaFieldsMode());
    assertFalse(client.getTableConfig().populateMetaFields());
  }

  @Test
  public void testSetMetaFieldsModeOnFreshTableToFileNameOnly() {
    assertTrue(prepareTable());
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode FILE_NAME_ONLY");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(MetaFieldsMode.FILE_NAME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  @Test
  public void testSetMetaFieldsModeOnFreshTableToCombinedMode() {
    assertTrue(prepareTable());
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_AND_FILE_NAME");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  @Test
  public void testSetMetaFieldsModeOnFreshTableToNone() {
    assertTrue(prepareTable());
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode NONE");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(MetaFieldsMode.NONE, client.getTableConfig().getMetaFieldsMode());
    assertFalse(client.getTableConfig().populateMetaFields());
  }

  @Test
  public void testSetMetaFieldsModeToAllWritesTheModeExplicitly() throws IOException {
    assertTrue(prepareTable());
    // First move to a selective mode, then back to ALL. Legal here only because the table has no
    // commits — on a populated table this would be a widening and refused outright.
    shell.evaluate(() -> "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    Object result = shell.evaluate(() -> "table set-meta-fields-mode --target-mode ALL");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(MetaFieldsMode.ALL, client.getTableConfig().getMetaFieldsMode());
    assertTrue(client.getTableConfig().populateMetaFields());
    // The mode is written explicitly rather than deleted. Deleting it would leave the table
    // resolving through the legacy fallback -- indistinguishable from a table predating the
    // property -- and would make this command's effect invisible in hoodie.properties. The v10->v9
    // downgrade handler also reads the mode to derive the boolean it writes back.
    assertEquals(MetaFieldsMode.ALL.name(),
        client.getTableConfig().getString(HoodieTableConfig.META_FIELDS_MODE));
  }

  @Test
  public void testSetMetaFieldsModeRefusesWideningOnPopulatedTableEvenWithForce() throws Exception {
    assertTrue(prepareTable());
    shell.evaluate(() -> "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    createDummyCommitFile("20260101000000000");
    HoodieCLI.refreshTableMetadata();

    // Widening is one-way-forbidden and --force does not override it: existing files are not
    // rewritten, so the table would advertise a column that is null for every row written so far,
    // and incremental queries would then admit the table and silently skip those rows. There is no
    // consequence for an operator to knowingly accept, so this is a hard failure.
    for (String command : new String[] {
        "table set-meta-fields-mode --target-mode ALL",
        "table set-meta-fields-mode --target-mode ALL --force true",
        "table set-meta-fields-mode --target-mode COMMIT_TIME_AND_FILE_NAME --force true"}) {
      Object result = shell.evaluate(() -> command);
      assertFalse(ShellEvaluationResultUtil.isSuccess(result), "expected refusal for: " + command);
      assertTrue(result.toString().contains("widen"),
          "expected a widening refusal for '" + command + "', got: " + result);
      assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
          HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode(),
          "mode must be unchanged after a refused widening");
    }
  }

  @Test
  public void testSetMetaFieldsModeAllowsNarrowingOnPopulatedTableWithForce() throws Exception {
    assertTrue(prepareTable());
    shell.evaluate(() -> "table set-meta-fields-mode --target-mode COMMIT_TIME_AND_FILE_NAME");
    createDummyCommitFile("20260101000000000");
    HoodieCLI.refreshTableMetadata();

    // Narrowing is the direction the CLI exists to allow. It still needs --force, because it leaves
    // mixed-mode files, but it is not refused outright the way widening is.
    Object refused = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    assertFalse(ShellEvaluationResultUtil.isSuccess(refused));

    Object forced = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY --force true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(forced), "narrowing with --force must succeed: " + forced);
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  @Test
  public void testSetMetaFieldsModeAcceptsLowercaseTargetMode() {
    assertTrue(prepareTable());
    // Routed through MetaFieldsMode.parse rather than valueOf, so an operator typing the mode in
    // lower case is not rejected.
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode commit_time_only");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), "expected lowercase to be accepted: " + result);
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  @Test
  public void testSetMetaFieldsModeKeepsBothPropertiesInAgreement() {
    assertTrue(prepareTable());
    // hoodie.properties must never contradict itself: the legacy boolean is derived from the mode,
    // never taken from the caller, so a pre-1.3.0 reader that sees only the boolean treats a
    // selective table as NONE rather than assuming meta columns that are physically null.
    for (MetaFieldsMode mode : MetaFieldsMode.values()) {
      shell.evaluate(() -> "table set-meta-fields-mode --target-mode " + mode.name() + " --force true");
      HoodieTableConfig tableConfig = HoodieCLI.getTableMetaClient().getTableConfig();
      if (tableConfig.getMetaFieldsMode() == mode) {
        assertEquals(mode.toLegacyPopulateMetaFields(), tableConfig.populateMetaFields(),
            "populate.meta.fields must be the derived value for mode " + mode);
      }
    }
  }

  @Test
  public void testSetMetaFieldsModeNoOpWhenAlreadyInTargetMode() {
    assertTrue(prepareTable());
    Object first = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    assertTrue(ShellEvaluationResultUtil.isSuccess(first));
    // Second call — same target — should be a no-op message.
    Object second = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    assertTrue(ShellEvaluationResultUtil.isSuccess(second));
    assertTrue(second.toString().contains("already in COMMIT_TIME_ONLY"),
        "expected no-op message, got: " + second);
  }

  @Test
  public void testSetMetaFieldsModeRejectsUnknownValue() {
    assertTrue(prepareTable());
    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode BOGUS_MODE");
    // Shell evaluate returns the exception object on failure.
    assertFalse(ShellEvaluationResultUtil.isSuccess(result));
    assertTrue(result.toString().contains("BOGUS_MODE"),
        "expected error message to name the rejected value, got: " + result);
  }

  @Test
  public void testSetMetaFieldsModeRefusesOnPopulatedTable() throws Exception {
    assertTrue(prepareTable());
    createDummyCommitFile("20260101000000000");
    HoodieCLI.refreshTableMetadata();

    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    assertFalse(ShellEvaluationResultUtil.isSuccess(result));
    assertTrue(result.toString().contains("Refusing to change") || result.toString().contains("--force"),
        "expected refusal message, got: " + result);

    // Mode must not have changed.
    assertEquals(MetaFieldsMode.ALL,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  @Test
  public void testSetMetaFieldsModeWithForceOnPopulatedTable() throws Exception {
    assertTrue(prepareTable());
    createDummyCommitFile("20260101000000000");
    HoodieCLI.refreshTableMetadata();

    Object result = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY --force true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  /**
   * Pins the whole migration matrix rather than spot-checking it. Twenty ordered pairs, each either
   * a legal narrowing or a refused widening, on a table that already has commits.
   *
   * <p>The subtle entries are the mutually-wider siblings: {@code COMMIT_TIME_ONLY} and
   * {@code FILE_NAME_ONLY} each populate a column the other does not, so neither can migrate to the
   * other in either direction. A spot-check of "narrowing works, widening does not" would miss that
   * the relation is a lattice rather than a chain.
   *
   * <p>Uses a fresh table per pair so the starting mode can be set without tripping the very guard
   * under test -- setting the initial mode happens before any commit exists.
   */
  @Test
  public void testSetMetaFieldsModeMigrationMatrixOnPopulatedTable() throws Exception {
    for (MetaFieldsMode from : MetaFieldsMode.values()) {
      for (MetaFieldsMode to : MetaFieldsMode.values()) {
        if (from == to) {
          continue;
        }
        // Fresh table per pair; connect to it so HoodieCLI points at the right one.
        String pairName = tableName + "_" + from.name() + "_to_" + to.name();
        String pairPath = tablePath(pairName);
        assertTrue(ShellEvaluationResultUtil.isSuccess(
            shell.evaluate(() -> "create --path " + pairPath + " --tableName " + pairName)));

        // Establish the starting mode while the table is still empty, then make it "populated".
        assertTrue(ShellEvaluationResultUtil.isSuccess(
            shell.evaluate(() -> "table set-meta-fields-mode --target-mode " + from.name())),
            "setting the initial mode on an empty table must succeed: " + from);
        createDummyCommitFileAt(pairPath, "20260101000000000");
        HoodieCLI.refreshTableMetadata();
        assertEquals(from, HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());

        boolean widening = to.isWiderThan(from);
        Object result = shell.evaluate(() ->
            "table set-meta-fields-mode --target-mode " + to.name() + " --force true");

        if (widening) {
          assertFalse(ShellEvaluationResultUtil.isSuccess(result),
              from + " -> " + to + " adds a meta column and must be refused even with --force");
          assertTrue(result.toString().contains("widen"),
              "expected a widening refusal for " + from + " -> " + to + ", got: " + result);
          assertEquals(from, HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode(),
              "a refused migration must leave the mode untouched: " + from + " -> " + to);
        } else {
          assertTrue(ShellEvaluationResultUtil.isSuccess(result),
              from + " -> " + to + " drops meta columns and must be allowed with --force, got: " + result);
          HoodieTableConfig tableConfig = HoodieCLI.getTableMetaClient().getTableConfig();
          assertEquals(to, tableConfig.getMetaFieldsMode(), from + " -> " + to);
          assertEquals(to.toLegacyPopulateMetaFields(), tableConfig.populateMetaFields(),
              "the derived boolean must follow the new mode for " + from + " -> " + to);
        }
      }
    }
  }

  /** Both mutually-wider directions between the two single-column modes are refused. */
  @Test
  public void testSetMetaFieldsModeRefusesBothSiblingDirections() throws Exception {
    assertTrue(prepareTable());
    shell.evaluate(() -> "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY");
    createDummyCommitFile("20260101000000000");
    HoodieCLI.refreshTableMetadata();

    Object toSibling = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode FILE_NAME_ONLY --force true");
    assertFalse(ShellEvaluationResultUtil.isSuccess(toSibling),
        "COMMIT_TIME_ONLY -> FILE_NAME_ONLY adds _hoodie_file_name and must be refused");
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());

    // ...and the reverse, on a table that starts the other way round.
    String otherName = tableName + "_sibling_reverse";
    String otherPath = tablePath(otherName);
    assertTrue(ShellEvaluationResultUtil.isSuccess(
        shell.evaluate(() -> "create --path " + otherPath + " --tableName " + otherName)));
    shell.evaluate(() -> "table set-meta-fields-mode --target-mode FILE_NAME_ONLY");
    createDummyCommitFileAt(otherPath, "20260101000000000");
    HoodieCLI.refreshTableMetadata();

    Object toOther = shell.evaluate(() ->
        "table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY --force true");
    assertFalse(ShellEvaluationResultUtil.isSuccess(toOther),
        "FILE_NAME_ONLY -> COMMIT_TIME_ONLY adds _hoodie_commit_time and must be refused");
    assertEquals(MetaFieldsMode.FILE_NAME_ONLY,
        HoodieCLI.getTableMetaClient().getTableConfig().getMetaFieldsMode());
  }

  private void createDummyCommitFileAt(String tableBasePath, String instantTime) throws IOException {
    java.nio.file.Path timelineDir =
        Paths.get(tableBasePath, METAFOLDER_NAME, "timeline");
    if (!timelineDir.toFile().exists()) {
      timelineDir.toFile().mkdirs();
    }
    String completionTime = instantTime + "1";
    java.nio.file.Files.createFile(timelineDir.resolve(instantTime + "_" + completionTime + ".commit"));
  }

  private void createDummyCommitFile(String instantTime) throws IOException {
    // Timeline v2 layout: files live under .hoodie/timeline/. Writing an empty completed commit
    // is enough to make countInstants > 0 for the safety check.
    java.nio.file.Path timelineDir = Paths.get(metaPath, "timeline");
    if (!timelineDir.toFile().exists()) {
      timelineDir.toFile().mkdirs();
    }
    // Completed-commit filename in v2 uses <requested>_<completed>.commit; the completion time is
    // used for range queries. Any monotonically-later value works for a synthetic commit.
    String completionTime = instantTime + "1";
    java.nio.file.Files.createFile(timelineDir.resolve(instantTime + "_" + completionTime + ".commit"));
  }
}
