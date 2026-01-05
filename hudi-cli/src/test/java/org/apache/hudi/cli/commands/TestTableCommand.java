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
    assertEquals("org.apache.hudi.client.transaction.lock.InProcessLockProvider",
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
    assertEquals("org.apache.hudi.client.transaction.lock.InProcessLockProvider",
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
}
