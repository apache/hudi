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
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link TableCommand}.
 */
public class TestTableCommand extends AbstractShellIntegrationTest {

  private final String tableName = "test_table";
  private String tablePath;
  private String metaPath;

  /**
   * Init path after Mini hdfs init.
   */
  @BeforeEach
  public void init() {
    HoodieCLI.conf = jsc.hadoopConfiguration();
    tablePath = basePath + File.separator + tableName;
    metaPath = tablePath + File.separator + METAFOLDER_NAME;
  }

  /**
   * Method to create a table for connect or desc.
   */
  private boolean prepareTable() {
    CommandResult cr = getShell().executeCommand(
        "create --path " + tablePath + " --tableName " + tableName);
    return cr.isSuccess();
  }

  /**
   * Test Case for connect table.
   */
  @Test
  public void testConnectTable() {
    // Prepare table
    assertTrue(prepareTable());

    // Test connect with specified values
    CommandResult cr = getShell().executeCommand(
        "connect --path " + tablePath + " --initialCheckIntervalMs 3000 "
            + "--maxWaitIntervalMs 40000 --maxCheckIntervalMs 8");
    assertTrue(cr.isSuccess());

    // Check specified values
    ConsistencyGuardConfig conf = HoodieCLI.consistencyGuardConfig;
    assertEquals(3000, conf.getInitialConsistencyCheckIntervalMs());
    assertEquals(40000, conf.getMaxConsistencyCheckIntervalMs());
    assertEquals(8, conf.getMaxConsistencyChecks());

    // Check default values
    assertFalse(conf.isConsistencyCheckEnabled());
    assertEquals(new Integer(1), HoodieCLI.layoutVersion.getVersion());
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
    assertEquals(metaPath, client.getArchivePath());
    assertEquals(tablePath, client.getBasePath());
    assertEquals(metaPath, client.getMetaPath());
    assertEquals(HoodieTableType.COPY_ON_WRITE, client.getTableType());
    assertEquals(new Integer(1), client.getTimelineLayoutVersion().getVersion());
  }

  /**
   * Test Cases for create table with specified values.
   */
  @Test
  public void testCreateWithSpecifiedValues() {
    // Test create with specified values
    CommandResult cr = getShell().executeCommand(
        "create --path " + tablePath + " --tableName " + tableName
            + " --tableType MERGE_ON_READ --archiveLogFolder archive");
    assertTrue(cr.isSuccess());
    assertEquals("Metadata for table " + tableName + " loaded", cr.getResult().toString());
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(metaPath + File.separator + "archive", client.getArchivePath());
    assertEquals(tablePath, client.getBasePath());
    assertEquals(metaPath, client.getMetaPath());
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
    CommandResult cr = getShell().executeCommand("desc");
    assertTrue(cr.isSuccess());

    // check table's basePath metaPath and type
    assertTrue(cr.getResult().toString().contains(tablePath));
    assertTrue(cr.getResult().toString().contains(metaPath));
    assertTrue(cr.getResult().toString().contains("COPY_ON_WRITE"));
  }

  /**
   * Test case of command 'refresh'.
   */
  @Test
  public void testRefresh() throws IOException {
    List<String> refreshCommands = Arrays.asList("refresh", "metadata refresh",
        "commits refresh", "cleans refresh", "savepoints refresh");
    for (String command: refreshCommands) {
      testRefreshCommand(command);
    }
  }

  private void testRefreshCommand(String command) throws IOException {
    // clean table matedata
    FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
    fs.delete(new Path(tablePath + File.separator + HoodieTableMetaClient.METAFOLDER_NAME), true);

    // Create table
    assertTrue(prepareTable());

    HoodieTimeline timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "There should have no instant at first");

    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(tablePath, instantTime, jsc.hadoopConfiguration());
    }

    // Before refresh, no instant
    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "there should have no instant");

    CommandResult cr = getShell().executeCommand(command);
    assertTrue(cr.isSuccess());

    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();

    // After refresh, there are 4 instants
    assertEquals(4, timeline.countInstants(), "there should have 4 instants");
  }
}
