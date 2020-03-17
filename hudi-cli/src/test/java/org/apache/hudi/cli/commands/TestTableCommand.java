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

import org.apache.hudi.cli.AbstractShellIntegrationTest;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.junit.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Cases for {@link TableCommand}.
 */
public class TestTableCommand extends AbstractShellIntegrationTest {

  /**
   * Test Cases for create, desc and connect table.
   */
  @Test
  public void testCreateAndConnectTable() {
    // Prepare
    String tableName = "test_table";
    HoodieCLI.conf = jsc.hadoopConfiguration();
    String tablePath = basePath + File.separator + tableName;
    String metaPath = tablePath + File.separator + METAFOLDER_NAME;

    // Test create default
    CommandResult cr = getShell().executeCommand(
        "create --path " + tablePath + " --tableName " + tableName);
    assertEquals("Metadata for table " + tableName + " loaded", cr.getResult().toString());
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(metaPath, client.getArchivePath());
    assertEquals(tablePath, client.getBasePath());
    assertEquals(metaPath, client.getMetaPath());
    assertEquals(HoodieTableType.COPY_ON_WRITE, client.getTableType());
    assertEquals(new Integer(1), client.getTimelineLayoutVersion().getVersion());

    // Test desc
    cr = getShell().executeCommand("desc");
    assertTrue(cr.isSuccess());
    // check table's basePath metaPath and type
    assertTrue(cr.getResult().toString().contains(tablePath));
    assertTrue(cr.getResult().toString().contains(metaPath));
    assertTrue(cr.getResult().toString().contains("COPY_ON_WRITE"));

    // Test connect with specified values
    // Check specified values
    cr = getShell().executeCommand(
        "connect --path " + tablePath + " --initialCheckIntervalMs 3000 "
          + "--maxWaitIntervalMs 40000 --maxCheckIntervalMs 8");
    assertTrue(cr.isSuccess());
    ConsistencyGuardConfig conf = HoodieCLI.consistencyGuardConfig;
    assertEquals(3000, conf.getInitialConsistencyCheckIntervalMs());
    assertEquals(40000, conf.getMaxConsistencyCheckIntervalMs());
    assertEquals(8, conf.getMaxConsistencyChecks());
    // Check default values
    assertTrue(!conf.isConsistencyCheckEnabled());
    assertEquals(new Integer(1), HoodieCLI.layoutVersion.getVersion());
  }

  /**
   * Test Cases for create table with specified values.
   */
  @Test
  public void testCreateWithSpecifiedValues() {
    // Prepare
    String tableName = "test_table";
    HoodieCLI.conf = jsc.hadoopConfiguration();
    String tablePath = basePath + File.separator + tableName;
    String metaPath = tablePath + File.separator + METAFOLDER_NAME;

    // Test create with specified values
    CommandResult cr = getShell().executeCommand(
        "create --path " + tablePath + " --tableName " + tableName
          + " --tableType MERGE_ON_READ --archiveLogFolder archive");
    assertEquals("Metadata for table " + tableName + " loaded", cr.getResult().toString());
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    assertEquals(metaPath + File.separator + "archive", client.getArchivePath());
    assertEquals(tablePath, client.getBasePath());
    assertEquals(metaPath, client.getMetaPath());
    assertEquals(HoodieTableType.MERGE_ON_READ, client.getTableType());
  }
}
