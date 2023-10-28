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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test class for {@link TableCommand#changeTableType(String)}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestTableCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;
  private String tablePath;

  @Test
  public void testChangeTableCOW2MOR() throws IOException {
    tablePath = basePath + Path.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTestDataGenerator.createCommitFile(tablePath, "100", jsc.hadoopConfiguration());

    Object result = shell.evaluate(() -> "table change-table-type COW_TO_MOR");

    assertNotNull(result);
    assertEquals(HoodieTableType.MERGE_ON_READ, HoodieCLI.getTableMetaClient().getTableType());
  }

  @Test
  public void testChangeTableMOR2COW() throws IOException {
    tablePath = basePath + Path.SEPARATOR + tableName + "_mor2cow";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    Object result = shell.evaluate(() -> "table change-table-type MOR_TO_COW");

    assertNotNull(result);
    assertEquals(HoodieTableType.COPY_ON_WRITE, HoodieCLI.getTableMetaClient().getTableType());
  }

  @Test
  public void testChangeTableMOR2COW_withoutCompaction() throws IOException {
    tablePath = basePath + Path.SEPARATOR + tableName + "_cow2mor";
    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTestDataGenerator.createDeltaCommitFile(tablePath, "100", jsc.hadoopConfiguration());

    Object result = shell.evaluate(() -> "table change-table-type MOR_TO_COW");

    assertEquals(HoodieException.class, result.getClass());
    assertEquals("The last action must be a completed compaction for this operation. But is deltacommit[status=COMPLETED]",
        ((HoodieException) result).getMessage());
    // table change type failed, remain MOR
    assertEquals(HoodieTableType.MERGE_ON_READ, HoodieCLI.getTableMetaClient().getTableType());
  }
}
