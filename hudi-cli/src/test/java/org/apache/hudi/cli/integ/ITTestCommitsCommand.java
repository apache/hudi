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
import org.apache.hudi.cli.commands.RollbacksCommand;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.CommitsCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
public class ITTestCommitsCommand extends HoodieCLIIntegrationTestBase {

  @BeforeEach
  public void init() throws IOException {
    tableName = "test_table_" + ITTestCommitsCommand.class.getName();
    basePath = Paths.get(basePath, tableName).toString();

    HoodieCLI.conf = jsc.hadoopConfiguration();
    // Create table and connect
    new TableCommand().createTable(
        basePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    initMetaClient();
  }

  /**
   * Test case of 'commit rollback' command.
   */
  @Test
  public void testRollbackCommit() throws Exception {
    //Create some commits files and base files
    Map<String, String> partitionAndFileId = new HashMap<String, String>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, "file-1");
        put(DEFAULT_SECOND_PARTITION_PATH, "file-2");
        put(DEFAULT_THIRD_PARTITION_PATH, "file-3");
      }
    };
    final String rollbackCommit = "102";
    HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(DEFAULT_PARTITION_PATHS)
        .addCommit("100")
        .withBaseFilesInPartitions(partitionAndFileId)
        .addCommit("101")
        .withBaseFilesInPartitions(partitionAndFileId)
        .addCommit(rollbackCommit)
        .withBaseFilesInPartitions(partitionAndFileId);

    CommandResult cr = getShell().executeCommand(String.format("commit rollback --commit %s --sparkMaster %s --sparkMemory %s",
        rollbackCommit, "local", "4G"));
    assertTrue(cr.isSuccess());

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    HoodieActiveTimeline rollbackTimeline = new RollbacksCommand.RollbackTimeline(metaClient);
    assertEquals(1, rollbackTimeline.getRollbackTimeline().countInstants(), "There should have 1 rollback instant.");

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    assertEquals(2, timeline.getCommitsTimeline().countInstants(), "There should have 2 instants.");

    // rollback complete commit
    CommandResult cr2 = getShell().executeCommand(String.format("commit rollback --commit %s --sparkMaster %s --sparkMemory %s",
            "101", "local", "4G"));
    assertTrue(cr2.isSuccess());

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    HoodieActiveTimeline rollbackTimeline2 = new RollbacksCommand.RollbackTimeline(metaClient);
    assertEquals(1, rollbackTimeline2.getRollbackTimeline().countInstants(), "There should have 2 rollback instant.");

    HoodieActiveTimeline timeline2 = metaClient.reloadActiveTimeline();
    assertEquals(2, timeline2.getCommitsTimeline().countInstants(), "There should have 1 instants.");
  }
}
